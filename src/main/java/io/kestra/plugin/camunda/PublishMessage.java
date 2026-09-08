package io.kestra.plugin.camunda;

import io.camunda.client.api.command.PublishMessageCommandStep1;
import io.camunda.client.api.response.PublishMessageResponse;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Publish a correlation message to a Camunda cluster",
    description = """
        Publishes a message that Camunda correlates against message subscriptions of running process instances,
        or buffers for its time to live when nothing is waiting for it yet.
        Leave `correlationKey` unset for a message that starts a process instance through a message start event."""
)
@Plugin(
    examples = {
        @Example(
            title = "Publish a payment confirmation for one order and log the message key.",
            full = true,
            code = """
                id: publish_camunda_message
                namespace: company.team

                tasks:
                  - id: publish_message
                    type: io.kestra.plugin.camunda.PublishMessage
                    restAddress: "{{ secret('CAMUNDA_REST_ADDRESS') }}"
                    clientId: "{{ secret('CAMUNDA_CLIENT_ID') }}"
                    clientSecret: "{{ secret('CAMUNDA_CLIENT_SECRET') }}"
                    authorizationServerUrl: "{{ secret('CAMUNDA_AUTH_SERVER_URL') }}"
                    messageName: payment-received
                    correlationKey: order-123
                    variables:
                      amount: 99.99

                  - id: log_result
                    type: io.kestra.plugin.core.log.Log
                    message: "Published message with key {{ outputs.publish_message.messageKey }}"
                """
        ),
        @Example(
            title = "Publish a message that starts a new process instance, with deduplication over 10 minutes.",
            full = true,
            code = """
                id: start_camunda_process_by_message
                namespace: company.team

                tasks:
                  - id: publish_message
                    type: io.kestra.plugin.camunda.PublishMessage
                    restAddress: http://localhost:8080
                    messageName: order-created
                    messageId: "{{ execution.id }}"
                    timeToLive: PT10M
                    variables:
                      orderId: ORD-123
                """
        )
    }
)
public class PublishMessage extends AbstractCamundaTask implements RunnableTask<PublishMessage.Output> {

    @Schema(
        title = "Name of the message",
        description = "Must match the message name of the BPMN message subscription."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> messageName;

    @Schema(
        title = "Correlation key of the message",
        description = "Matched against the correlation key of waiting subscriptions. Leave unset to publish a message without a correlation key, as used by message start events."
    )
    @PluginProperty(group = "main")
    private Property<String> correlationKey;

    @Schema(
        title = "Variables to merge into the correlated process instance"
    )
    @PluginProperty(group = "main")
    private Property<Map<String, Object>> variables;

    @Schema(
        title = "Unique ID used to deduplicate the message",
        description = "Camunda rejects a second message with the same name, correlation key and ID while the first one is still buffered."
    )
    @PluginProperty(group = "main")
    private Property<String> messageId;

    @Schema(
        title = "How long Camunda buffers the message when no subscription matches yet",
        description = "ISO-8601 duration. Defaults to the client default of one hour. `PT0S` discards the message when nothing is waiting for it."
    )
    @PluginProperty(group = "main")
    private Property<Duration> timeToLive;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();

        var rMessageName = runContext.render(this.messageName).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("`messageName` is required"));
        var rCorrelationKey = runContext.render(this.correlationKey).as(String.class).orElse(null);
        var rVariables = runContext.render(this.variables).asMap(String.class, Object.class);
        var rMessageId = runContext.render(this.messageId).as(String.class).orElse(null);
        var rTimeToLive = runContext.render(this.timeToLive).as(Duration.class).orElse(null);

        try (var client = this.camundaClient(runContext)) {
            var step = client.newPublishMessageCommand().messageName(rMessageName);

            PublishMessageCommandStep1.PublishMessageCommandStep3 command = rCorrelationKey != null
                ? step.correlationKey(rCorrelationKey)
                : step.withoutCorrelationKey();

            if (!rVariables.isEmpty()) {
                command = command.variables(rVariables);
            }
            if (rMessageId != null) {
                command = command.messageId(rMessageId);
            }
            if (rTimeToLive != null) {
                command = command.timeToLive(rTimeToLive);
            }

            PublishMessageResponse response = command.execute();
            logger.info("Published message {} with key {}", rMessageName, response.getMessageKey());

            return Output.builder()
                .messageKey(response.getMessageKey())
                .tenantId(response.getTenantId())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(title = "Key assigned to the published message")
        private final Long messageKey;

        @Schema(title = "Tenant the message belongs to")
        private final String tenantId;
    }
}
