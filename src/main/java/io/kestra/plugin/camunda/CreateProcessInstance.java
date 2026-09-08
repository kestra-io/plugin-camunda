package io.kestra.plugin.camunda;

import io.camunda.client.api.command.CreateProcessInstanceCommandStep1;
import io.camunda.client.api.command.FinalCommandStep;
import io.camunda.client.api.response.ProcessInstanceEvent;
import io.camunda.client.api.response.ProcessInstanceResult;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Start a Camunda process instance",
    description = """
        Starts an instance of a deployed process, either by its BPMN process ID or by a process definition key.
        With `awaitCompletion` set to `true` the task blocks until the process instance ends and returns its variables,
        so `requestTimeout` must be longer than the process takes to run."""
)
@Plugin(
    examples = {
        @Example(
            title = "Start a process instance and pass flow inputs as process variables.",
            full = true,
            code = """
                id: start_camunda_process
                namespace: company.team

                inputs:
                  - id: orderId
                    type: STRING

                tasks:
                  - id: start_process
                    type: io.kestra.plugin.camunda.CreateProcessInstance
                    restAddress: "{{ secret('CAMUNDA_REST_ADDRESS') }}"
                    clientId: "{{ secret('CAMUNDA_CLIENT_ID') }}"
                    clientSecret: "{{ secret('CAMUNDA_CLIENT_SECRET') }}"
                    authorizationServerUrl: "{{ secret('CAMUNDA_AUTH_SERVER_URL') }}"
                    audience: "{{ secret('CAMUNDA_AUDIENCE') }}"
                    processId: order-fulfillment
                    variables:
                      orderId: "{{ inputs.orderId }}"
                """
        ),
        @Example(
            title = "Start a process instance, wait for it to finish, and read a result variable.",
            full = true,
            code = """
                id: run_camunda_process
                namespace: company.team

                tasks:
                  - id: run_process
                    type: io.kestra.plugin.camunda.CreateProcessInstance
                    restAddress: http://localhost:8080
                    processId: order-fulfillment
                    variables:
                      orderId: ORD-123
                    awaitCompletion: true
                    fetchVariables:
                      - totalAmount
                    requestTimeout: PT2M

                  - id: log_total
                    type: io.kestra.plugin.core.log.Log
                    message: "Order total is {{ outputs.run_process.variables.totalAmount }}"
                """
        )
    }
)
public class CreateProcessInstance extends AbstractCamundaTask implements RunnableTask<CreateProcessInstance.Output> {

    @Schema(
        title = "BPMN process ID of the process to start",
        description = "The `id` attribute of the process element. Mutually exclusive with `processDefinitionKey`."
    )
    @PluginProperty(group = "main")
    private Property<String> processId;

    @Schema(
        title = "Key of the process definition to start",
        description = "Targets one exact deployed version. Mutually exclusive with `processId`."
    )
    @PluginProperty(group = "main")
    private Property<Long> processDefinitionKey;

    @Schema(
        title = "Version of the process definition",
        description = "Only used with `processId`. Defaults to the latest deployed version."
    )
    @PluginProperty(group = "main")
    private Property<Integer> processVersion;

    @Schema(
        title = "Variables to set on the new process instance"
    )
    @PluginProperty(group = "main")
    private Property<Map<String, Object>> variables;

    @Schema(
        title = "Wait for the process instance to complete",
        description = """
            When `true`, the task returns only once the process instance has ended, and its variables are available in the `variables` output.
            Killing the execution while it waits stops the task but not the process instance, Camunda keeps running it. The process
            instance key is only known once the command returns, so there is nothing to cancel it by while the wait is in flight."""
    )
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<Boolean> awaitCompletion = Property.ofValue(false);

    @Schema(
        title = "Variables to return when awaiting completion",
        description = "Only used with `awaitCompletion`. Returns every process variable when not set."
    )
    @PluginProperty(group = "main")
    private Property<List<String>> fetchVariables;

    @Schema(
        title = "Timeout of the command sent to Camunda",
        description = "ISO-8601 duration. Defaults to the client default of 10 seconds, which is usually too short when `awaitCompletion` is enabled."
    )
    @PluginProperty(group = "advanced")
    private Property<Duration> requestTimeout;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();

        var rProcessId = runContext.render(this.processId).as(String.class).filter(v -> !v.isBlank()).orElse(null);
        var rProcessDefinitionKey = runContext.render(this.processDefinitionKey).as(Long.class).orElse(null);
        var rVersion = runContext.render(this.processVersion).as(Integer.class).orElse(null);
        var rVariables = runContext.render(this.variables).asMap(String.class, Object.class);
        var rAwaitCompletion = runContext.render(this.awaitCompletion).as(Boolean.class).orElse(false);
        var rFetchVariables = runContext.render(this.fetchVariables).asList(String.class);
        var rRequestTimeout = runContext.render(this.requestTimeout).as(Duration.class).orElse(null);

        if (rProcessId == null && rProcessDefinitionKey == null) {
            throw new IllegalArgumentException("Either `processId` or `processDefinitionKey` is required");
        }
        if (rProcessId != null && rProcessDefinitionKey != null) {
            throw new IllegalArgumentException("`processId` and `processDefinitionKey` are mutually exclusive");
        }
        if (rVersion != null && rProcessId == null) {
            throw new IllegalArgumentException("`processVersion` can only be used with `processId`, `processDefinitionKey` already targets one version");
        }

        try {
            var client = this.openClient(runContext);

            CreateProcessInstanceCommandStep1.CreateProcessInstanceCommandStep3 command = rProcessId != null
                ? withVersion(client.newCreateInstanceCommand().bpmnProcessId(rProcessId), rVersion)
                : client.newCreateInstanceCommand().processDefinitionKey(rProcessDefinitionKey);

            if (!rVariables.isEmpty()) {
                command = command.variables(rVariables);
            }

            logger.info("Starting process instance of {}", rProcessId != null ? rProcessId : rProcessDefinitionKey);

            return rAwaitCompletion
                ? awaitResult(command, rFetchVariables, rRequestTimeout)
                : start(command, rRequestTimeout);
        } finally {
            this.closeClient();
        }
    }

    private static CreateProcessInstanceCommandStep1.CreateProcessInstanceCommandStep3 withVersion(
        CreateProcessInstanceCommandStep1.CreateProcessInstanceCommandStep2 command,
        Integer version
    ) {
        return version != null ? command.version(version) : command.latestVersion();
    }

    private static Output start(
        CreateProcessInstanceCommandStep1.CreateProcessInstanceCommandStep3 command,
        Duration requestTimeout
    ) {
        FinalCommandStep<ProcessInstanceEvent> finalStep = requestTimeout != null ? command.requestTimeout(requestTimeout) : command;
        var event = finalStep.execute();

        return Output.builder()
            .processInstanceKey(event.getProcessInstanceKey())
            .bpmnProcessId(event.getBpmnProcessId())
            .processDefinitionKey(event.getProcessDefinitionKey())
            .version(event.getVersion())
            .tenantId(event.getTenantId())
            .build();
    }

    private static Output awaitResult(
        CreateProcessInstanceCommandStep1.CreateProcessInstanceCommandStep3 command,
        List<String> fetchVariables,
        Duration requestTimeout
    ) {
        var withResult = command.withResult();
        if (!fetchVariables.isEmpty()) {
            withResult = withResult.fetchVariables(fetchVariables);
        }

        FinalCommandStep<ProcessInstanceResult> finalStep = requestTimeout != null ? withResult.requestTimeout(requestTimeout) : withResult;
        var result = finalStep.execute();

        return Output.builder()
            .processInstanceKey(result.getProcessInstanceKey())
            .bpmnProcessId(result.getBpmnProcessId())
            .processDefinitionKey(result.getProcessDefinitionKey())
            .version(result.getVersion())
            .tenantId(result.getTenantId())
            .variables(result.getVariablesAsMap())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(title = "Key of the created process instance")
        private final Long processInstanceKey;

        @Schema(title = "BPMN process ID of the started process")
        private final String bpmnProcessId;

        @Schema(title = "Key of the process definition that was instantiated")
        private final Long processDefinitionKey;

        @Schema(title = "Version of the process definition that was instantiated")
        private final Integer version;

        @Schema(title = "Tenant the process instance belongs to")
        private final String tenantId;

        @Schema(
            title = "Variables of the completed process instance",
            description = "Only set when `awaitCompletion` is enabled."
        )
        private final Map<String, Object> variables;
    }
}
