package io.kestra.plugin.camunda;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Complete a Camunda job",
    description = """
        Reports a job as done so the process instance moves past the service task, optionally merging output variables into it.
        Use it with the [Trigger](https://kestra.io/plugins/plugin-camunda/triggers/io.kestra.plugin.camunda.trigger), which activates a job and hands its key to the flow without completing it.
        Camunda accepts the command only while the job lock is held, so the flow must finish within the trigger `timeout`."""
)
@Plugin(
    examples = {
        @Example(
            title = "Handle a Camunda job in Kestra and report the outcome back to the process instance.",
            full = true,
            code = """
                id: handle_camunda_job
                namespace: company.team

                triggers:
                  - id: on_camunda_job
                    type: io.kestra.plugin.camunda.Trigger
                    grpcAddress: "{{ secret('CAMUNDA_GRPC_ADDRESS') }}"
                    clientId: "{{ secret('CAMUNDA_CLIENT_ID') }}"
                    clientSecret: "{{ secret('CAMUNDA_CLIENT_SECRET') }}"
                    authorizationServerUrl: "{{ secret('CAMUNDA_AUTH_SERVER_URL') }}"
                    jobType: send-notification
                    timeout: PT5M

                tasks:
                  - id: send_notification
                    type: io.kestra.plugin.core.log.Log
                    message: "Notifying about order {{ trigger.variables.orderId }}"

                  - id: complete_job
                    type: io.kestra.plugin.camunda.CompleteJob
                    restAddress: "{{ secret('CAMUNDA_REST_ADDRESS') }}"
                    clientId: "{{ secret('CAMUNDA_CLIENT_ID') }}"
                    clientSecret: "{{ secret('CAMUNDA_CLIENT_SECRET') }}"
                    authorizationServerUrl: "{{ secret('CAMUNDA_AUTH_SERVER_URL') }}"
                    jobKey: "{{ trigger.jobKey }}"
                    variables:
                      notified: true
                """
        )
    }
)
public class CompleteJob extends AbstractCamundaTask implements RunnableTask<VoidOutput> {

    @Schema(
        title = "Key of the job to complete",
        description = "Available as `{{ trigger.jobKey }}` when the execution was started by the Camunda trigger."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<Long> jobKey;

    @Schema(
        title = "Variables to merge into the process instance"
    )
    @PluginProperty(group = "main")
    private Property<Map<String, Object>> variables;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var logger = runContext.logger();

        var rJobKey = runContext.render(this.jobKey).as(Long.class)
            .orElseThrow(() -> new IllegalArgumentException("`jobKey` is required"));
        var rVariables = runContext.render(this.variables).asMap(String.class, Object.class);

        try (var client = this.camundaClient(runContext)) {
            var command = client.newCompleteCommand(rJobKey);

            if (!rVariables.isEmpty()) {
                command = command.variables(rVariables);
            }

            command.execute();
            logger.info("Completed job {}", rJobKey);
        }

        return null;
    }
}
