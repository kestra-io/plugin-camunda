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

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Cancel a running Camunda process instance",
    description = "Terminates the process instance and every element instance inside it. Fails if the process instance is already finished."
)
@Plugin(
    examples = {
        @Example(
            title = "Cancel a process instance started earlier in the flow.",
            full = true,
            code = """
                id: cancel_camunda_process
                namespace: company.team

                tasks:
                  - id: start_process
                    type: io.kestra.plugin.camunda.CreateProcessInstance
                    restAddress: "{{ secret('CAMUNDA_REST_ADDRESS') }}"
                    clientId: "{{ secret('CAMUNDA_CLIENT_ID') }}"
                    clientSecret: "{{ secret('CAMUNDA_CLIENT_SECRET') }}"
                    authorizationServerUrl: "{{ secret('CAMUNDA_AUTH_SERVER_URL') }}"
                    processId: order-fulfillment

                  - id: cancel_process
                    type: io.kestra.plugin.camunda.CancelProcessInstance
                    restAddress: "{{ secret('CAMUNDA_REST_ADDRESS') }}"
                    clientId: "{{ secret('CAMUNDA_CLIENT_ID') }}"
                    clientSecret: "{{ secret('CAMUNDA_CLIENT_SECRET') }}"
                    authorizationServerUrl: "{{ secret('CAMUNDA_AUTH_SERVER_URL') }}"
                    processInstanceKey: "{{ outputs.start_process.processInstanceKey }}"
                """
        )
    }
)
public class CancelProcessInstance extends AbstractCamundaTask implements RunnableTask<VoidOutput> {

    @Schema(
        title = "Key of the process instance to cancel"
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<Long> processInstanceKey;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        var rProcessInstanceKey = runContext.render(this.processInstanceKey).as(Long.class)
            .orElseThrow(() -> new IllegalArgumentException("`processInstanceKey` is required"));

        try (var client = this.camundaClient(runContext)) {
            client.newCancelInstanceCommand(rProcessInstanceKey).execute();
            logger.info("Cancelled process instance {}", rProcessInstanceKey);
        }

        return null;
    }
}
