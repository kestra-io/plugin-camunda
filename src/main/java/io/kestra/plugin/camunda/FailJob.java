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
    title = "Report a Camunda job as failed",
    description = """
        Tells Camunda the job could not be done, so the process instance stops waiting on this worker.
        Use it in a flow's `errors` block alongside the [Trigger](https://kestra.io/plugins/plugin-camunda/triggers/io.kestra.plugin.camunda.trigger), which does not report failure by itself.

        Leaving a job neither completed nor failed is not equivalent. Camunda re-offers a job whose lock expired
        without decrementing its retries, so the trigger activates it again, the flow fails again, and that repeats
        for as long as the process instance lives. Retries never reach zero, so no incident is ever raised and
        nothing surfaces in Operate.

        `retries` defaults to `0`, which raises an incident immediately and makes the failure visible.
        Pass `{{ trigger.retries - 1 }}` instead to spend the retries modelled on the BPMN task before the incident."""
)
@Plugin(
    examples = {
        @Example(
            title = "Complete a Camunda job on success and report it as failed on error.",
            full = true,
            code = """
                id: handle_camunda_job_with_errors
                namespace: company.team

                triggers:
                  - id: on_camunda_job
                    type: io.kestra.plugin.camunda.Trigger
                    restAddress: "{{ secret('CAMUNDA_REST_ADDRESS') }}"
                    clientId: "{{ secret('CAMUNDA_CLIENT_ID') }}"
                    clientSecret: "{{ secret('CAMUNDA_CLIENT_SECRET') }}"
                    authorizationServerUrl: "{{ secret('CAMUNDA_AUTH_SERVER_URL') }}"
                    audience: "{{ secret('CAMUNDA_AUDIENCE') }}"
                    audience: "{{ secret('CAMUNDA_AUDIENCE') }}"
                    jobType: charge-payment
                    timeout: PT5M

                tasks:
                  - id: charge
                    type: io.kestra.plugin.core.http.Request
                    uri: https://payments.example.com/charge

                  - id: complete_job
                    type: io.kestra.plugin.camunda.CompleteJob
                    restAddress: "{{ secret('CAMUNDA_REST_ADDRESS') }}"
                    clientId: "{{ secret('CAMUNDA_CLIENT_ID') }}"
                    clientSecret: "{{ secret('CAMUNDA_CLIENT_SECRET') }}"
                    authorizationServerUrl: "{{ secret('CAMUNDA_AUTH_SERVER_URL') }}"
                    audience: "{{ secret('CAMUNDA_AUDIENCE') }}"
                    audience: "{{ secret('CAMUNDA_AUDIENCE') }}"
                    jobKey: "{{ trigger.jobKey }}"

                errors:
                  - id: fail_job
                    type: io.kestra.plugin.camunda.FailJob
                    restAddress: "{{ secret('CAMUNDA_REST_ADDRESS') }}"
                    clientId: "{{ secret('CAMUNDA_CLIENT_ID') }}"
                    clientSecret: "{{ secret('CAMUNDA_CLIENT_SECRET') }}"
                    authorizationServerUrl: "{{ secret('CAMUNDA_AUTH_SERVER_URL') }}"
                    audience: "{{ secret('CAMUNDA_AUDIENCE') }}"
                    audience: "{{ secret('CAMUNDA_AUDIENCE') }}"
                    jobKey: "{{ trigger.jobKey }}"
                    errorMessage: "Kestra execution {{ execution.id }} failed"
                """
        ),
        @Example(
            title = "Spend the retries modelled on the BPMN task before raising an incident, backing off between attempts.",
            full = true,
            code = """
                id: retry_camunda_job
                namespace: company.team

                triggers:
                  - id: on_camunda_job
                    type: io.kestra.plugin.camunda.Trigger
                    restAddress: http://localhost:8080
                    jobType: charge-payment
                    timeout: PT5M

                tasks:
                  - id: charge
                    type: io.kestra.plugin.core.http.Request
                    uri: https://payments.example.com/charge

                  - id: complete_job
                    type: io.kestra.plugin.camunda.CompleteJob
                    restAddress: http://localhost:8080
                    jobKey: "{{ trigger.jobKey }}"

                errors:
                  - id: fail_job
                    type: io.kestra.plugin.camunda.FailJob
                    restAddress: http://localhost:8080
                    jobKey: "{{ trigger.jobKey }}"
                    retries: "{{ trigger.retries - 1 }}"
                    retryBackoff: PT30S
                    errorMessage: "Charge failed, {{ trigger.retries - 1 }} attempts left"
                """
        )
    }
)
public class FailJob extends AbstractCamundaTask implements RunnableTask<VoidOutput> {

    @Schema(
        title = "Key of the job to fail",
        description = "Available as `{{ trigger.jobKey }}` when the execution was started by the Camunda trigger."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<Long> jobKey;

    @Schema(
        title = "Retries left after this failure",
        description = """
            `0`, the default, raises an incident in Camunda straight away.
            Any higher value makes the job activatable again, so pass `{{ trigger.retries - 1 }}` to spend the
            retries modelled on the BPMN task first. Camunda raises the incident once this reaches zero."""
    )
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<Integer> retries = Property.ofValue(0);

    @Schema(
        title = "Message attached to the failure",
        description = "Shown on the incident in Operate, so include whatever identifies the failed run."
    )
    @PluginProperty(group = "main")
    private Property<String> errorMessage;

    @Schema(
        title = "How long Camunda waits before making the job activatable again",
        description = "ISO-8601 duration. Only has an effect when `retries` is above zero."
    )
    @PluginProperty(group = "main")
    private Property<Duration> retryBackoff;

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
        var rRetries = runContext.render(this.retries).as(Integer.class).orElse(0);
        var rErrorMessage = runContext.render(this.errorMessage).as(String.class).filter(v -> !v.isBlank()).orElse(null);
        var rRetryBackoff = runContext.render(this.retryBackoff).as(Duration.class).orElse(null);
        var rVariables = runContext.render(this.variables).asMap(String.class, Object.class);

        if (rRetries < 0) {
            throw new IllegalArgumentException("`retries` cannot be negative, use 0 to raise an incident immediately");
        }

        try {
            var command = this.openClient(runContext).newFailCommand(rJobKey).retries(rRetries);

            if (rErrorMessage != null) {
                command = command.errorMessage(rErrorMessage);
            }
            if (rRetryBackoff != null) {
                command = command.retryBackoff(rRetryBackoff);
            }
            if (!rVariables.isEmpty()) {
                command = command.variables(rVariables);
            }

            command.execute();

            if (rRetries == 0) {
                logger.info("Failed job {}, no retries left so Camunda raises an incident", rJobKey);
            } else {
                logger.info("Failed job {}, {} retries left", rJobKey, rRetries);
            }
        } finally {
            this.closeClient();
        }

        return null;
    }
}
