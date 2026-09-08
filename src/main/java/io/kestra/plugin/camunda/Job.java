package io.kestra.plugin.camunda;

import io.camunda.client.api.response.ActivatedJob;
import io.kestra.core.models.tasks.Output;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

import java.time.Instant;
import java.util.Map;

/**
 * A job activated by the Camunda job worker, exposed to the flow as {@code trigger.*}.
 */
@Builder
@Getter
public class Job implements Output {

    @Schema(
        title = "Key of the activated job",
        description = "Pass it to the `CompleteJob` task to report the job as done."
    )
    private final Long jobKey;

    @Schema(title = "Type of the job, as declared on the BPMN service task")
    private final String type;

    @Schema(title = "Key of the process instance the job belongs to")
    private final Long processInstanceKey;

    @Schema(title = "BPMN process ID of the process instance")
    private final String bpmnProcessId;

    @Schema(title = "Key of the process definition")
    private final Long processDefinitionKey;

    @Schema(title = "Version of the process definition")
    private final Integer processDefinitionVersion;

    @Schema(title = "ID of the BPMN element that created the job")
    private final String elementId;

    @Schema(title = "Key of the element instance that created the job")
    private final Long elementInstanceKey;

    @Schema(title = "Variables of the job, limited to `fetchVariables` when it is set")
    private final Map<String, Object> variables;

    @Schema(title = "Custom headers defined on the BPMN element")
    private final Map<String, String> customHeaders;

    @Schema(title = "Name of the worker that activated the job")
    private final String worker;

    @Schema(title = "Remaining retries of the job")
    private final Integer retries;

    @Schema(
        title = "Instant at which the job lock expires",
        description = "Camunda makes the job available to workers again after this point, so the flow should complete the job before it."
    )
    private final Instant deadline;

    @Schema(title = "Tenant the job belongs to")
    private final String tenantId;

    static Job of(ActivatedJob job) {
        return Job.builder()
            .jobKey(job.getKey())
            .type(job.getType())
            .processInstanceKey(job.getProcessInstanceKey())
            .bpmnProcessId(job.getBpmnProcessId())
            .processDefinitionKey(job.getProcessDefinitionKey())
            .processDefinitionVersion(job.getProcessDefinitionVersion())
            .elementId(job.getElementId())
            .elementInstanceKey(job.getElementInstanceKey())
            .variables(job.getVariablesAsMap())
            .customHeaders(job.getCustomHeaders())
            .worker(job.getWorker())
            .retries(job.getRetries())
            .deadline(Instant.ofEpochMilli(job.getDeadline()))
            .tenantId(job.getTenantId())
            .build();
    }
}
