package io.kestra.plugin.camunda;

import io.camunda.client.api.command.DeployResourceCommandStep1;
import io.camunda.client.api.response.DeploymentEvent;
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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Deploy BPMN, DMN and form resources to a Camunda cluster",
    description = """
        Sends one deployment command holding every resource, so either all of them are deployed or none is.
        A resource value is either the file content itself or a `kestra://` internal storage URI produced by an earlier task."""
)
@Plugin(
    examples = {
        @Example(
            title = "Deploy a BPMN process stored as a Namespace File.",
            full = true,
            code = """
                id: deploy_camunda_process
                namespace: company.team

                tasks:
                  - id: deploy
                    type: io.kestra.plugin.camunda.Deploy
                    restAddress: "{{ secret('CAMUNDA_REST_ADDRESS') }}"
                    clientId: "{{ secret('CAMUNDA_CLIENT_ID') }}"
                    clientSecret: "{{ secret('CAMUNDA_CLIENT_SECRET') }}"
                    authorizationServerUrl: "{{ secret('CAMUNDA_AUTH_SERVER_URL') }}"
                    resources:
                      order-fulfillment.bpmn: "{{ read('order-fulfillment.bpmn') }}"
                """
        ),
        @Example(
            title = "Deploy a process and a decision table together against a local development cluster.",
            full = true,
            code = """
                id: deploy_camunda_resources
                namespace: company.team

                tasks:
                  - id: deploy
                    type: io.kestra.plugin.camunda.Deploy
                    restAddress: http://localhost:8080
                    resources:
                      order-fulfillment.bpmn: "{{ read('order-fulfillment.bpmn') }}"
                      discount.dmn: "{{ read('discount.dmn') }}"

                  - id: log_deployment
                    type: io.kestra.plugin.core.log.Log
                    message: "Deployed {{ outputs.deploy.processes | length }} process(es) as deployment {{ outputs.deploy.deploymentKey }}"
                """
        )
    }
)
public class Deploy extends AbstractCamundaTask implements RunnableTask<Deploy.Output> {

    private static final Set<String> ALLOWED_EXTENSIONS = Set.of(".bpmn", ".xml", ".dmn", ".form");

    @Schema(
        title = "Resources to deploy, keyed by resource name",
        description = """
            The key is the resource name and must end with `.bpmn` or `.xml` for a process, `.dmn` for a decision, or `.form` for a form.
            The value is either the resource content or a `kestra://` internal storage URI."""
    )
    @NotNull
    @PluginProperty(group = "main", additionalProperties = String.class)
    private Property<Map<String, String>> resources;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        Map<String, String> rResources = runContext.render(this.resources).asMap(String.class, String.class);

        if (rResources.isEmpty()) {
            throw new IllegalArgumentException("`resources` must hold at least one resource");
        }

        // resolved up front so that a bad resource name or a missing file fails without opening a connection
        var contents = new LinkedHashMap<String, String>();
        for (var resource : rResources.entrySet()) {
            contents.put(validateResourceName(resource.getKey()), resolveContent(runContext, resource.getValue()));
        }

        try (var client = this.camundaClient(runContext)) {
            var command = client.newDeployResourceCommand();
            // the fluent builder only exposes the final step after the first resource, so keep the last returned step
            DeployResourceCommandStep1.DeployResourceCommandStep2 step = null;

            for (var resource : contents.entrySet()) {
                step = step == null
                    ? command.addResourceStringUtf8(resource.getValue(), resource.getKey())
                    : step.addResourceStringUtf8(resource.getValue(), resource.getKey());
            }

            logger.info("Deploying {} resource(s): {}", contents.size(), contents.keySet());
            DeploymentEvent event = step.execute();

            logger.info("Deployment {} created", event.getKey());

            return Output.builder()
                .deploymentKey(event.getKey())
                .tenantId(event.getTenantId())
                .processes(event.getProcesses().stream()
                    .map(process -> DeployedProcess.builder()
                        .bpmnProcessId(process.getBpmnProcessId())
                        .processDefinitionKey(process.getProcessDefinitionKey())
                        .version(process.getVersion())
                        .resourceName(process.getResourceName())
                        .build()
                    )
                    .toList()
                )
                .decisions(event.getDecisions().stream()
                    .map(decision -> DeployedDecision.builder()
                        .dmnDecisionId(decision.getDmnDecisionId())
                        .dmnDecisionName(decision.getDmnDecisionName())
                        .decisionKey(decision.getDecisionKey())
                        .version(decision.getVersion())
                        .build()
                    )
                    .toList()
                )
                .forms(event.getForm().stream()
                    .map(form -> DeployedForm.builder()
                        .formId(form.getFormId())
                        .formKey(form.getFormKey())
                        .version(form.getVersion())
                        .resourceName(form.getResourceName())
                        .build()
                    )
                    .toList()
                )
                .build();
        }
    }

    private static String validateResourceName(String name) {
        var lowerCase = name.toLowerCase();
        if (ALLOWED_EXTENSIONS.stream().noneMatch(lowerCase::endsWith)) {
            throw new IllegalArgumentException(
                "Resource name '" + name + "' must end with one of " + ALLOWED_EXTENSIONS + ", Camunda derives the resource type from the extension"
            );
        }
        return name;
    }

    private static String resolveContent(RunContext runContext, String value) throws IOException {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Resource content cannot be empty");
        }

        if (!value.startsWith("kestra://")) {
            return value;
        }

        try (InputStream is = runContext.storage().getFile(URI.create(value))) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(title = "Key of the created deployment")
        private final Long deploymentKey;

        @Schema(title = "Tenant the deployment belongs to")
        private final String tenantId;

        @Schema(title = "Processes deployed by this command")
        private final List<DeployedProcess> processes;

        @Schema(title = "Decisions deployed by this command")
        private final List<DeployedDecision> decisions;

        @Schema(title = "Forms deployed by this command")
        private final List<DeployedForm> forms;
    }

    @Builder
    @Getter
    public static class DeployedProcess {

        @Schema(title = "BPMN process ID, the `id` attribute of the process element")
        private final String bpmnProcessId;

        @Schema(title = "Key assigned to this process definition")
        private final Long processDefinitionKey;

        @Schema(title = "Version of the process definition")
        private final Integer version;

        @Schema(title = "Name of the deployed resource")
        private final String resourceName;
    }

    @Builder
    @Getter
    public static class DeployedDecision {

        @Schema(title = "DMN decision ID")
        private final String dmnDecisionId;

        @Schema(title = "DMN decision name")
        private final String dmnDecisionName;

        @Schema(title = "Key assigned to this decision")
        private final Long decisionKey;

        @Schema(title = "Version of the decision")
        private final Integer version;
    }

    @Builder
    @Getter
    public static class DeployedForm {

        @Schema(title = "Form ID")
        private final String formId;

        @Schema(title = "Key assigned to this form")
        private final Long formKey;

        @Schema(title = "Version of the form")
        private final Integer version;

        @Schema(title = "Name of the deployed resource")
        private final String resourceName;
    }
}
