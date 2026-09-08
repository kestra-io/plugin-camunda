package io.kestra.plugin.camunda;

import io.camunda.client.CamundaClient;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

/**
 * Connection and authentication properties shared by every Camunda task and trigger.
 *
 * Implementations declare the fields, {@link CamundaClientFactory} turns them into a client.
 */
public interface CamundaConnectionInterface {

    Property<String> getRestAddress();

    Property<String> getGrpcAddress();

    Property<String> getUsername();

    Property<String> getPassword();

    Property<String> getClientId();

    Property<String> getClientSecret();

    Property<String> getAuthorizationServerUrl();

    Property<String> getAudience();

    Property<String> getClusterId();

    Property<String> getRegion();

    Property<String> getTenantId();

    /**
     * Builds a client. The caller owns it and must close it.
     */
    default CamundaClient camundaClient(RunContext runContext) throws IllegalVariableEvaluationException {
        return CamundaClientFactory.of(runContext, this);
    }
}
