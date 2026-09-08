package io.kestra.plugin.camunda;

import io.camunda.client.CamundaClient;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.WorkerJobLifecycle;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.concurrent.atomic.AtomicReference;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractCamundaTask extends Task implements CamundaConnectionInterface, WorkerJobLifecycle {

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicReference<CamundaClient> runningClient = new AtomicReference<>();

    @Schema(
        title = "REST API base URL of the Camunda cluster",
        description = "For a self-managed cluster, the orchestration cluster address, for example `http://localhost:8080`. Most commands use the REST API by default."
    )
    @PluginProperty(group = "connection")
    private Property<String> restAddress;

    @Schema(
        title = "gRPC gateway address of the Camunda cluster",
        description = "For a self-managed cluster, for example `http://localhost:26500`. Used by the job worker stream of the [Trigger](https://kestra.io/plugins/plugin-camunda/triggers/io.kestra.plugin.camunda.trigger)."
    )
    @PluginProperty(group = "connection")
    private Property<String> grpcAddress;

    @Schema(
        title = "Username for Basic authentication",
        description = "Must be set together with `password`. Mutually exclusive with the OAuth2 properties."
    )
    @PluginProperty(group = "connection")
    private Property<String> username;

    @Schema(
        title = "Password for Basic authentication",
        description = "Must be set together with `username`."
    )
    @ToString.Exclude
    @PluginProperty(group = "connection", secret = true)
    private Property<String> password;

    @Schema(
        title = "OAuth2 client ID",
        description = "Set together with `clientSecret` and either `authorizationServerUrl` (self-managed) or `clusterId` (SaaS)."
    )
    @PluginProperty(group = "connection")
    private Property<String> clientId;

    @Schema(
        title = "OAuth2 client secret",
        description = "Set together with `clientId`."
    )
    @ToString.Exclude
    @PluginProperty(group = "connection", secret = true)
    private Property<String> clientSecret;

    @Schema(
        title = "OAuth2 token endpoint",
        description = "Required for OAuth2 against a self-managed cluster, for example `http://localhost:18080/auth/realms/camunda-platform/protocol/openid-connect/token`."
    )
    @PluginProperty(group = "connection")
    private Property<String> authorizationServerUrl;

    @Schema(
        title = "OAuth2 audience",
        description = "Audience claim requested with the access token. Defaults to whatever the identity provider issues."
    )
    @PluginProperty(group = "connection")
    private Property<String> audience;

    @Schema(
        title = "Camunda SaaS cluster ID",
        description = "Switches the client to SaaS mode, where `restAddress` and `grpcAddress` are derived from the cluster ID and region."
    )
    @PluginProperty(group = "connection")
    private Property<String> clusterId;

    @Schema(
        title = "Camunda SaaS region",
        description = "For example `bru-2`. Only used with `clusterId`."
    )
    @PluginProperty(group = "connection")
    private Property<String> region;

    @Schema(
        title = "Camunda tenant ID",
        description = """
            Camunda's own multi-tenancy identifier, unrelated to the Kestra tenant the flow runs in.
            Applied to every command this task sends. Defaults to `<default>`, which is the only tenant
            on a cluster that does not have multi-tenancy enabled."""
    )
    @PluginProperty(group = "connection")
    private Property<String> tenantId;

    /**
     * Opens a client and keeps a reference to it so that {@link #kill()} can tear it down.
     * Always pair with {@link #closeClient()} in a finally block.
     */
    protected CamundaClient openClient(RunContext runContext) throws IllegalVariableEvaluationException {
        var client = this.camundaClient(runContext);
        this.runningClient.set(client);

        return client;
    }

    /**
     * Closes the client at most once, whichever of the task thread or {@link #kill()} gets there first.
     */
    protected void closeClient() {
        var client = this.runningClient.getAndSet(null);
        if (client != null) {
            client.close();
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Camunda commands block in {@code CamundaFuture.join()}, which ignores thread interruption, so a
     * killed execution would otherwise sit until `requestTimeout` elapses. Closing the client from here
     * shuts the transports down and fails the in-flight command.
     */
    @Override
    public void kill() {
        this.closeClient();
    }
}
