package io.kestra.plugin.camunda;

import io.camunda.client.CamundaClient;
import io.camunda.client.CamundaClientBuilder;
import io.camunda.client.CredentialsProvider;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

import java.net.URI;

/**
 * Connection and authentication properties shared by every Camunda task and trigger.
 *
 * The fields are declared by the implementations, because tasks extend {@code Task} and the trigger
 * extends {@code AbstractTrigger}, so there is no shared base class to hold them.
 *
 * Three authentication modes are supported, picked from what is set:
 * <ul>
 *     <li>SaaS: {@code clusterId} + {@code clientId} + {@code clientSecret} (+ optional {@code region})</li>
 *     <li>OAuth2 self-managed: {@code clientId} + {@code clientSecret} + {@code authorizationServerUrl}</li>
 *     <li>Basic: {@code username} + {@code password}</li>
 * </ul>
 * With none of them set the client connects without credentials, which only works against a
 * development cluster that has API protection disabled.
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
        // blank is treated as unset, so "is it configured" stays a single null check below
        var rRestAddress = runContext.render(getRestAddress()).as(String.class).filter(v -> !v.isBlank()).orElse(null);
        var rGrpcAddress = runContext.render(getGrpcAddress()).as(String.class).filter(v -> !v.isBlank()).orElse(null);
        var rUsername = runContext.render(getUsername()).as(String.class).filter(v -> !v.isBlank()).orElse(null);
        var rPassword = runContext.render(getPassword()).as(String.class).filter(v -> !v.isBlank()).orElse(null);
        var rClientId = runContext.render(getClientId()).as(String.class).filter(v -> !v.isBlank()).orElse(null);
        var rClientSecret = runContext.render(getClientSecret()).as(String.class).filter(v -> !v.isBlank()).orElse(null);
        var rAuthorizationServerUrl = runContext.render(getAuthorizationServerUrl()).as(String.class).filter(v -> !v.isBlank()).orElse(null);
        var rAudience = runContext.render(getAudience()).as(String.class).filter(v -> !v.isBlank()).orElse(null);
        var rClusterId = runContext.render(getClusterId()).as(String.class).filter(v -> !v.isBlank()).orElse(null);
        var rRegion = runContext.render(getRegion()).as(String.class).filter(v -> !v.isBlank()).orElse(null);
        var rTenantId = runContext.render(getTenantId()).as(String.class).filter(v -> !v.isBlank()).orElse(null);

        var basic = rUsername != null || rPassword != null;
        var oauth = rClientId != null || rClientSecret != null;

        if (basic && oauth) {
            throw new IllegalArgumentException("`username`/`password` and `clientId`/`clientSecret` are mutually exclusive, pick one authentication mode");
        }
        if (basic && (rUsername == null || rPassword == null)) {
            throw new IllegalArgumentException("`username` and `password` must both be set for Basic authentication");
        }
        if (oauth && (rClientId == null || rClientSecret == null)) {
            throw new IllegalArgumentException("`clientId` and `clientSecret` must both be set for OAuth2 authentication");
        }
        if (rClusterId != null && !oauth) {
            throw new IllegalArgumentException("`clientId` and `clientSecret` are required alongside `clusterId` for Camunda SaaS");
        }
        if (rClusterId != null && (rRestAddress != null || rGrpcAddress != null)) {
            throw new IllegalArgumentException("`restAddress`/`grpcAddress` cannot be combined with `clusterId`, SaaS addresses are derived from the cluster ID and region");
        }
        if (rClusterId != null && (rAuthorizationServerUrl != null || rAudience != null)) {
            throw new IllegalArgumentException("`authorizationServerUrl`/`audience` cannot be combined with `clusterId`, Camunda SaaS derives the OAuth endpoint and audience from the cluster ID and region");
        }
        if (oauth && rClusterId == null && rAuthorizationServerUrl == null) {
            throw new IllegalArgumentException("`authorizationServerUrl` is required for OAuth2 against a self-managed cluster, or set `clusterId` for Camunda SaaS");
        }
        if (oauth && rClusterId == null && rAudience == null) {
            // the SDK's own validate() does requireNonNull on audience, and environment overrides are
            // disabled here, so there is no fallback to CAMUNDA_TOKEN_AUDIENCE to pick it up
            throw new IllegalArgumentException("`audience` is required for OAuth2 against a self-managed cluster, it is the audience claim the identity provider must issue the token for");
        }
        if (rClusterId == null && rRestAddress == null && rGrpcAddress == null) {
            // the client would otherwise fall back to its defaults, http://0.0.0.0:8080 and
            // http://0.0.0.0:26500, and surface a connection error instead of a configuration one
            throw new IllegalArgumentException("One of `restAddress`, `grpcAddress` or `clusterId` is required");
        }

        CamundaClientBuilder builder;

        if (rClusterId != null) {
            var cloud = CamundaClient.newCloudClientBuilder()
                .withClusterId(rClusterId)
                .withClientId(rClientId)
                .withClientSecret(rClientSecret);

            builder = rRegion != null ? cloud.withRegion(rRegion) : cloud;
        } else {
            builder = CamundaClient.newClientBuilder();

            if (rRestAddress != null) {
                builder.restAddress(URI.create(rRestAddress));
            }
            if (rGrpcAddress != null) {
                builder.grpcAddress(URI.create(rGrpcAddress));
            }

            // preferRestOverGrpc defaults to true and each command reads it to pick its transport, so
            // without this a client given only grpcAddress still sends commands to the default REST
            // address, http://0.0.0.0:8080. Configuring one address picks that transport, configuring
            // both leaves the SDK default in place.
            if (rRestAddress != null && rGrpcAddress == null) {
                builder.preferRestOverGrpc(true);
            } else if (rGrpcAddress != null && rRestAddress == null) {
                builder.preferRestOverGrpc(false);
            }
            if (basic) {
                builder.credentialsProvider(CredentialsProvider.newBasicAuthCredentialsProviderBuilder()
                    .applyEnvironmentOverrides(false)
                    .username(rUsername)
                    .password(rPassword)
                    .build()
                );
            } else if (oauth) {
                var oauthBuilder = CredentialsProvider.newCredentialsProviderBuilder()
                    .applyEnvironmentOverrides(false)
                    .clientId(rClientId)
                    .clientSecret(rClientSecret)
                    .authorizationServerUrl(rAuthorizationServerUrl);

                if (rAudience != null) {
                    oauthBuilder.audience(rAudience);
                }

                builder.credentialsProvider(oauthBuilder.build());
            }
        }

        if (rTenantId != null) {
            builder.defaultTenantId(rTenantId);
        }

        // the SDK reads CAMUNDA_*/ZEEBE_* environment variables by default, which would let the
        // worker environment silently override what the flow declares
        return builder.applyEnvironmentVariableOverrides(false).build();
    }
}
