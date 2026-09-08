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
        var restAddress = render(runContext, getRestAddress());
        var grpcAddress = render(runContext, getGrpcAddress());
        var username = render(runContext, getUsername());
        var password = render(runContext, getPassword());
        var clientId = render(runContext, getClientId());
        var clientSecret = render(runContext, getClientSecret());
        var authorizationServerUrl = render(runContext, getAuthorizationServerUrl());
        var audience = render(runContext, getAudience());
        var clusterId = render(runContext, getClusterId());
        var region = render(runContext, getRegion());
        var tenantId = render(runContext, getTenantId());

        var basic = username != null || password != null;
        var oauth = clientId != null || clientSecret != null;

        if (basic && oauth) {
            throw new IllegalArgumentException("`username`/`password` and `clientId`/`clientSecret` are mutually exclusive, pick one authentication mode");
        }
        if (basic && (username == null || password == null)) {
            throw new IllegalArgumentException("`username` and `password` must both be set for Basic authentication");
        }
        if (oauth && (clientId == null || clientSecret == null)) {
            throw new IllegalArgumentException("`clientId` and `clientSecret` must both be set for OAuth2 authentication");
        }
        if (clusterId != null && !oauth) {
            throw new IllegalArgumentException("`clientId` and `clientSecret` are required alongside `clusterId` for Camunda SaaS");
        }
        if (clusterId != null && (restAddress != null || grpcAddress != null)) {
            throw new IllegalArgumentException("`restAddress`/`grpcAddress` cannot be combined with `clusterId`, SaaS addresses are derived from the cluster ID and region");
        }
        if (oauth && clusterId == null && authorizationServerUrl == null) {
            throw new IllegalArgumentException("`authorizationServerUrl` is required for OAuth2 against a self-managed cluster, or set `clusterId` for Camunda SaaS");
        }

        CamundaClientBuilder builder;

        if (clusterId != null) {
            var cloud = CamundaClient.newCloudClientBuilder()
                .withClusterId(clusterId)
                .withClientId(clientId)
                .withClientSecret(clientSecret);

            builder = region != null ? cloud.withRegion(region) : cloud;
        } else {
            builder = CamundaClient.newClientBuilder();

            if (restAddress != null) {
                builder.restAddress(URI.create(restAddress));
            }
            if (grpcAddress != null) {
                builder.grpcAddress(URI.create(grpcAddress));
            }
            if (basic) {
                builder.credentialsProvider(CredentialsProvider.newBasicAuthCredentialsProviderBuilder()
                    .applyEnvironmentOverrides(false)
                    .username(username)
                    .password(password)
                    .build()
                );
            } else if (oauth) {
                var oauthBuilder = CredentialsProvider.newCredentialsProviderBuilder()
                    .applyEnvironmentOverrides(false)
                    .clientId(clientId)
                    .clientSecret(clientSecret)
                    .authorizationServerUrl(authorizationServerUrl);

                if (audience != null) {
                    oauthBuilder.audience(audience);
                }

                builder.credentialsProvider(oauthBuilder.build());
            }
        }

        if (tenantId != null) {
            builder.defaultTenantId(tenantId);
        }

        // the SDK reads CAMUNDA_*/ZEEBE_* environment variables by default, which would let the
        // worker environment silently override what the flow declares
        return builder.applyEnvironmentVariableOverrides(false).build();
    }

    /** Blank is treated as unset, so "is it configured" stays a single null check above. */
    private static String render(RunContext runContext, Property<String> property) throws IllegalVariableEvaluationException {
        return runContext.render(property)
            .as(String.class)
            .map(String::trim)
            .filter(value -> !value.isEmpty())
            .orElse(null);
    }
}
