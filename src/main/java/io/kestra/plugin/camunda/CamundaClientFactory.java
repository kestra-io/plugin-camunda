package io.kestra.plugin.camunda;

import io.camunda.client.CamundaClient;
import io.camunda.client.CamundaClientBuilder;
import io.camunda.client.CredentialsProvider;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

import java.net.URI;

/**
 * Builds a {@link CamundaClient} from the properties of a {@link CamundaConnectionInterface}.
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
final class CamundaClientFactory {

    private CamundaClientFactory() {
    }

    static CamundaClient of(RunContext runContext, CamundaConnectionInterface connection) throws IllegalVariableEvaluationException {
        var restAddress = render(runContext, connection.getRestAddress());
        var grpcAddress = render(runContext, connection.getGrpcAddress());
        var username = render(runContext, connection.getUsername());
        var password = render(runContext, connection.getPassword());
        var clientId = render(runContext, connection.getClientId());
        var clientSecret = render(runContext, connection.getClientSecret());
        var authorizationServerUrl = render(runContext, connection.getAuthorizationServerUrl());
        var audience = render(runContext, connection.getAudience());
        var clusterId = render(runContext, connection.getClusterId());
        var region = render(runContext, connection.getRegion());
        var tenantId = render(runContext, connection.getTenantId());

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

        CamundaClientBuilder builder = clusterId != null
            ? cloudBuilder(clusterId, clientId, clientSecret, region)
            : selfManagedBuilder(restAddress, grpcAddress, basic, username, password, oauth, clientId, clientSecret, authorizationServerUrl, audience);

        if (tenantId != null) {
            builder.defaultTenantId(tenantId);
        }

        // the SDK reads CAMUNDA_*/ZEEBE_* environment variables by default, which would let the
        // worker environment silently override what the flow declares
        return builder.applyEnvironmentVariableOverrides(false).build();
    }

    private static CamundaClientBuilder cloudBuilder(String clusterId, String clientId, String clientSecret, String region) {
        var builder = CamundaClient.newCloudClientBuilder()
            .withClusterId(clusterId)
            .withClientId(clientId)
            .withClientSecret(clientSecret);

        return region != null ? builder.withRegion(region) : builder;
    }

    private static CamundaClientBuilder selfManagedBuilder(
        String restAddress,
        String grpcAddress,
        boolean basic,
        String username,
        String password,
        boolean oauth,
        String clientId,
        String clientSecret,
        String authorizationServerUrl,
        String audience
    ) {
        var builder = CamundaClient.newClientBuilder();

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

        return builder;
    }

    private static String render(RunContext runContext, Property<String> property) throws IllegalVariableEvaluationException {
        return runContext.render(property)
            .as(String.class)
            .map(String::trim)
            .filter(value -> !value.isEmpty())
            .orElse(null);
    }
}
