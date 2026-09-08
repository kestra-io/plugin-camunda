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
        var config = Config.of(runContext, connection);
        config.validate();

        CamundaClientBuilder builder = config.isSaas() ? cloudBuilder(config) : selfManagedBuilder(config);

        if (config.tenantId() != null) {
            builder.defaultTenantId(config.tenantId());
        }

        // the SDK reads CAMUNDA_*/ZEEBE_* environment variables by default, which would let the
        // worker environment silently override what the flow declares
        return builder.applyEnvironmentVariableOverrides(false).build();
    }

    private static CamundaClientBuilder cloudBuilder(Config config) {
        var builder = CamundaClient.newCloudClientBuilder()
            .withClusterId(config.clusterId())
            .withClientId(config.clientId())
            .withClientSecret(config.clientSecret());

        return config.region() != null ? builder.withRegion(config.region()) : builder;
    }

    private static CamundaClientBuilder selfManagedBuilder(Config config) {
        var builder = CamundaClient.newClientBuilder();

        if (config.restAddress() != null) {
            builder.restAddress(URI.create(config.restAddress()));
        }
        if (config.grpcAddress() != null) {
            builder.grpcAddress(URI.create(config.grpcAddress()));
        }

        if (config.isBasic()) {
            builder.credentialsProvider(CredentialsProvider.newBasicAuthCredentialsProviderBuilder()
                .applyEnvironmentOverrides(false)
                .username(config.username())
                .password(config.password())
                .build()
            );
        } else if (config.isOauth()) {
            var oauthBuilder = CredentialsProvider.newCredentialsProviderBuilder()
                .applyEnvironmentOverrides(false)
                .clientId(config.clientId())
                .clientSecret(config.clientSecret())
                .authorizationServerUrl(config.authorizationServerUrl());

            if (config.audience() != null) {
                oauthBuilder.audience(config.audience());
            }

            builder.credentialsProvider(oauthBuilder.build());
        }

        return builder;
    }

    /**
     * The rendered connection properties. A blank property is normalised to {@code null}, so
     * "is it set" is a single null check everywhere below.
     */
    private record Config(
        String restAddress,
        String grpcAddress,
        String username,
        String password,
        String clientId,
        String clientSecret,
        String authorizationServerUrl,
        String audience,
        String clusterId,
        String region,
        String tenantId
    ) {

        static Config of(RunContext runContext, CamundaConnectionInterface connection) throws IllegalVariableEvaluationException {
            return new Config(
                render(runContext, connection.getRestAddress()),
                render(runContext, connection.getGrpcAddress()),
                render(runContext, connection.getUsername()),
                render(runContext, connection.getPassword()),
                render(runContext, connection.getClientId()),
                render(runContext, connection.getClientSecret()),
                render(runContext, connection.getAuthorizationServerUrl()),
                render(runContext, connection.getAudience()),
                render(runContext, connection.getClusterId()),
                render(runContext, connection.getRegion()),
                render(runContext, connection.getTenantId())
            );
        }

        boolean isBasic() {
            return username != null || password != null;
        }

        boolean isOauth() {
            return clientId != null || clientSecret != null;
        }

        boolean isSaas() {
            return clusterId != null;
        }

        void validate() {
            if (isBasic() && isOauth()) {
                throw new IllegalArgumentException("`username`/`password` and `clientId`/`clientSecret` are mutually exclusive, pick one authentication mode");
            }
            if (isBasic() && (username == null || password == null)) {
                throw new IllegalArgumentException("`username` and `password` must both be set for Basic authentication");
            }
            if (isOauth() && (clientId == null || clientSecret == null)) {
                throw new IllegalArgumentException("`clientId` and `clientSecret` must both be set for OAuth2 authentication");
            }
            if (isSaas() && !isOauth()) {
                throw new IllegalArgumentException("`clientId` and `clientSecret` are required alongside `clusterId` for Camunda SaaS");
            }
            if (isSaas() && (restAddress != null || grpcAddress != null)) {
                throw new IllegalArgumentException("`restAddress`/`grpcAddress` cannot be combined with `clusterId`, SaaS addresses are derived from the cluster ID and region");
            }
            if (isOauth() && !isSaas() && authorizationServerUrl == null) {
                throw new IllegalArgumentException("`authorizationServerUrl` is required for OAuth2 against a self-managed cluster, or set `clusterId` for Camunda SaaS");
            }
        }

        /**
         * A record's generated toString() prints every component, and Lombok's @ToString.Exclude does not
         * apply to records, so it is replaced here to keep credentials out of any future log line.
         */
        @Override
        public String toString() {
            return "Config[restAddress=" + restAddress + ", grpcAddress=" + grpcAddress
                + ", clusterId=" + clusterId + ", region=" + region + ", tenantId=" + tenantId
                + ", authorizationServerUrl=" + authorizationServerUrl + ", audience=" + audience
                + ", credentials=" + (isSaas() || isOauth() ? "oauth" : isBasic() ? "basic" : "none") + "]";
        }

        private static String render(RunContext runContext, Property<String> property) throws IllegalVariableEvaluationException {
            return runContext.render(property)
                .as(String.class)
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .orElse(null);
        }
    }
}
