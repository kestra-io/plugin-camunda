package io.kestra.plugin.camunda;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Authentication configuration is rejected before any connection is opened, so these run without a cluster.
 */
@KestraTest
class CamundaConnectionTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void basicAndOauth_areMutuallyExclusive() {
        var task = builder()
            .username(Property.ofValue("demo"))
            .password(Property.ofValue("demo"))
            .clientId(Property.ofValue("client"))
            .clientSecret(Property.ofValue("secret"))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(exception.getMessage(), containsString("mutually exclusive"));
    }

    @Test
    void basicAuth_requiresBothUsernameAndPassword() {
        var task = builder()
            .username(Property.ofValue("demo"))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(exception.getMessage(), containsString("`username` and `password`"));
    }

    @Test
    void oauth_requiresBothClientIdAndSecret() {
        var task = builder()
            .clientId(Property.ofValue("client"))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(exception.getMessage(), containsString("`clientId` and `clientSecret`"));
    }

    @Test
    void selfManagedOauth_requiresAuthorizationServerUrl() {
        var task = builder()
            .clientId(Property.ofValue("client"))
            .clientSecret(Property.ofValue("secret"))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(exception.getMessage(), containsString("`authorizationServerUrl` is required"));
    }

    @Test
    void saas_requiresClientCredentials() {
        var task = builder()
            .clusterId(Property.ofValue("cluster-id"))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(exception.getMessage(), containsString("alongside `clusterId`"));
    }

    @Test
    void saas_explicitAddressOverridesTheDerivedOne() throws Exception {
        // The cloud builder's build() calls determineRestAddress() and overwrites whatever was set, so
        // an override only survives by not using it. Camunda derives
        // https://<region>.zeebe.camunda.io:443/<clusterId>, which 404s on a cluster serving REST at
        // /v2 on the gRPC host, and this is the only way to point at the right one.
        var override = "https://cluster-id.sin-2.zeebe.camunda.io";
        var task = builder()
            .clusterId(Property.ofValue("cluster-id"))
            .region(Property.ofValue("sin-2"))
            .clientId(Property.ofValue("client"))
            .clientSecret(Property.ofValue("secret"))
            .restAddress(Property.ofValue(override))
            .build();

        try (var client = task.camundaClient(runContextFactory.of())) {
            assertThat(client.getConfiguration().getRestAddress().toString(), is(override));
        }
    }

    @Test
    void selfManagedOauth_requiresAudience() {
        // the SDK's own validate() does requireNonNull on audience, and environment overrides are
        // disabled, so without this guard the user gets an NPE instead of a configuration error
        var task = builder()
            .restAddress(Property.ofValue("http://localhost:8080"))
            .clientId(Property.ofValue("client"))
            .clientSecret(Property.ofValue("secret"))
            .authorizationServerUrl(Property.ofValue("http://localhost:18080/token"))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(exception.getMessage(), containsString("`audience` is required"));
    }

    @Test
    void transportRest_withoutRestAddress_isRejected() {
        // would otherwise send REST to the client default, http://0.0.0.0:8080
        var task = builder()
            .grpcAddress(Property.ofValue("http://localhost:26500"))
            .transport(Property.ofValue(Transport.REST))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(exception.getMessage(), containsString("`transport: REST` requires `restAddress`"));
    }

    @Test
    void transportGrpc_withoutGrpcAddress_isRejected() {
        var task = builder()
            .restAddress(Property.ofValue("http://localhost:8080"))
            .transport(Property.ofValue(Transport.GRPC))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(exception.getMessage(), containsString("`transport: GRPC` requires `grpcAddress`"));
    }

    @Test
    void transportOnSaas_needsNoAddressBecauseBothAreDerived() {
        // clusterId with no addresses derives both, so either transport is reachable
        var task = builder()
            .clusterId(Property.ofValue("cluster-id"))
            .clientId(Property.ofValue("client"))
            .clientSecret(Property.ofValue("secret"))
            .transport(Property.ofValue(Transport.GRPC))
            .build();

        assertDoesNotThrow(() -> task.camundaClient(runContextFactory.of()).close());
    }

    @Test
    void transportRestOnSaas_withoutRestAddress_namesTheConsole() {
        // gRPC works from a bare SaaS config but REST does not, so asking for REST without an address
        // must say where to get one instead of 404ing at command time
        var task = builder()
            .clusterId(Property.ofValue("cluster-id"))
            .clientId(Property.ofValue("client"))
            .clientSecret(Property.ofValue("secret"))
            .transport(Property.ofValue(Transport.REST))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(exception.getMessage(), containsString("Camunda Console"));
    }

    @Test
    void noAddressAtAll_isRejectedRatherThanDefaultingToLoopback() {
        var task = builder().build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(exception.getMessage(), containsString("One of `restAddress`, `grpcAddress` or `clusterId`"));
    }

    @Test
    void saas_cannotBeCombinedWithSelfManagedOauthProperties() {
        var task = builder()
            .clusterId(Property.ofValue("cluster-id"))
            .clientId(Property.ofValue("client"))
            .clientSecret(Property.ofValue("secret"))
            .audience(Property.ofValue("zeebe-api"))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(exception.getMessage(), containsString("cannot be combined with `clusterId`"));
    }

    @Test
    void blankCredential_countsAsUnsetRatherThanHalfConfigured() throws Exception {
        // `password: "{{ inputs.maybeEmpty }}"` rendering to "" must not switch Basic auth on, which
        // would otherwise fail the "both must be set" guard or send an empty credential
        var task = builder()
            .restAddress(Property.ofValue("http://localhost:8080"))
            .password(Property.ofValue("   "))
            .build();

        // no auth mode is configured, so building the client succeeds and only the command would fail
        task.camundaClient(runContextFactory.of()).close();
    }

    private static CompleteJob.CompleteJobBuilder<?, ?> builder() {
        return CompleteJob.builder()
            .id("auth-test")
            .type(CompleteJob.class.getName())
            .jobKey(Property.ofValue(1L));
    }
}
