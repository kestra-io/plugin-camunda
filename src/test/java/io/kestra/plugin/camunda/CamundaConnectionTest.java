package io.kestra.plugin.camunda;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
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
    void saas_cannotBeCombinedWithExplicitAddresses() {
        var task = builder()
            .clusterId(Property.ofValue("cluster-id"))
            .clientId(Property.ofValue("client"))
            .clientSecret(Property.ofValue("secret"))
            .restAddress(Property.ofValue("http://localhost:8080"))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(exception.getMessage(), containsString("cannot be combined with `clusterId`"));
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
