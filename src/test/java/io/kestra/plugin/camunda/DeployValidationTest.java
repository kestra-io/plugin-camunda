package io.kestra.plugin.camunda;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Resource validation happens before the client is built, so these run without a cluster.
 */
@KestraTest
class DeployValidationTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void emptyResources_isRejected() {
        var task = Deploy.builder()
            .id("deploy-test")
            .type(Deploy.class.getName())
            .resources(Property.ofValue(Map.of()))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(exception.getMessage(), containsString("at least one resource"));
    }

    @Test
    void unknownResourceExtension_isRejected() {
        var task = Deploy.builder()
            .id("deploy-test")
            .type(Deploy.class.getName())
            .resources(Property.ofValue(Map.of("order-fulfillment.txt", "<definitions/>")))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(exception.getMessage(), containsString("order-fulfillment.txt"));
    }

    @Test
    void blankResourceContent_isRejected() {
        var task = Deploy.builder()
            .id("deploy-test")
            .type(Deploy.class.getName())
            .resources(Property.ofValue(Map.of("order-fulfillment.bpmn", "  ")))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(exception.getMessage(), containsString("cannot be empty"));
    }
}
