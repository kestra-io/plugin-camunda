package io.kestra.plugin.camunda;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class CreateProcessInstanceValidationTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void neitherProcessIdNorDefinitionKey_isRejected() {
        var task = CreateProcessInstance.builder()
            .id("create-test")
            .type(CreateProcessInstance.class.getName())
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(exception.getMessage(), containsString("Either `processId` or `processDefinitionKey`"));
    }

    @Test
    void bothProcessIdAndDefinitionKey_isRejected() {
        var task = CreateProcessInstance.builder()
            .id("create-test")
            .type(CreateProcessInstance.class.getName())
            .processId(Property.ofValue("kestra-order-fulfillment"))
            .processDefinitionKey(Property.ofValue(1L))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(exception.getMessage(), containsString("mutually exclusive"));
    }

    @Test
    void processVersionWithoutProcessId_isRejected() {
        var task = CreateProcessInstance.builder()
            .id("create-test")
            .type(CreateProcessInstance.class.getName())
            .processDefinitionKey(Property.ofValue(1L))
            .processVersion(Property.ofValue(2))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(exception.getMessage(), containsString("`processVersion` can only be used with `processId`"));
    }
}
