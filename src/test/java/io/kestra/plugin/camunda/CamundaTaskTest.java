package io.kestra.plugin.camunda;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Runs every task against a real Camunda broker, see {@link CamundaTestCluster}.
 */
@KestraTest
class CamundaTaskTest {

    @Inject
    RunContextFactory runContextFactory;

    @BeforeAll
    static void startCluster() {
        CamundaTestCluster.start();
    }

    @Test
    void deploy_processesAndDecisionsInOneCommand() throws Exception {
        var output = deployTestResources();

        assertThat(output.getDeploymentKey(), greaterThan(0L));
        assertThat(output.getProcesses(), hasSize(2));
        assertThat(output.getDecisions(), hasSize(1));
        assertThat(
            output.getDecisions().getFirst().getDmnDecisionId(),
            is("kestra-discount")
        );
    }

    @Test
    void createProcessInstance_awaitingCompletionReturnsVariables() throws Exception {
        deployTestResources();

        var output = CreateProcessInstance.builder()
            .id("create")
            .type(CreateProcessInstance.class.getName())
            .restAddress(Property.ofValue(CamundaTestCluster.restAddress()))
            .processId(Property.ofValue("kestra-instant-quote"))
            .variables(Property.ofValue(Map.of("orderId", "ORD-123")))
            .awaitCompletion(Property.ofValue(true))
            .fetchVariables(Property.ofValue(List.of("orderId")))
            .requestTimeout(Property.ofValue(Duration.ofSeconds(30)))
            .build()
            .run(runContextFactory.of());

        assertThat(output.getProcessInstanceKey(), greaterThan(0L));
        assertThat(output.getBpmnProcessId(), is("kestra-instant-quote"));
        assertThat(output.getVariables(), hasEntry("orderId", "ORD-123"));
    }

    @Test
    void cancelProcessInstance_terminatesAWaitingInstance() throws Exception {
        deployTestResources();

        var created = CreateProcessInstance.builder()
            .id("create")
            .type(CreateProcessInstance.class.getName())
            .restAddress(Property.ofValue(CamundaTestCluster.restAddress()))
            .processId(Property.ofValue("kestra-order-fulfillment"))
            .build()
            .run(runContextFactory.of());

        assertThat(created.getProcessInstanceKey(), greaterThan(0L));

        // the instance waits on the `kestra-notify` service task, so it is still cancellable
        CancelProcessInstance.builder()
            .id("cancel")
            .type(CancelProcessInstance.class.getName())
            .restAddress(Property.ofValue(CamundaTestCluster.restAddress()))
            .processInstanceKey(Property.ofValue(created.getProcessInstanceKey()))
            .build()
            .run(runContextFactory.of());
    }

    @Test
    void publishMessage_returnsAMessageKey() throws Exception {
        var output = PublishMessage.builder()
            .id("publish")
            .type(PublishMessage.class.getName())
            .restAddress(Property.ofValue(CamundaTestCluster.restAddress()))
            .messageName(Property.ofValue("payment-received"))
            .correlationKey(Property.ofValue("ORD-123"))
            .variables(Property.ofValue(Map.of("amount", 99.99)))
            .timeToLive(Property.ofValue(Duration.ofSeconds(5)))
            .build()
            .run(runContextFactory.of());

        assertThat(output.getMessageKey(), greaterThan(0L));
    }

    @Test
    void grpcAddressAlone_sendsCommandsOverGrpc() throws Exception {
        // preferRestOverGrpc defaults to true, so without the transport switch this would go to the
        // default REST address (http://0.0.0.0:8080) and fail with a connection error
        var output = Deploy.builder()
            .id("deploy")
            .type(Deploy.class.getName())
            .grpcAddress(Property.ofValue(CamundaTestCluster.grpcAddress()))
            .resources(Property.ofValue(Map.of("instant-quote.bpmn", resource("instant-quote.bpmn"))))
            .build()
            .run(runContextFactory.of());

        assertThat(output.getDeploymentKey(), greaterThan(0L));
        assertThat(output.getProcesses(), hasSize(1));
    }

    @Test
    void deploy_readsResourceFromInternalStorage() throws Exception {
        var runContext = runContextFactory.of();
        var uri = runContext.storage().putFile(
            new java.io.ByteArrayInputStream(resource("instant-quote.bpmn").getBytes(StandardCharsets.UTF_8)),
            "instant-quote.bpmn"
        );

        var output = Deploy.builder()
            .id("deploy")
            .type(Deploy.class.getName())
            .restAddress(Property.ofValue(CamundaTestCluster.restAddress()))
            .resources(Property.ofValue(Map.of("instant-quote.bpmn", uri.toString())))
            .build()
            .run(runContext);

        assertThat(output.getProcesses(), hasSize(1));
        assertThat(
            output.getProcesses().stream().map(Deploy.DeployedProcess::getBpmnProcessId).toList(),
            contains("kestra-instant-quote")
        );
    }

    private Deploy.Output deployTestResources() throws Exception {
        var output = Deploy.builder()
            .id("deploy")
            .type(Deploy.class.getName())
            .restAddress(Property.ofValue(CamundaTestCluster.restAddress()))
            .resources(Property.ofValue(Map.of(
                "order-fulfillment.bpmn", resource("order-fulfillment.bpmn"),
                "instant-quote.bpmn", resource("instant-quote.bpmn"),
                "discount.dmn", resource("discount.dmn")
            )))
            .build()
            .run(runContextFactory.of());

        assertThat(output, notNullValue());
        return output;
    }

    static String resource(String name) throws IOException {
        try (var is = CamundaTaskTest.class.getClassLoader().getResourceAsStream(name)) {
            if (is == null) {
                throw new IOException("Test resource " + name + " not found");
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
