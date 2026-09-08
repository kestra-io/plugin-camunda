package io.kestra.plugin.camunda;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class TriggerTest {

    @Inject
    RunContextFactory runContextFactory;

    @BeforeAll
    static void startCluster() {
        CamundaTestCluster.start();
    }

    @Test
    void activatesJob_thenReleasesTheStreamOnStop() throws Exception {
        deployProcess();

        var trigger = Trigger.builder()
            .id("on-camunda-job")
            .type(Trigger.class.getName())
            .restAddress(Property.ofValue(CamundaTestCluster.restAddress()))
            .jobType(Property.ofValue("kestra-notify"))
            .timeout(Property.ofValue(Duration.ofMinutes(2)))
            .fetchVariables(Property.ofValue(List.of("orderId")))
            .build();

        var jobs = new ConcurrentLinkedQueue<Job>();
        var completed = new AtomicBoolean(false);

        var subscription = Flux.from(trigger.publisher(runContextFactory.of()))
            .subscribeOn(Schedulers.boundedElastic())
            .doOnComplete(() -> completed.set(true))
            .subscribe(jobs::add);

        try {
            var created = createProcessInstance();

            await().atMost(Duration.ofSeconds(30)).until(() -> !jobs.isEmpty());

            var job = jobs.poll();
            assertThat(job, notNullValue());
            assertThat(job.getJobKey(), greaterThan(0L));
            assertThat(job.getType(), is("kestra-notify"));
            assertThat(job.getElementId(), is("notify"));
            assertThat(job.getBpmnProcessId(), is("kestra-order-fulfillment"));
            assertThat(job.getProcessInstanceKey(), is(created.getProcessInstanceKey()));
            assertThat(job.getVariables(), hasEntry("orderId", "ORD-123"));
            assertThat(job.getDeadline(), notNullValue());

            // the trigger leaves the job activated, the flow reports the outcome
            CompleteJob.builder()
                .id("complete")
                .type(CompleteJob.class.getName())
                .restAddress(Property.ofValue(CamundaTestCluster.restAddress()))
                .jobKey(Property.ofValue(job.getJobKey()))
                .variables(Property.ofValue(Map.of("notified", true)))
                .build()
                .run(runContextFactory.of());

            trigger.stop();

            await().atMost(Duration.ofSeconds(30)).untilTrue(completed);
        } finally {
            trigger.kill();
            subscription.dispose();
        }
    }

    private void deployProcess() throws Exception {
        Deploy.builder()
            .id("deploy")
            .type(Deploy.class.getName())
            .restAddress(Property.ofValue(CamundaTestCluster.restAddress()))
            .resources(Property.ofValue(Map.of("order-fulfillment.bpmn", CamundaTaskTest.resource("order-fulfillment.bpmn"))))
            .build()
            .run(runContextFactory.of());
    }

    private CreateProcessInstance.Output createProcessInstance() throws Exception {
        return CreateProcessInstance.builder()
            .id("create")
            .type(CreateProcessInstance.class.getName())
            .restAddress(Property.ofValue(CamundaTestCluster.restAddress()))
            .processId(Property.ofValue("kestra-order-fulfillment"))
            .variables(Property.ofValue(Map.of("orderId", "ORD-123")))
            .build()
            .run(runContextFactory.of());
    }
}
