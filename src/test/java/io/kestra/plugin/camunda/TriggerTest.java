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
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
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
        var process = deployUniqueProcess();

        var trigger = Trigger.builder()
            .id("on-camunda-job")
            .type(Trigger.class.getName())
            .restAddress(Property.ofValue(CamundaTestCluster.restAddress()))
            .jobType(Property.ofValue(process.jobType()))
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
            var created = createProcessInstance(process);

            await().atMost(Duration.ofSeconds(30)).until(() -> !jobs.isEmpty());

            var job = jobs.poll();
            assertThat(job, notNullValue());
            assertThat(job.getJobKey(), greaterThan(0L));
            assertThat(job.getType(), is(process.jobType()));
            assertThat(job.getElementId(), is("notify"));
            assertThat(job.getBpmnProcessId(), is(process.processId()));
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

    @Test
    void activatesJob_forAnExplicitTenant() throws Exception {
        var process = deployUniqueProcess();

        // <default> is the only tenant on a cluster without multi-tenancy, so this asserts the tenant
        // reaches the worker builder at all: the client's defaultTenantId does not configure a worker
        var trigger = Trigger.builder()
            .id("on-camunda-job-tenanted")
            .type(Trigger.class.getName())
            .restAddress(Property.ofValue(CamundaTestCluster.restAddress()))
            .jobType(Property.ofValue(process.jobType()))
            .tenantId(Property.ofValue("<default>"))
            .build();

        var jobs = new ConcurrentLinkedQueue<Job>();
        var subscription = Flux.from(trigger.publisher(runContextFactory.of()))
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(jobs::add);

        try {
            createProcessInstance(process);

            await().atMost(Duration.ofSeconds(30)).until(() -> !jobs.isEmpty());

            var job = jobs.poll();
            assertThat(job, notNullValue());
            assertThat(job.getTenantId(), is("<default>"));
        } finally {
            trigger.kill();
            subscription.dispose();
        }
    }

    @Test
    void killBeforeTheWorkerOpens_returnsInsteadOfWaitingForTermination() {
        var trigger = Trigger.builder()
            .id("never-subscribed")
            .type(Trigger.class.getName())
            .restAddress(Property.ofValue(CamundaTestCluster.restAddress()))
            .jobType(Property.ofValue("kestra-notify"))
            .build();

        // kill() waits for the publisher to release the worker, which never happens here because the
        // publisher was never subscribed, so it must fall through instead of blocking on the latch
        var start = System.nanoTime();
        trigger.kill();
        var elapsed = Duration.ofNanos(System.nanoTime() - start);

        assertThat(elapsed, lessThan(Duration.ofSeconds(5)));
    }

    @Test
    void killWithoutAnOpenClient_isANoOp() {
        CompleteJob.builder()
            .id("never-run")
            .type(CompleteJob.class.getName())
            .jobKey(Property.ofValue(1L))
            .build()
            .kill();
    }

    /**
     * Every worker in these tests watches its own job type. Sharing one would let whichever worker is
     * open activate another test's job, which fails the test that was waiting for it.
     */
    private TestProcess deployUniqueProcess() throws Exception {
        var suffix = UUID.randomUUID().toString().substring(0, 8);
        var process = new TestProcess("kestra-order-fulfillment-" + suffix, "kestra-notify-" + suffix);

        var bpmn = CamundaTaskTest.resource("order-fulfillment.bpmn")
            .replace("kestra-order-fulfillment", process.processId())
            .replace("kestra-notify", process.jobType());

        Deploy.builder()
            .id("deploy")
            .type(Deploy.class.getName())
            .restAddress(Property.ofValue(CamundaTestCluster.restAddress()))
            .resources(Property.ofValue(Map.of(process.processId() + ".bpmn", bpmn)))
            .build()
            .run(runContextFactory.of());

        return process;
    }

    private CreateProcessInstance.Output createProcessInstance(TestProcess process) throws Exception {
        return CreateProcessInstance.builder()
            .id("create")
            .type(CreateProcessInstance.class.getName())
            .restAddress(Property.ofValue(CamundaTestCluster.restAddress()))
            .processId(Property.ofValue(process.processId()))
            .variables(Property.ofValue(Map.of("orderId", "ORD-123")))
            .build()
            .run(runContextFactory.of());
    }

    private record TestProcess(String processId, String jobType) {
    }
}
