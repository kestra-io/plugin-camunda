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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Covers what the single-service-task fixture cannot: that a published message actually correlates to a
 * waiting instance, that CompleteJob's variables reach the process instance, and that FailJob decrements
 * retries where a lock expiry would not.
 */
@KestraTest
class MessageAndJobOutcomeTest {

    @Inject
    RunContextFactory runContextFactory;

    @BeforeAll
    static void startCluster() {
        CamundaTestCluster.start();
    }

    @Test
    void publishedMessageCorrelates_andCompleteJobVariablesReachTheInstance() throws Exception {
        var f = deployMessageFlow();

        // the instance parks on the message catch event, so no job exists yet
        var created = createInstance(f, "ORD-" + f.suffix());
        var firstJobs = new ConcurrentLinkedQueue<Job>();
        var firstTrigger = trigger(f.firstJobType());
        var firstSub = Flux.from(firstTrigger.publisher(runContextFactory.of()))
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(firstJobs::add);

        try {
            // nothing should arrive until the message correlates
            Thread.sleep(2000);
            assertThat(firstJobs.isEmpty(), is(true));

            var published = PublishMessage.builder()
                .id("publish")
                .type(PublishMessage.class.getName())
                .restAddress(Property.ofValue(CamundaTestCluster.restAddress()))
                .messageName(Property.ofValue(f.messageName()))
                .correlationKey(Property.ofValue("ORD-" + f.suffix()))
                .variables(Property.ofValue(Map.of("amount", 42)))
                .build()
                .run(runContextFactory.of());

            assertThat(published.getMessageKey(), greaterThan(0L));

            // the message advanced this exact instance to the first service task
            await().atMost(Duration.ofSeconds(30)).until(() -> !firstJobs.isEmpty());
            var firstJob = firstJobs.poll();
            assertThat(firstJob, notNullValue());
            assertThat(firstJob.getProcessInstanceKey(), is(created.getProcessInstanceKey()));
            assertThat(firstJob.getElementId(), is("first"));
            // variables published with the message were merged in
            assertThat(firstJob.getVariables(), hasEntry("amount", 42));

            CompleteJob.builder()
                .id("complete")
                .type(CompleteJob.class.getName())
                .restAddress(Property.ofValue(CamundaTestCluster.restAddress()))
                .jobKey(Property.ofValue(firstJob.getJobKey()))
                .variables(Property.ofValue(Map.of("firstDone", true)))
                .build()
                .run(runContextFactory.of());

            // the second job proves CompleteJob's variables landed on the instance
            var secondJobs = new ConcurrentLinkedQueue<Job>();
            var secondTrigger = trigger(f.secondJobType());
            var secondSub = Flux.from(secondTrigger.publisher(runContextFactory.of()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(secondJobs::add);

            try {
                await().atMost(Duration.ofSeconds(30)).until(() -> !secondJobs.isEmpty());
                var secondJob = secondJobs.poll();
                assertThat(secondJob, notNullValue());
                assertThat(secondJob.getElementId(), is("second"));
                assertThat(secondJob.getVariables(), hasEntry("firstDone", true));
            } finally {
                secondTrigger.kill();
                secondSub.dispose();
            }
        } finally {
            firstTrigger.kill();
            firstSub.dispose();
        }
    }

    @Test
    void failJobDecrementsRetries_whereALockExpiryWouldNot() throws Exception {
        var f = deployMessageFlow();
        createInstance(f, "ORD-" + f.suffix());

        PublishMessage.builder()
            .id("publish")
            .type(PublishMessage.class.getName())
            .restAddress(Property.ofValue(CamundaTestCluster.restAddress()))
            .messageName(Property.ofValue(f.messageName()))
            .correlationKey(Property.ofValue("ORD-" + f.suffix()))
            .build()
            .run(runContextFactory.of());

        var jobs = new ConcurrentLinkedQueue<Job>();
        var first = trigger(f.firstJobType());
        var sub = Flux.from(first.publisher(runContextFactory.of()))
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(jobs::add);

        Long jobKey;
        int retriesBefore;
        try {
            await().atMost(Duration.ofSeconds(30)).until(() -> !jobs.isEmpty());
            var job = jobs.poll();
            assertThat(job, notNullValue());
            jobKey = job.getJobKey();
            retriesBefore = job.getRetries();
            assertThat(retriesBefore, greaterThan(0));
        } finally {
            first.kill();
            sub.dispose();
        }

        FailJob.builder()
            .id("fail")
            .type(FailJob.class.getName())
            .restAddress(Property.ofValue(CamundaTestCluster.restAddress()))
            .jobKey(Property.ofValue(jobKey))
            .retries(Property.ofValue(retriesBefore - 1))
            .errorMessage(Property.ofValue("deliberate failure"))
            .variables(Property.ofValue(Map.of("failed", true)))
            .build()
            .run(runContextFactory.of());

        // the job comes back with one retry fewer, which a lock expiry alone never does
        var reactivated = new ConcurrentLinkedQueue<Job>();
        var again = trigger(f.firstJobType());
        var againSub = Flux.from(again.publisher(runContextFactory.of()))
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(reactivated::add);

        try {
            await().atMost(Duration.ofSeconds(30)).until(() -> !reactivated.isEmpty());
            var job = reactivated.poll();
            assertThat(job, notNullValue());
            assertThat(job.getJobKey(), is(jobKey));
            assertThat(job.getRetries(), is(retriesBefore - 1));
            assertThat(job.getVariables(), hasEntry("failed", true));
        } finally {
            again.kill();
            againSub.dispose();
        }
    }

    @Test
    void fetchVariables_excludesWhatWasNotAskedFor() throws Exception {
        var f = deployMessageFlow();
        var orderId = "ORD-" + f.suffix();
        createInstance(f, orderId);

        PublishMessage.builder()
            .id("publish")
            .type(PublishMessage.class.getName())
            .restAddress(Property.ofValue(CamundaTestCluster.restAddress()))
            .messageName(Property.ofValue(f.messageName()))
            .correlationKey(Property.ofValue(orderId))
            .variables(Property.ofValue(Map.of("amount", 42, "unwanted", "noise")))
            .build()
            .run(runContextFactory.of());

        var jobs = new ConcurrentLinkedQueue<Job>();
        var trigger = Trigger.builder()
            .id("fetch-" + f.suffix())
            .type(Trigger.class.getName())
            .restAddress(Property.ofValue(CamundaTestCluster.restAddress()))
            .jobType(Property.ofValue(f.firstJobType()))
            .timeout(Property.ofValue(Duration.ofMinutes(2)))
            .fetchVariables(Property.ofValue(java.util.List.of("amount")))
            .build();

        var sub = Flux.from(trigger.publisher(runContextFactory.of()))
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(jobs::add);

        try {
            await().atMost(Duration.ofSeconds(30)).until(() -> !jobs.isEmpty());
            var job = jobs.poll();
            assertThat(job, notNullValue());
            assertThat(job.getVariables(), hasEntry("amount", 42));
            // orderId and unwanted exist on the instance but were not requested
            assertThat(job.getVariables(), not(hasKey("unwanted")));
            assertThat(job.getVariables(), not(hasKey("orderId")));
        } finally {
            trigger.kill();
            sub.dispose();
        }
    }

    private Trigger trigger(String jobType) {
        return Trigger.builder()
            .id("on-" + jobType)
            .type(Trigger.class.getName())
            .restAddress(Property.ofValue(CamundaTestCluster.restAddress()))
            .jobType(Property.ofValue(jobType))
            .timeout(Property.ofValue(Duration.ofMinutes(2)))
            .build();
    }

    private CreateProcessInstance.Output createInstance(MessageFlow f, String orderId) throws Exception {
        return CreateProcessInstance.builder()
            .id("create")
            .type(CreateProcessInstance.class.getName())
            .restAddress(Property.ofValue(CamundaTestCluster.restAddress()))
            .processId(Property.ofValue(f.processId()))
            .variables(Property.ofValue(Map.of("orderId", orderId)))
            .build()
            .run(runContextFactory.of());
    }

    /** Unique ids per test, so concurrent workers and buffered messages cannot cross over. */
    private MessageFlow deployMessageFlow() throws Exception {
        var suffix = UUID.randomUUID().toString().substring(0, 8);
        var f = new MessageFlow(
            suffix,
            "kestra-message-flow-" + suffix,
            "kestra-payment-received-" + suffix,
            "kestra-first-" + suffix,
            "kestra-second-" + suffix
        );

        var bpmn = CamundaTaskTest.resource("message-flow.bpmn")
            .replace("kestra-message-flow", f.processId())
            .replace("kestra-payment-received", f.messageName())
            .replace("kestra-first", f.firstJobType())
            .replace("kestra-second", f.secondJobType());

        Deploy.builder()
            .id("deploy")
            .type(Deploy.class.getName())
            .restAddress(Property.ofValue(CamundaTestCluster.restAddress()))
            .resources(Property.ofValue(Map.of(f.processId() + ".bpmn", bpmn)))
            .build()
            .run(runContextFactory.of());

        return f;
    }

    private record MessageFlow(
        String suffix,
        String processId,
        String messageName,
        String firstJobType,
        String secondJobType
    ) {
    }
}
