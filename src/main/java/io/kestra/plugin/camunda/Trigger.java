package io.kestra.plugin.camunda;

import io.camunda.client.CamundaClient;
import io.camunda.client.api.worker.JobWorker;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.RealtimeTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.reactivestreams.Publisher;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Start a flow for each Camunda job of a given type",
    description = """
        Holds a Camunda job worker open and starts one execution per activated job, so a Kestra flow can implement a BPMN service task.
        The job is not completed by the trigger: the flow decides the outcome and reports it with the [CompleteJob](https://kestra.io/plugins/plugin-camunda/tasks/io.kestra.plugin.camunda.completejob) task, using `{{ trigger.jobKey }}`.
        A job the flow never completes stays locked until `timeout` elapses, after which Camunda hands it to a worker again, so keep `timeout` above the expected flow duration.
        Job activation uses the same transport as the tasks, which is the REST API when `restAddress` is set. Set `grpcAddress` together with `streamEnabled` for push-based streaming instead of long polling."""
)
@Plugin(
    examples = {
        @Example(
            title = "React to Camunda jobs of a given type and complete them from the flow.",
            full = true,
            code = """
                id: handle_camunda_job
                namespace: company.team

                triggers:
                  - id: on_camunda_job
                    type: io.kestra.plugin.camunda.Trigger
                    restAddress: "{{ secret('CAMUNDA_REST_ADDRESS') }}"
                    clientId: "{{ secret('CAMUNDA_CLIENT_ID') }}"
                    clientSecret: "{{ secret('CAMUNDA_CLIENT_SECRET') }}"
                    authorizationServerUrl: "{{ secret('CAMUNDA_AUTH_SERVER_URL') }}"
                    jobType: send-notification
                    timeout: PT5M

                tasks:
                  - id: handle_job
                    type: io.kestra.plugin.core.log.Log
                    message: "Handling Camunda job {{ trigger.jobKey }} for process instance {{ trigger.processInstanceKey }}"

                  - id: complete_job
                    type: io.kestra.plugin.camunda.CompleteJob
                    restAddress: "{{ secret('CAMUNDA_REST_ADDRESS') }}"
                    clientId: "{{ secret('CAMUNDA_CLIENT_ID') }}"
                    clientSecret: "{{ secret('CAMUNDA_CLIENT_SECRET') }}"
                    authorizationServerUrl: "{{ secret('CAMUNDA_AUTH_SERVER_URL') }}"
                    jobKey: "{{ trigger.jobKey }}"
                """
        ),
        @Example(
            title = "Stream jobs over gRPC from a local development cluster and only fetch the variables the flow needs.",
            full = true,
            code = """
                id: stream_camunda_jobs
                namespace: company.team

                triggers:
                  - id: on_camunda_job
                    type: io.kestra.plugin.camunda.Trigger
                    grpcAddress: http://localhost:26500
                    streamEnabled: true
                    jobType: charge-payment
                    fetchVariables:
                      - orderId
                      - amount

                tasks:
                  - id: log_job
                    type: io.kestra.plugin.core.log.Log
                    message: "Charging {{ trigger.variables.amount }} for order {{ trigger.variables.orderId }}"
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<Job>, CamundaConnectionInterface {

    @Schema(
        title = "Type of the jobs to activate",
        description = "Matches the task definition type of the BPMN service task."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> jobType;

    @Schema(
        title = "How long the job stays locked for this worker",
        description = "ISO-8601 duration. Must be longer than the flow takes to complete the job, otherwise Camunda hands the job to another worker and the flow runs twice. Defaults to the client default of 5 minutes."
    )
    @PluginProperty(group = "main")
    private Property<Duration> timeout;

    @Schema(
        title = "Variables to fetch for each job",
        description = "Fetches every process variable when not set, which can be expensive on processes carrying large payloads."
    )
    @PluginProperty(group = "main")
    private Property<List<String>> fetchVariables;

    @Schema(
        title = "Name reported to Camunda as the job worker name",
        description = "Shows up in Operate and in the `worker` trigger output. Defaults to the trigger ID."
    )
    @PluginProperty(group = "advanced")
    private Property<String> workerName;

    @Schema(
        title = "Maximum number of jobs activated at once",
        description = "Bounds how many executions the trigger can start in parallel before Camunda is asked for more jobs. Defaults to the client default of 32."
    )
    @PluginProperty(group = "advanced")
    private Property<Integer> maxJobsActive;

    @Schema(
        title = "Push jobs over a gRPC stream instead of long polling",
        description = "Requires `grpcAddress`, job streaming has no REST equivalent. Disabled by default so that a trigger configured with `restAddress` alone does not open a stream to an unreachable gateway."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> streamEnabled = Property.ofValue(false);

    @Schema(title = "REST API base URL of the Camunda cluster")
    @PluginProperty(group = "connection")
    private Property<String> restAddress;

    @Schema(title = "gRPC gateway address of the Camunda cluster")
    @PluginProperty(group = "connection")
    private Property<String> grpcAddress;

    @Schema(title = "Username for Basic authentication")
    @PluginProperty(group = "connection")
    private Property<String> username;

    @Schema(title = "Password for Basic authentication")
    @ToString.Exclude
    @PluginProperty(group = "connection", secret = true)
    private Property<String> password;

    @Schema(title = "OAuth2 client ID")
    @PluginProperty(group = "connection")
    private Property<String> clientId;

    @Schema(title = "OAuth2 client secret")
    @ToString.Exclude
    @PluginProperty(group = "connection", secret = true)
    private Property<String> clientSecret;

    @Schema(title = "OAuth2 token endpoint, required for a self-managed cluster")
    @PluginProperty(group = "connection")
    private Property<String> authorizationServerUrl;

    @Schema(title = "OAuth2 audience")
    @PluginProperty(group = "connection")
    private Property<String> audience;

    @Schema(title = "Camunda SaaS cluster ID")
    @PluginProperty(group = "connection")
    private Property<String> clusterId;

    @Schema(title = "Camunda SaaS region")
    @PluginProperty(group = "connection")
    private Property<String> region;

    @Schema(
        title = "Camunda tenant ID the worker activates jobs for",
        description = """
            Camunda's own multi-tenancy identifier, unrelated to the Kestra tenant the flow runs in.
            Defaults to `<default>`, so on a multi-tenant cluster a worker left unset will not see
            jobs belonging to any other tenant."""
    )
    @PluginProperty(group = "connection")
    private Property<String> tenantId;

    /** How long {@link #kill()} waits for the job worker and client to close before giving up. */
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    /** Set when the publisher's callback begins, so {@link #stop} knows termination will be signalled. */
    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicBoolean publisherStarted = new AtomicBoolean(false);

    /** Released by {@link #stop()} to let the worker thread close the stream. */
    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final CountDownLatch stopSignal = new CountDownLatch(1);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final CountDownLatch waitForTermination = new CountDownLatch(1);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicReference<JobWorker> worker = new AtomicReference<>();

    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) {
        RunContext runContext = conditionContext.getRunContext();

        return Flux.from(publisher(runContext))
            .map(job -> TriggerService.generateRealtimeExecution(this, conditionContext, context, job));
    }

    public Publisher<Job> publisher(RunContext runContext) {
        return Flux.create(sink -> {
            var logger = runContext.logger();
            // set before anything can fail, so kill() knows the finally block below will run
            this.publisherStarted.set(true);

            try {
                var rJobType = runContext.render(this.jobType).as(String.class).filter(v -> !v.isBlank())
                    .orElseThrow(() -> new IllegalArgumentException("`jobType` is required"));
                var rTimeout = runContext.render(this.timeout).as(Duration.class).orElse(null);
                var rFetchVariables = runContext.render(this.fetchVariables).asList(String.class);
                var rWorkerName = runContext.render(this.workerName).as(String.class).filter(v -> !v.isBlank()).orElse(this.id);
                var rMaxJobsActive = runContext.render(this.maxJobsActive).as(Integer.class).orElse(null);
                var rStreamEnabled = runContext.render(this.streamEnabled).as(Boolean.class).orElse(false);
                var rTenantId = runContext.render(this.tenantId).as(String.class).filter(v -> !v.isBlank()).orElse(null);
                var rGrpcAddress = runContext.render(this.grpcAddress).as(String.class).filter(v -> !v.isBlank()).orElse(null);

                if (rStreamEnabled && rGrpcAddress == null) {
                    // otherwise the client streams against the default gRPC address, 0.0.0.0:26500, and
                    // retries forever while long polling keeps the trigger looking healthy
                    throw new IllegalArgumentException("`streamEnabled` requires `grpcAddress`, job streaming has no REST equivalent");
                }

                try (CamundaClient client = this.camundaClient(runContext)) {
                    var builder = client.newWorker()
                        .jobType(rJobType)
                        .handler((jobClient, job) -> sink.next(Job.of(job)))
                        .name(rWorkerName)
                        .streamEnabled(rStreamEnabled);

                    if (rTimeout != null) {
                        builder = builder.timeout(rTimeout);
                    }
                    if (rMaxJobsActive != null) {
                        builder = builder.maxJobsActive(rMaxJobsActive);
                    }
                    if (!rFetchVariables.isEmpty()) {
                        builder = builder.fetchVariables(rFetchVariables);
                    }
                    if (rTenantId != null) {
                        // the client's defaultTenantId only applies to commands, a job worker activates
                        // jobs for defaultJobWorkerTenantIds, which stays ["<default>"] unless set here
                        builder = builder.tenantId(rTenantId);
                    }

                    try (JobWorker jobWorker = builder.open()) {
                        this.worker.set(jobWorker);
                        logger.debug(
                            "Camunda job worker opened triggerId={} jobType={} workerName={} streamEnabled={}",
                            this.id, rJobType, rWorkerName, rStreamEnabled
                        );

                        // the worker activates jobs on its own threads, so hold this one until stop() or kill()
                        this.stopSignal.await();
                    }
                }

                sink.complete();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                sink.complete();
            } catch (Exception e) {
                logger.error("Camunda trigger triggerId={} failed: {}", this.id, e.getMessage());
                sink.error(e);
            } finally {
                this.worker.set(null);
                this.waitForTermination.countDown();
            }
        }, FluxSink.OverflowStrategy.BUFFER);
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void kill() {
        stop(true);
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void stop() {
        stop(false); // must be non-blocking
    }

    private void stop(boolean wait) {
        if (!this.isActive.compareAndSet(true, false)) {
            return;
        }

        // no RunContext here, so use a stable logger
        LoggerFactory.getLogger(Trigger.class)
            .debug("Stopping Camunda trigger triggerId={} (wait={})", this.id, wait);

        this.stopSignal.countDown();

        // publisherStarted is read after the signal, so a publisher racing to start either sees the
        // released latch and closes immediately, or is already past the flag and will count down
        if (!wait || !this.publisherStarted.get()) {
            return;
        }

        try {
            // kill() runs on the worker's kill-dispatch thread, so this wait is bounded: an unresponsive
            // gateway during shutdown must not stall kill signals for every other job on this worker
            if (!this.waitForTermination.await(SHUTDOWN_TIMEOUT.toSeconds(), TimeUnit.SECONDS)) {
                LoggerFactory.getLogger(Trigger.class)
                    .warn("Camunda job worker triggerId={} did not close within {}, abandoning it", this.id, SHUTDOWN_TIMEOUT);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
