Talk to a Camunda 8 cluster from Kestra flows: deploy BPMN, DMN and form resources, start and cancel
process instances, publish correlation messages, and implement a BPMN service task as a Kestra flow.

Built on the official `io.camunda:camunda-client-java` client, so it works against a self-managed
orchestration cluster and against Camunda SaaS.

## Connecting

Every task and the trigger take the same connection properties.

- `restAddress`: REST API base URL, for example `http://localhost:8080`. Commands use it unless only
  `grpcAddress` is set. One of `restAddress`, `grpcAddress` or `clusterId` is required.
- `grpcAddress`: gRPC gateway address, for example `http://localhost:26500`. Setting it without
  `restAddress` sends every command over gRPC. Required for the trigger's `streamEnabled` mode, which
  has no REST equivalent.
- `transport`: `REST` or `GRPC`. Defaults to gRPC on SaaS, and on a self-managed cluster to the API
  implied by whichever address is set, falling back to REST when both or neither are.
- `tenantId`: Camunda's own tenant, unrelated to the Kestra tenant the flow runs in. Always applied to
  commands, defaulting to `<default>`. On the trigger it additionally selects which tenants' jobs the
  worker activates, which is a separate SDK setting.

Four authentication modes, picked from what is set:

| Mode | Properties |
| --- | --- |
| None | nothing, works only on a cluster with API protection disabled |
| Basic | `username`, `password` |
| OAuth2 self-managed | `clientId`, `clientSecret`, `authorizationServerUrl`, `audience` |
| Camunda SaaS | `clusterId`, `clientId`, `clientSecret`, optional `region` |

`audience` is required for the self-managed OAuth2 mode, commonly `zeebe-api`. The client validates it
and there is no fallback, because this plugin disables environment overrides (below). Camunda SaaS
derives its own audience, so leave it unset there.

Camunda's client normally reads `CAMUNDA_*` and `ZEEBE_*` environment variables. This plugin turns
that off, so a flow always connects with what it declares and never with what the worker happens to
have in its environment.

Camunda SaaS uses gRPC by default here, which is a deliberate divergence from the client's own REST
preference. A default free-tier cluster answers `Failed with code 404: 'Not Found'` on the REST base
the client derives, `https://<region>.zeebe.camunda.io:443/<clusterId>`, while gRPC succeeds with the
same credentials in the same execution. gRPC is served by every supported cluster, so defaulting to
it works everywhere REST does and also where REST does not.

`transport: REST` opts back in on a cluster that serves it. Known limitation: on a cluster whose REST
API lives at `/v2` on the gRPC host rather than the derived base, REST is not usable yet. Overriding
`restAddress` alongside `clusterId` points the client at the right host, but the deployment request
then fails on its `multipart/form-data` body. Use gRPC on such a cluster.

## Deploying resources

`Deploy` sends one command holding every resource, so either all of them are deployed or none is.
Resource names carry the type: `.bpmn` or `.xml` for a process, `.dmn` for a decision, `.form` for a
form. A value is either the content itself or a `kestra://` internal storage URI produced by an
earlier task.

```yaml
id: deploy_camunda_resources
namespace: company.team

tasks:
  - id: deploy
    type: io.kestra.plugin.camunda.Deploy
    restAddress: http://localhost:8080
    resources:
      order-fulfillment.bpmn: "{{ read('order-fulfillment.bpmn') }}"
      discount.dmn: "{{ read('discount.dmn') }}"
```

## Driving process instances

`CreateProcessInstance` starts a process by `processId` (the BPMN process ID) or by
`processDefinitionKey`. Set `awaitCompletion` to block until the instance ends and read its variables
back, and raise `requestTimeout` above the expected process duration when you do, the client default
is 10 seconds.

```yaml
id: run_camunda_process
namespace: company.team

tasks:
  - id: run_process
    type: io.kestra.plugin.camunda.CreateProcessInstance
    restAddress: http://localhost:8080
    processId: order-fulfillment
    variables:
      orderId: ORD-123
    awaitCompletion: true
    requestTimeout: PT2M
```

`CancelProcessInstance` terminates a running instance by key.

## Implementing a service task as a flow

`Trigger` holds a Camunda job worker open and starts one execution per activated job. The trigger does
not decide the outcome: the flow reports it with `CompleteJob` on success and `FailJob` on error, both
keyed on `{{ trigger.jobKey }}`.

Always report an outcome. Camunda re-offers a job whose lock expired without decrementing its retries,
so a flow that reports neither is activated again every `timeout`, fails again, and repeats for as long
as the process instance lives. Retries never reach zero, so no incident is raised and nothing surfaces
in Operate. `FailJob` with the default `retries: 0` raises the incident immediately.

Delivery is at-least-once. A lock expiry, a worker restart mid-flow, or a flow slower than `timeout`
each produce a second execution for the same job. Keep `timeout` above the expected flow duration, and
make the work idempotent or guard it on the job key if a repeat would be harmful.

`maxJobsActive` bounds how many jobs the worker activates, not how many executions run at once: the
trigger releases each job as soon as its execution is created. Use the flow's `concurrency` block to
limit parallelism, and remember that time queued behind that limit counts against the job lock.

```yaml
id: handle_camunda_job
namespace: company.team

triggers:
  - id: on_camunda_job
    type: io.kestra.plugin.camunda.Trigger
    restAddress: http://localhost:8080
    jobType: send-notification
    timeout: PT5M

tasks:
  - id: send_notification
    type: io.kestra.plugin.core.log.Log
    message: "Notifying about order {{ trigger.variables.orderId }}"

  - id: complete_job
    type: io.kestra.plugin.camunda.CompleteJob
    restAddress: http://localhost:8080
    jobKey: "{{ trigger.jobKey }}"
    variables:
      notified: true

errors:
  - id: fail_job
    type: io.kestra.plugin.camunda.FailJob
    restAddress: http://localhost:8080
    jobKey: "{{ trigger.jobKey }}"
    errorMessage: "Kestra execution {{ execution.id }} failed"
```

Jobs are activated over the same transport as the tasks, which is the REST API when `restAddress` is
set. Set `grpcAddress` and `streamEnabled: true` for push-based streaming instead of long polling.

## Messages

`PublishMessage` publishes a correlation message. Leave `correlationKey` unset for a message that
starts a process instance through a message start event. `timeToLive` controls how long Camunda
buffers the message when nothing is waiting for it yet, and `messageId` deduplicates it over that
window.

## Local development

`docker-compose.yml` in this repository starts a single-node Camunda 8 cluster without secondary
storage, which is enough for every task here. Operate and Tasklist are not part of it.

Camunda is published on its own default ports, `http://localhost:8080` for REST and
`localhost:26500` for gRPC, so the examples above work as written. Kestra's own dev server in that
same file is on `http://localhost:8090` to keep 8080 free.
