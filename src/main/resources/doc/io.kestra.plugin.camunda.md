Talk to a Camunda 8 cluster from Kestra flows: deploy BPMN, DMN and form resources, start and cancel
process instances, publish correlation messages, and implement a BPMN service task as a Kestra flow.

Built on the official `io.camunda:camunda-client-java` client, so it works against a self-managed
orchestration cluster and against Camunda SaaS.

## Connecting

Every task and the trigger take the same connection properties.

- `restAddress`: REST API base URL, for example `http://localhost:8080`. Most commands use it.
- `grpcAddress`: gRPC gateway address, for example `http://localhost:26500`. Needed only for the
  trigger's `streamEnabled` mode, job streaming has no REST equivalent.
- `tenantId`: applied to every command, on a cluster with multi-tenancy enabled.

Four authentication modes, picked from what is set:

| Mode | Properties |
| --- | --- |
| None | nothing, works only on a cluster with API protection disabled |
| Basic | `username`, `password` |
| OAuth2 self-managed | `clientId`, `clientSecret`, `authorizationServerUrl`, optional `audience` |
| Camunda SaaS | `clusterId`, `clientId`, `clientSecret`, optional `region` |

Camunda's client normally reads `CAMUNDA_*` and `ZEEBE_*` environment variables. This plugin turns
that off, so a flow always connects with what it declares and never with what the worker happens to
have in its environment.

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

`Trigger` holds a Camunda job worker open and starts one execution per activated job. The trigger
does not complete the job: the flow decides the outcome and reports it with `CompleteJob`, using
`{{ trigger.jobKey }}`.

A job the flow never completes stays locked until the trigger's `timeout` elapses, after which
Camunda hands it to a worker again and the flow runs a second time. Keep `timeout` above the
expected flow duration.

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
