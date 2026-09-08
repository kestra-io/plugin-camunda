# Kestra Camunda Plugin

## What

- Provides plugin components under `io.kestra.plugin.camunda`, talking to a Camunda 8 cluster through the official `io.camunda:camunda-client-java` client.
- Tasks: `Deploy`, `CreateProcessInstance`, `CancelProcessInstance`, `PublishMessage`, `CompleteJob`, `FailJob`.
- Trigger: `Trigger`, a realtime job-worker trigger emitting one execution per activated Camunda job.

## Why

- Teams with an existing Camunda 8 footprint had no way to drive Camunda process instances from a Kestra flow, or to trigger Kestra flows from Camunda jobs, without hand-rolled gRPC or REST client code.
- Lets Kestra sit next to an existing Camunda deployment instead of replacing it, which is the coexistence half of the story (the migration half is a separate skill).
- Operationally, a BPMN service task can be implemented as a Kestra flow, and a Kestra flow can start, await and cancel Camunda process instances.

## How

### Architecture

Single-module plugin, flat package `io.kestra.plugin.camunda`. No sub-plugins.

- `CamundaConnectionInterface` owns the whole connection concern: the property getters plus `camundaClient()`, which renders them, validates the authentication mode and builds the client. Environment variable overrides are disabled so a flow only connects with what it declares.
- The interface exists because tasks extend `Task` and the trigger extends `AbstractTrigger`, so there is no shared base class. `AbstractCamundaTask` declares the fields for tasks, `Trigger` declares them itself. That duplication is the price of the class hierarchy, do not add a third layer to hide it.
- `Job` is the trigger output, built from `ActivatedJob`.
- The trigger opens a job worker inside `Flux.create` and blocks the subscribing thread on a latch. `stop()` counts the latch down (non-blocking), `kill()` also waits for the worker to close.

Infrastructure dependencies (Docker Compose services):

- `app`: local Kestra server with the built plugin mounted, on port 8090.
- `camunda`: single-node Camunda 8 cluster, no secondary storage, REST on 8080 and gRPC on 26500. Authorizations are off, the gRPC gateway checks them separately from the REST unprotect flag.

### Dependency pinning

The Kestra platform BOM pins older `httpclient5` and `protobuf-java` than `camunda-client-java` 8.9 was
compiled against, which fails at runtime with `NoSuchMethodError` and `NoClassDefFoundError`. `build.gradle`
forces the versions the client needs. Revisit those forces whenever `camundaVersion` or `kestraVersion` moves.

### Project Structure

```
plugin-camunda/
├── src/main/java/io/kestra/plugin/camunda/
├── src/test/java/io/kestra/plugin/camunda/
├── src/test/resources/*.bpmn, *.dmn
├── build.gradle
└── README.md
```

## Local rules

- Base the wording on the implemented packages and classes, not on template README text.
- Tests boot a real Camunda cluster with Testcontainers, so `./gradlew test` needs Docker. Keep the cluster a single shared instance (`CamundaTestCluster`), booting one per test class is slow.

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
- https://docs.camunda.io/docs/apis-tools/java-client/getting-started/
- https://docs.camunda.io/docs/apis-tools/camunda-api-rest/camunda-api-rest-overview/
