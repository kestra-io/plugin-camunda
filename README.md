<p align="center">
  <a href="https://www.kestra.io">
    <img src="https://kestra.io/banner.png"  alt="Kestra workflow orchestrator" />
  </a>
</p>

<h1 align="center" style="border-bottom: none">
    Event-Driven Declarative Orchestrator
</h1>

<div align="center">
 <a href="https://github.com/kestra-io/kestra/releases"><img src="https://img.shields.io/github/tag-pre/kestra-io/kestra.svg?color=blueviolet" alt="Last Version" /></a>
  <a href="https://github.com/kestra-io/kestra/blob/develop/LICENSE"><img src="https://img.shields.io/github/license/kestra-io/kestra?color=blueviolet" alt="License" /></a>
  <a href="https://github.com/kestra-io/kestra/stargazers"><img src="https://img.shields.io/github/stars/kestra-io/kestra?color=blueviolet&logo=github" alt="Github star" /></a> <br>
<a href="https://kestra.io"><img src="https://img.shields.io/badge/Website-kestra.io-192A4E?color=blueviolet" alt="Kestra infinitely scalable orchestration and scheduling platform"></a>
<a href="https://kestra.io/slack"><img src="https://img.shields.io/badge/Slack-Join%20Community-blueviolet?logo=slack" alt="Slack"></a>
</div>

<br />

<p align="center">
  <a href="https://twitter.com/kestra_io" style="margin: 0 10px;">
        <img src="https://kestra.io/twitter.svg" alt="twitter" width="35" height="25" /></a>
  <a href="https://www.linkedin.com/company/kestra/" style="margin: 0 10px;">
        <img src="https://kestra.io/linkedin.svg" alt="linkedin" width="35" height="25" /></a>
  <a href="https://www.youtube.com/@kestra-io" style="margin: 0 10px;">
        <img src="https://kestra.io/youtube.svg" alt="youtube" width="35" height="25" /></a>
</p>

<br />
<p align="center">
    <a href="https://go.kestra.io/video/product-overview" target="_blank">
        <img src="https://kestra.io/startvideo.png" alt="Get started in 3 minutes with Kestra" width="640px" />
    </a>
</p>
<p align="center" style="color:grey;"><i>Get started with Kestra in 3 minutes.</i></p>

# Kestra Camunda Plugin

Interact with a Camunda 8 cluster from Kestra flows, using the official `io.camunda:camunda-client-java` client.

## Why

Teams with an existing Camunda 8 footprint had no way to drive Camunda process instances from a Kestra
flow, or to implement a Camunda service task as a Kestra flow, without hand-rolled gRPC or REST client
code. This plugin covers both directions, so Kestra can run next to Camunda instead of replacing it.

## What

Tasks and triggers under `io.kestra.plugin.camunda`:

- `Deploy`: deploy BPMN, DMN and form resources in one atomic command.
- `CreateProcessInstance`: start a process instance, optionally awaiting its result.
- `CancelProcessInstance`: terminate a running process instance.
- `PublishMessage`: publish a correlation message.
- `CompleteJob`: report an activated job as done, with output variables.
- `FailJob`: report an activated job as failed, raising a Camunda incident.
- `Trigger`: hold a job worker open and start one execution per activated job.

Authentication covers no credentials (development clusters), Basic auth, OAuth2 against a self-managed
identity provider, and Camunda SaaS client credentials.

## Local development

`docker-compose.yml` starts a single-node Camunda 8 cluster without secondary storage on
`http://localhost:8080` (REST) and `localhost:26500` (gRPC), which is enough for every task here.
Kestra's own dev server in that file is on `http://localhost:8090` so that Camunda keeps port 8080.

Tests use Testcontainers with the same image, so `./gradlew test` needs Docker.

## Documentation
* Full documentation can be found under: [kestra.io/docs](https://kestra.io/docs)
* Documentation for developing a plugin is included in the [Plugin Developer Guide](https://kestra.io/docs/plugin-developer-guide/)


## License
Apache 2.0 © [Kestra Technologies](https://kestra.io)


## Stay up to date

We release new versions every month. Give the [main repository](https://github.com/kestra-io/kestra) a star to stay up to date with the latest releases and get notified about future updates.

![Star the repo](https://kestra.io/star.gif)
