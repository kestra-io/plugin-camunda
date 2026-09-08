package io.kestra.plugin.camunda;

/**
 * Which API a task's commands are sent over.
 *
 * Camunda exposes the same commands on a REST API and a gRPC gateway. Which one a cluster actually
 * serves varies: SaaS derives both addresses but older clusters do not serve every REST v2 endpoint,
 * and a self-managed deployment may expose only one of the two.
 */
public enum Transport {
    /** The REST API, at `restAddress` or the address SaaS derives from the cluster ID and region. */
    REST,

    /** The gRPC gateway, at `grpcAddress` or the address SaaS derives from the cluster ID and region. */
    GRPC
}
