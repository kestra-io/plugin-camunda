package io.kestra.plugin.camunda;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * A single-node Camunda 8 orchestration cluster, started once for the whole test run.
 *
 * Secondary storage is disabled, so the search and history APIs are unavailable, but every
 * command this plugin sends (deploy, process instance, message, job) runs on the broker alone.
 */
final class CamundaTestCluster {

    private static final DockerImageName IMAGE = DockerImageName.parse("camunda/camunda:8.9.19");
    private static final int REST_PORT = 8080;
    private static final int GRPC_PORT = 26500;
    private static final int MANAGEMENT_PORT = 9600;

    private static GenericContainer<?> container;

    private CamundaTestCluster() {
    }

    static synchronized void start() {
        if (container != null) {
            return;
        }

        container = new GenericContainer<>(IMAGE)
            .withEnv("SPRING_PROFILES_ACTIVE", "broker")
            .withEnv("CAMUNDA_SECURITY_AUTHENTICATION_UNPROTECTEDAPI", "true")
            // unprotecting the API covers REST only, the gRPC gateway still runs authorization checks
            .withEnv("CAMUNDA_SECURITY_AUTHORIZATIONS_ENABLED", "false")
            .withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_TYPE", "none")
            .withExposedPorts(REST_PORT, GRPC_PORT, MANAGEMENT_PORT)
            .waitingFor(Wait.forHttp("/actuator/health/readiness")
                .forPort(MANAGEMENT_PORT)
                .forStatusCode(200)
                .withStartupTimeout(Duration.ofMinutes(3))
            );

        container.start();
    }

    static String restAddress() {
        return "http://" + container.getHost() + ":" + container.getMappedPort(REST_PORT);
    }

    static String grpcAddress() {
        return "http://" + container.getHost() + ":" + container.getMappedPort(GRPC_PORT);
    }
}
