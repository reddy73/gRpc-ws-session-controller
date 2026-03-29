package io.session;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

import java.util.logging.Logger;

/**
 * Entry point — wires up all components and starts the session survivability controller.
 */
public class SessionController {

    private static final Logger log = Logger.getLogger(SessionController.class.getName());

    public static void main(String[] args) {
        String namespace = System.getenv().getOrDefault("NAMESPACE", "default");

        // Wire up components
        SessionRegistry registry = new SessionRegistry();
        RetryClassifier classifier = new RetryClassifier(RetryClassifier.defaultRules());

        DrainHandler drainHandler = new DrainHandler(registry, (session, remainingMs) ->
            log.info(String.format("[notify] session=%s type=%s remaining=%dms",
                    session.getId(), session.getType(), remainingMs))
        );

        EndpointWatcher.SessionRebinder rebinder = (session, oldEp, newEp) ->
            log.info(String.format("[rebind] session=%s %s -> %s", session.getId(), oldEp, newEp));

        // Demonstrate classifier
        for (String method : new String[]{
                "/myapp.v1.MyService/Watch",
                "/myapp.v1.MyService/StreamEvents",
                "/myapp.v1.MyService/Create"}) {
            log.info("method=" + method + " retryClass=" + classifier.classify(method));
        }

        // Register a demo session
        Session demo = new Session();
        demo.setId("demo-session-1");
        demo.setType(Session.Type.GRPC);
        demo.setPodName("myapp-pod-abc");
        demo.setNamespace(namespace);
        demo.setEndpoint("10.0.0.1:50051");
        registry.register(demo);

        log.info("session controller running namespace=" + namespace + " sessions=" + registry.size());

        // Connect to Kubernetes and start endpoint watcher
        try (KubernetesClient client = new KubernetesClientBuilder().build()) {
            EndpointWatcher watcher = new EndpointWatcher(registry, rebinder);
            watcher.watch(client, namespace);

            // Simulate drain after 5s (demo)
            Thread.sleep(5000);
            drainHandler.handle("myapp-pod-abc", namespace, 30_000);

            // Keep running
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("shutting down");
        }
    }
}
