package io.session;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.grpc.ManagedChannel;

import java.util.logging.Logger;

/**
 * Entry point — wires real gRPC drain (GOAWAY) into the session controller.
 *
 * Flow:
 *   1. GrpcChannelRegistry opens a real ManagedChannel to the echoserver
 *   2. StreamingEchoClient holds an open bidi stream on that channel
 *   3. GrpcDrainNotifier calls channel.shutdown() → sends HTTP/2 GOAWAY
 *   4. DrainHandler polls until the session is unregistered (stream closed)
 */
public class SessionController {

    private static final Logger log = Logger.getLogger(SessionController.class.getName());

    // Target echoserver — override via env var for in-cluster use
    private static final String ECHO_TARGET =
            System.getenv().getOrDefault("ECHO_TARGET", "localhost:50051");

    public static void main(String[] args) throws InterruptedException {
        String namespace = System.getenv().getOrDefault("NAMESPACE", "default");
        String podName   = System.getenv().getOrDefault("TARGET_POD", "myapp-pod-abc");

        // ── Core components ──────────────────────────────────────────────
        SessionRegistry      registry  = new SessionRegistry();
        GrpcChannelRegistry  channels  = new GrpcChannelRegistry();
        RetryClassifier      classifier = new RetryClassifier(RetryClassifier.defaultRules());

        // Real drain notifier — sends GOAWAY and waits for stream to finish
        GrpcDrainNotifier drainNotifier = new GrpcDrainNotifier(channels, registry);
        DrainHandler      drainHandler  = new DrainHandler(registry, drainNotifier);

        EndpointWatcher.SessionRebinder rebinder = (session, oldEp, newEp) ->
            log.info(String.format("[rebind] session=%s %s -> %s",
                    session.getId(), oldEp, newEp));

        // ── Classifier demo ───────────────────────────────────────────────
        for (String method : new String[]{
                "/myapp.v1.MyService/Watch",
                "/myapp.v1.MyService/StreamEvents",
                "/myapp.v1.MyService/Create"}) {
            log.info("method=" + method + " retryClass=" + classifier.classify(method));
        }

        // ── Open a real gRPC channel + streaming session ──────────────────
        String sessionId = "stream-session-1";
        ManagedChannel channel = channels.connect(sessionId, ECHO_TARGET);

        Session session = new Session();
        session.setId(sessionId);
        session.setType(Session.Type.GRPC);
        session.setPodName(podName);
        session.setNamespace(namespace);
        session.setEndpoint(ECHO_TARGET);
        session.setRetryClass(Session.RetryClass.UNSAFE); // bidi stream
        registry.register(session);

        // Start streaming — holds the channel open so drain is observable
        StreamingEchoClient streamClient = new StreamingEchoClient(channel, sessionId);
        streamClient.startStreaming(1000); // send a message every 1s

        log.info("[controller] streaming session open target=" + ECHO_TARGET
                + " sessions=" + registry.size());

        // ── Kubernetes endpoint watcher ───────────────────────────────────
        try (KubernetesClient k8s = new KubernetesClientBuilder().build()) {
            EndpointWatcher watcher = new EndpointWatcher(registry, rebinder);
            watcher.watch(k8s, namespace);

            // ── Trigger drain after 5s ────────────────────────────────────
            log.info("[controller] drain will fire in 5s...");
            Thread.sleep(5_000);

            log.info("[controller] firing drain for pod=" + podName);
            drainHandler.handle(podName, namespace, 30_000);

            // Wait for the stream to observe GOAWAY and close
            boolean closed = streamClient.awaitClose(35_000);
            log.info("[controller] stream closed=" + closed
                    + " sent=" + streamClient.getSent()
                    + " received=" + streamClient.getReceived());

            Thread.currentThread().join();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            streamClient.stop();
            log.info("[controller] shutting down");
        }
    }
}
