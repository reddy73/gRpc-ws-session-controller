package io.session;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.grpc.ManagedChannel;
import io.session.drain.DrainHandler;
import io.session.drain.GrpcDrainNotifier;
import io.session.drain.WebSocketDrainNotifier;
import io.session.endpoint.EndpointWatcher;
import io.session.model.Session;
import io.session.model.SessionRegistry;
import io.session.retry.RetryClassifier;
import io.session.transport.GrpcChannelRegistry;
import io.session.transport.StreamingEchoClient;
import io.session.transport.WebSocketEchoClient;
import io.session.transport.WebSocketSessionRegistry;

import java.net.http.WebSocket;
import java.util.logging.Logger;

/** Entry point — wires all packages into a running session survivability controller. */
public class SessionController {

    private static final Logger log = Logger.getLogger(SessionController.class.getName());

    private static final String ECHO_TARGET = System.getenv().getOrDefault("ECHO_TARGET", "localhost:50051");

    public static void main(String[] args) throws InterruptedException {
        String namespace = System.getenv().getOrDefault("NAMESPACE", "default");
        String podName   = System.getenv().getOrDefault("TARGET_POD", "myapp-pod-abc");

        // ── model ─────────────────────────────────────────────────────────
        SessionRegistry registry = new SessionRegistry();

        // ── transport ─────────────────────────────────────────────────────
        GrpcChannelRegistry      grpcChannels = new GrpcChannelRegistry();
        WebSocketSessionRegistry wsRegistry   = new WebSocketSessionRegistry();

        // ── drain ─────────────────────────────────────────────────────────
        GrpcDrainNotifier      grpcNotifier = new GrpcDrainNotifier(grpcChannels, registry);
        WebSocketDrainNotifier wsNotifier   = new WebSocketDrainNotifier(wsRegistry, registry);

        // Unified notifier — dispatches by session type
        DrainHandler.DrainNotifier notifier = (session, remainingMs) -> {
            if (session.getType() == Session.Type.GRPC)      grpcNotifier.notify(session, remainingMs);
            else if (session.getType() == Session.Type.WEBSOCKET) wsNotifier.notify(session, remainingMs);
        };
        DrainHandler drainHandler = new DrainHandler(registry, notifier);

        // ── retry ─────────────────────────────────────────────────────────
        RetryClassifier classifier = new RetryClassifier(RetryClassifier.defaultRules());
        for (String m : new String[]{
                "/myapp.v1.MyService/Watch",
                "/myapp.v1.MyService/StreamEvents",
                "/myapp.v1.MyService/Create"}) {
            log.info("method=" + m + " retryClass=" + classifier.classify(m));
        }

        // ── endpoint ──────────────────────────────────────────────────────
        EndpointWatcher.SessionRebinder rebinder = (session, oldEp, newEp) ->
            log.info("[rebind] session=" + session.getId() + " " + oldEp + " -> " + newEp);

        // ── open gRPC session ─────────────────────────────────────────────
        String grpcSessionId = "grpc-session-1";
        ManagedChannel channel = grpcChannels.connect(grpcSessionId, ECHO_TARGET);

        Session grpcSession = new Session();
        grpcSession.setId(grpcSessionId);
        grpcSession.setType(Session.Type.GRPC);
        grpcSession.setPodName(podName);
        grpcSession.setNamespace(namespace);
        grpcSession.setEndpoint(ECHO_TARGET);
        grpcSession.setRetryClass(Session.RetryClass.UNSAFE);
        registry.register(grpcSession);

        StreamingEchoClient grpcClient = new StreamingEchoClient(channel, grpcSessionId);
        grpcClient.startStreaming(1000);

        // ── open WebSocket session (optional) ─────────────────────────────
        String wsTarget = System.getenv().getOrDefault("WS_TARGET", "");
        WebSocketEchoClient wsClient = null;
        if (!wsTarget.isEmpty()) {
            try {
                String wsSessionId = "ws-session-1";
                WebSocket ws = wsRegistry.connect(wsSessionId, wsTarget);

                Session wsSession = new Session();
                wsSession.setId(wsSessionId);
                wsSession.setType(Session.Type.WEBSOCKET);
                wsSession.setPodName(podName);
                wsSession.setNamespace(namespace);
                wsSession.setEndpoint(wsTarget);
                wsSession.setRetryClass(Session.RetryClass.UNSAFE);
                registry.register(wsSession);

                wsClient = new WebSocketEchoClient(ws, wsSessionId);
                wsClient.startStreaming(1000);
                log.info("[controller] WS session open target=" + wsTarget);
            } catch (Exception e) {
                log.warning("[controller] WS connect failed: " + e.getMessage());
            }
        }

        log.info("[controller] running namespace=" + namespace + " sessions=" + registry.size());

        // ── Kubernetes endpoint watcher ───────────────────────────────────
        try (KubernetesClient k8s = new KubernetesClientBuilder().build()) {
            new EndpointWatcher(registry, rebinder).watch(k8s, namespace);

            log.info("[controller] drain fires in 5s...");
            Thread.sleep(5_000);

            drainHandler.handle(podName, namespace, 30_000);

            boolean grpcClosed = grpcClient.awaitClose(35_000);
            log.info("[controller] gRPC drain complete=" + grpcClosed
                    + " sent=" + grpcClient.getSent());

            if (wsClient != null) {
                boolean wsClosed = wsClient.awaitClose(35_000);
                log.info("[controller] WS drain complete=" + wsClosed
                        + " sent=" + wsClient.getSent());
            }

            Thread.currentThread().join();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            grpcClient.stop();
            if (wsClient != null) wsClient.stop();
            log.info("[controller] shutting down");
        }
    }
}
