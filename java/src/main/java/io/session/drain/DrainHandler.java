package io.session.drain;

import io.session.model.Session;
import io.session.model.SessionRegistry;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Orchestrates graceful drain of sessions before pod termination.
 * Sends drain signals (gRPC GOAWAY / WS 1001 close frame) via DrainNotifier,
 * then polls until all sessions close or the deadline expires.
 */
public class DrainHandler {

    private static final Logger log = Logger.getLogger(DrainHandler.class.getName());

    private final SessionRegistry registry;
    private final DrainNotifier notifier;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public DrainHandler(SessionRegistry registry, DrainNotifier notifier) {
        this.registry = registry;
        this.notifier = notifier;
    }

    public void handle(String podName, String namespace, long deadlineMs) {
        List<Session> sessions = registry.byPod(podName, namespace);
        if (sessions.isEmpty()) return;

        log.info(String.format("[drain] pod=%s/%s sessions=%d deadline=%dms",
                namespace, podName, sessions.size(), deadlineMs));

        Instant deadline = Instant.now().plusMillis(deadlineMs);

        for (Session s : sessions) {
            long remaining = deadline.toEpochMilli() - Instant.now().toEpochMilli();
            if (remaining <= 0) {
                registry.unregister(s.getId());
                continue;
            }
            try {
                notifier.notify(s, remaining);
            } catch (Exception e) {
                log.warning("[drain] notify error session=" + s.getId() + " " + e.getMessage());
            }
        }

        scheduler.scheduleAtFixedRate(() -> {
            List<Session> remaining = registry.byPod(podName, namespace);
            if (remaining.isEmpty()) {
                log.info("[drain] all sessions drained pod=" + podName);
                scheduler.shutdown();
                return;
            }
            if (Instant.now().isAfter(deadline)) {
                log.warning("[drain] deadline hit, force-closing " + remaining.size() + " sessions");
                remaining.forEach(s -> registry.unregister(s.getId()));
                scheduler.shutdown();
            }
        }, 500, 500, TimeUnit.MILLISECONDS);
    }

    /** Implemented by GrpcDrainNotifier and WebSocketDrainNotifier. */
    public interface DrainNotifier {
        void notify(Session session, long remainingMs) throws Exception;
    }
}
