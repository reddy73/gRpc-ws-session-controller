package io.session;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Orchestrates graceful drain of sessions before pod termination.
 * Sends drain signals (gRPC GOAWAY / WS close frame) and waits for
 * sessions to close within the pod's termination deadline.
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

    /**
     * Handle a drain event for a specific pod.
     *
     * @param podName    pod being terminated
     * @param namespace  pod namespace
     * @param deadlineMs termination deadline in milliseconds
     */
    public void handle(String podName, String namespace, long deadlineMs) {
        List<Session> sessions = registry.byPod(podName, namespace);
        if (sessions.isEmpty()) return;

        log.info(String.format("[drain] pod=%s/%s sessions=%d deadline=%dms",
                namespace, podName, sessions.size(), deadlineMs));

        Instant deadline = Instant.now().plusMillis(deadlineMs);

        for (Session s : sessions) {
            long remaining = deadline.toEpochMilli() - Instant.now().toEpochMilli();
            if (remaining <= 0) {
                log.warning("[drain] deadline exceeded, forcing close session=" + s.getId());
                registry.unregister(s.getId());
                continue;
            }
            try {
                notifier.notify(s, remaining);
            } catch (Exception e) {
                log.warning("[drain] notify error session=" + s.getId() + " err=" + e.getMessage());
            }
        }

        // Poll until all sessions drain or deadline expires
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

    /** Callback interface for sending drain signals to sessions */
    public interface DrainNotifier {
        void notify(Session session, long remainingMs) throws Exception;
    }
}
