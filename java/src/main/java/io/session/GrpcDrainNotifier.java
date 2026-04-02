package io.session;

import java.util.logging.Logger;

/**
 * Real DrainNotifier backed by GrpcChannelRegistry.
 *
 * When called, it:
 *   1. Sends HTTP/2 GOAWAY by calling channel.shutdown()
 *   2. Waits up to remainingMs for in-flight RPCs to finish
 *   3. Force-closes if the budget is exhausted
 *
 * After drain, unregisters the session from the registry so the
 * DrainHandler poll loop sees it as gone and exits cleanly.
 */
public class GrpcDrainNotifier implements DrainHandler.DrainNotifier {

    private static final Logger log = Logger.getLogger(GrpcDrainNotifier.class.getName());

    private final GrpcChannelRegistry channels;
    private final SessionRegistry sessions;

    public GrpcDrainNotifier(GrpcChannelRegistry channels, SessionRegistry sessions) {
        this.channels = channels;
        this.sessions = sessions;
    }

    @Override
    public void notify(Session session, long remainingMs) throws Exception {
        if (session.getType() != Session.Type.GRPC) {
            log.info("[drain-notifier] skipping non-gRPC session=" + session.getId());
            return;
        }

        if (!channels.has(session.getId())) {
            log.warning("[drain-notifier] no channel found for session=" + session.getId() + ", unregistering");
            sessions.unregister(session.getId());
            return;
        }

        log.info("[drain-notifier] sending GOAWAY session=" + session.getId()
                + " endpoint=" + session.getEndpoint()
                + " budget=" + remainingMs + "ms");

        channels.drain(session.getId(), remainingMs);

        // Channel is closed — unregister so the poll loop sees it as drained
        sessions.unregister(session.getId());
        log.info("[drain-notifier] session drained and unregistered=" + session.getId());
    }
}
