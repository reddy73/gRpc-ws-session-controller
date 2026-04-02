package io.session;

import java.util.logging.Logger;

/**
 * Real DrainNotifier for WebSocket sessions.
 * Mirrors GrpcDrainNotifier — sends 1001 Going Away close frame
 * instead of HTTP/2 GOAWAY, then unregisters the session.
 */
public class WebSocketDrainNotifier implements DrainHandler.DrainNotifier {

    private static final Logger log = Logger.getLogger(WebSocketDrainNotifier.class.getName());

    private final WebSocketSessionRegistry wsRegistry;
    private final SessionRegistry sessions;

    public WebSocketDrainNotifier(WebSocketSessionRegistry wsRegistry, SessionRegistry sessions) {
        this.wsRegistry = wsRegistry;
        this.sessions = sessions;
    }

    @Override
    public void notify(Session session, long remainingMs) throws Exception {
        if (session.getType() != Session.Type.WEBSOCKET) {
            log.info("[ws-drain] skipping non-WS session=" + session.getId());
            return;
        }

        if (!wsRegistry.has(session.getId())) {
            log.warning("[ws-drain] no socket found for session=" + session.getId() + ", unregistering");
            sessions.unregister(session.getId());
            return;
        }

        log.info("[ws-drain] sending 1001 Going Away session=" + session.getId()
                + " endpoint=" + session.getEndpoint()
                + " budget=" + remainingMs + "ms");

        wsRegistry.drain(session.getId(), remainingMs);

        sessions.unregister(session.getId());
        log.info("[ws-drain] session drained and unregistered=" + session.getId());
    }
}
