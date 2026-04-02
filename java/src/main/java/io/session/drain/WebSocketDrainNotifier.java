package io.session.drain;

import io.session.model.Session;
import io.session.model.SessionRegistry;
import io.session.transport.WebSocketSessionRegistry;

import java.util.logging.Logger;

/** Drains a WebSocket session by sending a 1001 Going Away close frame. */
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
        if (session.getType() != Session.Type.WEBSOCKET) return;

        if (!wsRegistry.has(session.getId())) {
            log.warning("[ws-drain] no socket for session=" + session.getId());
            sessions.unregister(session.getId());
            return;
        }

        log.info("[ws-drain] 1001 Going Away session=" + session.getId()
                + " endpoint=" + session.getEndpoint() + " budget=" + remainingMs + "ms");

        wsRegistry.drain(session.getId(), remainingMs);
        sessions.unregister(session.getId());
        log.info("[ws-drain] drained session=" + session.getId());
    }
}
