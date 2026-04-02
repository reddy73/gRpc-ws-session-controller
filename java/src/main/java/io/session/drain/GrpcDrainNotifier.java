package io.session.drain;

import io.session.model.Session;
import io.session.model.SessionRegistry;
import io.session.transport.GrpcChannelRegistry;

import java.util.logging.Logger;

/** Drains a gRPC session by sending HTTP/2 GOAWAY via channel.shutdown(). */
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
        if (session.getType() != Session.Type.GRPC) return;

        if (!channels.has(session.getId())) {
            log.warning("[grpc-drain] no channel for session=" + session.getId());
            sessions.unregister(session.getId());
            return;
        }

        log.info("[grpc-drain] GOAWAY session=" + session.getId()
                + " endpoint=" + session.getEndpoint() + " budget=" + remainingMs + "ms");

        channels.drain(session.getId(), remainingMs);
        sessions.unregister(session.getId());
        log.info("[grpc-drain] drained session=" + session.getId());
    }
}
