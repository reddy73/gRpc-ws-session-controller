package io.session.transport;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/** Manages gRPC ManagedChannels keyed by session ID. */
public class GrpcChannelRegistry {

    private static final Logger log = Logger.getLogger(GrpcChannelRegistry.class.getName());

    private final ConcurrentHashMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    public ManagedChannel connect(String sessionId, String target) {
        ManagedChannel channel = ManagedChannelBuilder
                .forTarget(target)
                .usePlaintext()
                .build();
        channels.put(sessionId, channel);
        log.info("[grpc-channel] opened sessionId=" + sessionId + " target=" + target);
        return channel;
    }

    /** Shutdown sends HTTP/2 GOAWAY; awaitTermination waits for in-flight RPCs. */
    public void drain(String sessionId, long remainingMs) throws InterruptedException {
        ManagedChannel ch = channels.remove(sessionId);
        if (ch == null) return;

        log.info("[grpc-channel] draining sessionId=" + sessionId + " budget=" + remainingMs + "ms");
        ch.shutdown();

        boolean clean = ch.awaitTermination(remainingMs, TimeUnit.MILLISECONDS);
        if (!clean) {
            log.warning("[grpc-channel] drain timeout, forcing close sessionId=" + sessionId);
            ch.shutdownNow();
        } else {
            log.info("[grpc-channel] clean drain sessionId=" + sessionId);
        }
    }

    public void forceClose(String sessionId) {
        ManagedChannel ch = channels.remove(sessionId);
        if (ch != null) ch.shutdownNow();
    }

    public boolean has(String sessionId) { return channels.containsKey(sessionId); }
    public int size()                    { return channels.size(); }
}
