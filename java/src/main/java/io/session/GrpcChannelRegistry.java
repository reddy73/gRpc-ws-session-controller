package io.session;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Manages the lifecycle of gRPC ManagedChannels keyed by session ID.
 * This is the real replacement for the noopNotifier — it holds actual
 * connections that can be drained via channel.shutdown() + awaitTermination().
 */
public class GrpcChannelRegistry {

    private static final Logger log = Logger.getLogger(GrpcChannelRegistry.class.getName());

    private final ConcurrentHashMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    /** Open a plaintext channel to target (host:port) and register it under sessionId. */
    public ManagedChannel connect(String sessionId, String target) {
        ManagedChannel channel = ManagedChannelBuilder
                .forTarget(target)
                .usePlaintext()
                .build();
        channels.put(sessionId, channel);
        log.info("[channel] opened sessionId=" + sessionId + " target=" + target);
        return channel;
    }

    /**
     * Gracefully drain the channel for sessionId.
     * Calls shutdown() which sends HTTP/2 GOAWAY to the server,
     * then waits up to remainingMs for in-flight RPCs to complete.
     */
    public void drain(String sessionId, long remainingMs) throws InterruptedException {
        ManagedChannel ch = channels.remove(sessionId);
        if (ch == null) return;

        log.info("[channel] draining sessionId=" + sessionId + " budget=" + remainingMs + "ms");
        ch.shutdown();

        boolean clean = ch.awaitTermination(remainingMs, TimeUnit.MILLISECONDS);
        if (clean) {
            log.info("[channel] clean drain sessionId=" + sessionId);
        } else {
            log.warning("[channel] drain timeout, forcing close sessionId=" + sessionId);
            ch.shutdownNow();
        }
    }

    /** Force-close without waiting — used when deadline is already exceeded. */
    public void forceClose(String sessionId) {
        ManagedChannel ch = channels.remove(sessionId);
        if (ch != null) {
            ch.shutdownNow();
            log.warning("[channel] force-closed sessionId=" + sessionId);
        }
    }

    public boolean has(String sessionId) {
        return channels.containsKey(sessionId);
    }

    public int size() {
        return channels.size();
    }
}
