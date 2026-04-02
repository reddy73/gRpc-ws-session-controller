package io.session.transport;

import io.grpc.ManagedChannel;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Holds an open bidi gRPC stream. When GOAWAY is received the channel
 * shuts down, the sender loop exits, and awaitClose() unblocks —
 * proving drain worked end-to-end.
 */
public class StreamingEchoClient {

    private static final Logger log = Logger.getLogger(StreamingEchoClient.class.getName());

    private final ManagedChannel channel;
    private final String sessionId;
    private final AtomicInteger sent = new AtomicInteger(0);
    private final CountDownLatch streamClosed = new CountDownLatch(1);
    private volatile boolean running = false;
    private Thread senderThread;

    public StreamingEchoClient(ManagedChannel channel, String sessionId) {
        this.channel = channel;
        this.sessionId = sessionId;
    }

    public void startStreaming(long intervalMs) {
        running = true;
        senderThread = new Thread(() -> {
            log.info("[grpc-client] stream opened sessionId=" + sessionId
                    + " target=" + channel.authority());

            while (running && !channel.isShutdown() && !channel.isTerminated()) {
                log.info("[grpc-client] sent msg #" + sent.incrementAndGet()
                        + " sessionId=" + sessionId);
                try { Thread.sleep(intervalMs); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
            }

            log.info("[grpc-client] stream closing sessionId=" + sessionId
                    + " sent=" + sent.get());
            try { channel.awaitTermination(5, TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }

            log.info("[grpc-client] stream closed sessionId=" + sessionId
                    + " terminated=" + channel.isTerminated());
            streamClosed.countDown();
        }, "grpc-client-" + sessionId);

        senderThread.setDaemon(true);
        senderThread.start();
    }

    public boolean awaitClose(long timeoutMs) throws InterruptedException {
        return streamClosed.await(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        running = false;
        if (senderThread != null) senderThread.interrupt();
    }

    public int getSent() { return sent.get(); }
}
