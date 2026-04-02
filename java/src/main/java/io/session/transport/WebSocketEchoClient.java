package io.session.transport;

import java.net.http.WebSocket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/** Holds an open WebSocket and sends messages periodically. Makes WS drain observable. */
public class WebSocketEchoClient {

    private static final Logger log = Logger.getLogger(WebSocketEchoClient.class.getName());

    private final WebSocket ws;
    private final String sessionId;
    private final AtomicInteger sent = new AtomicInteger(0);
    private final CountDownLatch closed = new CountDownLatch(1);
    private volatile boolean running = false;
    private Thread senderThread;

    public WebSocketEchoClient(WebSocket ws, String sessionId) {
        this.ws = ws;
        this.sessionId = sessionId;
    }

    public void startStreaming(long intervalMs) {
        running = true;
        senderThread = new Thread(() -> {
            log.info("[ws-client] stream opened sessionId=" + sessionId);

            while (running && !ws.isInputClosed() && !ws.isOutputClosed()) {
                int n = sent.incrementAndGet();
                ws.sendText("{\"seq\":" + n + ",\"session\":\"" + sessionId + "\"}", true);
                log.info("[ws-client] sent #" + n + " sessionId=" + sessionId);
                try { Thread.sleep(intervalMs); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
            }

            log.info("[ws-client] stream closing sessionId=" + sessionId + " sent=" + sent.get());
            closed.countDown();
        }, "ws-client-" + sessionId);

        senderThread.setDaemon(true);
        senderThread.start();
    }

    public boolean awaitClose(long timeoutMs) throws InterruptedException {
        return closed.await(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        running = false;
        if (senderThread != null) senderThread.interrupt();
    }

    public int getSent() { return sent.get(); }
}
