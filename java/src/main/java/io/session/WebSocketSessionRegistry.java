package io.session;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Manages WebSocket connections keyed by session ID.
 * Mirrors GrpcChannelRegistry — same lifecycle, different transport.
 *
 * drain() sends a 1001 Going Away close frame and waits for the
 * server to echo the close handshake before returning.
 */
public class WebSocketSessionRegistry {

    private static final Logger log = Logger.getLogger(WebSocketSessionRegistry.class.getName());

    // Close status 1001 = Going Away (server shutting down / pod terminating)
    private static final int WS_GOING_AWAY = 1001;
    private static final String WS_GOING_AWAY_REASON = "pod terminating";

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ConcurrentHashMap<String, WebSocket> sockets = new ConcurrentHashMap<>();
    // Latch per session — released when close handshake completes
    private final ConcurrentHashMap<String, CompletableFuture<Void>> closeFutures = new ConcurrentHashMap<>();

    /** Connect to wsUri (e.g. "ws://host:port/path") and register under sessionId. */
    public WebSocket connect(String sessionId, String wsUri) throws Exception {
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        closeFutures.put(sessionId, closeFuture);

        WebSocket ws = httpClient.newWebSocketBuilder()
                .buildAsync(URI.create(wsUri), new Listener(sessionId, closeFuture))
                .get(10, TimeUnit.SECONDS);

        sockets.put(sessionId, ws);
        log.info("[ws] connected sessionId=" + sessionId + " uri=" + wsUri);
        return ws;
    }

    /**
     * Send a 1001 Going Away close frame and wait for the close handshake
     * to complete within remainingMs. This is the WS equivalent of GOAWAY.
     */
    public void drain(String sessionId, long remainingMs) throws Exception {
        WebSocket ws = sockets.remove(sessionId);
        CompletableFuture<Void> closeFuture = closeFutures.remove(sessionId);
        if (ws == null) return;

        log.info("[ws] sending 1001 Going Away sessionId=" + sessionId
                + " budget=" + remainingMs + "ms");

        // Send close frame — server should echo back to complete the handshake
        ws.sendClose(WS_GOING_AWAY, WS_GOING_AWAY_REASON);

        // Wait for the close handshake to complete
        if (closeFuture != null) {
            try {
                closeFuture.get(remainingMs, TimeUnit.MILLISECONDS);
                log.info("[ws] clean close handshake sessionId=" + sessionId);
            } catch (Exception e) {
                log.warning("[ws] close handshake timeout, aborting sessionId=" + sessionId);
                ws.abort();
            }
        }
    }

    /** Force-abort without waiting — used when deadline is exceeded. */
    public void forceClose(String sessionId) {
        WebSocket ws = sockets.remove(sessionId);
        closeFutures.remove(sessionId);
        if (ws != null) {
            ws.abort();
            log.warning("[ws] force-aborted sessionId=" + sessionId);
        }
    }

    public boolean has(String sessionId) {
        return sockets.containsKey(sessionId);
    }

    /** WebSocket listener — tracks messages and signals close completion. */
    private static class Listener implements WebSocket.Listener {

        private static final Logger log = Logger.getLogger(Listener.class.getName());

        private final String sessionId;
        private final CompletableFuture<Void> closeFuture;
        private int received = 0;

        Listener(String sessionId, CompletableFuture<Void> closeFuture) {
            this.sessionId = sessionId;
            this.closeFuture = closeFuture;
        }

        @Override
        public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
            received++;
            log.info("[ws] received #" + received + " sessionId=" + sessionId
                    + " data=" + data.toString().substring(0, Math.min(40, data.length())));
            ws.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket ws, int statusCode, String reason) {
            log.info("[ws] close handshake complete sessionId=" + sessionId
                    + " status=" + statusCode + " reason=" + reason);
            closeFuture.complete(null);
            return null;
        }

        @Override
        public void onError(WebSocket ws, Throwable error) {
            log.warning("[ws] error sessionId=" + sessionId + " " + error.getMessage());
            closeFuture.completeExceptionally(error);
        }
    }
}
