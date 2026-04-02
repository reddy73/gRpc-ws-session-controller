package io.session.transport;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/** Manages WebSocket connections keyed by session ID. */
public class WebSocketSessionRegistry {

    private static final Logger log = Logger.getLogger(WebSocketSessionRegistry.class.getName());

    private static final int    WS_GOING_AWAY        = 1001;
    private static final String WS_GOING_AWAY_REASON = "pod terminating";

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ConcurrentHashMap<String, WebSocket>                sockets      = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<Void>>  closeFutures = new ConcurrentHashMap<>();

    public WebSocket connect(String sessionId, String wsUri) throws Exception {
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        closeFutures.put(sessionId, closeFuture);

        WebSocket ws = httpClient.newWebSocketBuilder()
                .buildAsync(URI.create(wsUri), new Listener(sessionId, closeFuture))
                .get(10, TimeUnit.SECONDS);

        sockets.put(sessionId, ws);
        log.info("[ws-registry] connected sessionId=" + sessionId + " uri=" + wsUri);
        return ws;
    }

    /** Send 1001 Going Away and wait for close handshake. */
    public void drain(String sessionId, long remainingMs) throws Exception {
        WebSocket ws = sockets.remove(sessionId);
        CompletableFuture<Void> closeFuture = closeFutures.remove(sessionId);
        if (ws == null) return;

        log.info("[ws-registry] sending 1001 Going Away sessionId=" + sessionId
                + " budget=" + remainingMs + "ms");
        ws.sendClose(WS_GOING_AWAY, WS_GOING_AWAY_REASON);

        if (closeFuture != null) {
            try {
                closeFuture.get(remainingMs, TimeUnit.MILLISECONDS);
                log.info("[ws-registry] clean close handshake sessionId=" + sessionId);
            } catch (Exception e) {
                log.warning("[ws-registry] close timeout, aborting sessionId=" + sessionId);
                ws.abort();
            }
        }
    }

    public void forceClose(String sessionId) {
        WebSocket ws = sockets.remove(sessionId);
        closeFutures.remove(sessionId);
        if (ws != null) ws.abort();
    }

    public boolean has(String sessionId) { return sockets.containsKey(sessionId); }

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
            log.info("[ws-client] received #" + (++received) + " sessionId=" + sessionId);
            ws.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket ws, int statusCode, String reason) {
            log.info("[ws-client] close handshake complete sessionId=" + sessionId
                    + " status=" + statusCode);
            closeFuture.complete(null);
            return null;
        }

        @Override
        public void onError(WebSocket ws, Throwable error) {
            log.warning("[ws-client] error sessionId=" + sessionId + " " + error.getMessage());
            closeFuture.completeExceptionally(error);
        }
    }
}
