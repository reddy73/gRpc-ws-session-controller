package io.session.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Thread-safe registry of all active gRPC/WebSocket sessions. */
public class SessionRegistry {

    private final Map<String, Session> sessions = new ConcurrentHashMap<>();

    public void register(Session session) {
        session.setStartedAt(Instant.now());
        session.setLastSeen(Instant.now());
        sessions.put(session.getId(), session);
    }

    public void heartbeat(String id) {
        Session s = sessions.get(id);
        if (s != null) s.setLastSeen(Instant.now());
    }

    public void unregister(String id) {
        sessions.remove(id);
    }

    public List<Session> byPod(String podName, String namespace) {
        List<Session> result = new ArrayList<>();
        for (Session s : sessions.values()) {
            if (podName.equals(s.getPodName()) && namespace.equals(s.getNamespace()))
                result.add(s);
        }
        return result;
    }

    public List<Session> stale(long ttlSeconds) {
        Instant cutoff = Instant.now().minusSeconds(ttlSeconds);
        List<Session> result = new ArrayList<>();
        for (Session s : sessions.values()) {
            if (s.getLastSeen().isBefore(cutoff)) result.add(s);
        }
        return result;
    }

    public int size() { return sessions.size(); }
}
