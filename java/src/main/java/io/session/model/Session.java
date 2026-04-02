package io.session.model;

import java.time.Instant;
import java.util.Map;

/** Represents a long-lived gRPC stream or WebSocket connection. */
public class Session {

    public enum Type { GRPC, WEBSOCKET }

    public enum RetryClass {
        SAFE,         // idempotent, no side effects
        UNSAFE,       // stateful, must rebind
        CONDITIONAL   // retry with dedup token
    }

    private String id;
    private Type type;
    private String podName;
    private String namespace;
    private String endpoint;
    private Instant startedAt;
    private Instant lastSeen;
    private RetryClass retryClass;
    private Map<String, String> metadata;

    public String getId()                          { return id; }
    public void setId(String id)                   { this.id = id; }
    public Type getType()                          { return type; }
    public void setType(Type type)                 { this.type = type; }
    public String getPodName()                     { return podName; }
    public void setPodName(String podName)         { this.podName = podName; }
    public String getNamespace()                   { return namespace; }
    public void setNamespace(String namespace)     { this.namespace = namespace; }
    public String getEndpoint()                    { return endpoint; }
    public void setEndpoint(String endpoint)       { this.endpoint = endpoint; }
    public Instant getStartedAt()                  { return startedAt; }
    public void setStartedAt(Instant startedAt)    { this.startedAt = startedAt; }
    public Instant getLastSeen()                   { return lastSeen; }
    public void setLastSeen(Instant lastSeen)      { this.lastSeen = lastSeen; }
    public RetryClass getRetryClass()              { return retryClass; }
    public void setRetryClass(RetryClass rc)       { this.retryClass = rc; }
    public Map<String, String> getMetadata()       { return metadata; }
    public void setMetadata(Map<String, String> m) { this.metadata = m; }

    @Override
    public String toString() {
        return "Session{id=" + id + ", type=" + type + ", pod=" + podName + "/" + namespace + "}";
    }
}
