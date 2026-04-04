# gRPC / WebSocket Session Survivability for Kubernetes

> reddy73 / gRpc-ws-session-controller · Public

Kubernetes was designed to kill pods freely. This project makes long-lived gRPC streams and WebSocket connections survive that — through graceful drain, endpoint rebinding, and retry classification. Implemented in **Go** and **Java**.

Topics: `kubernetes` `grpc` `websocket` `golang` `java` `session-management` `kind` `controller`

---

## Table of Contents

1. [Real-world problems this solves](#real-world-problems-this-solves)
2. [Why this exists](#why-this-exists)
3. [Does Istio fix this?](#does-istio-fix-this)
4. [Architecture](#architecture)
5. [Core concepts](#core-concepts)
6. [Kubernetes manifests](#kubernetes-manifests)
7. [Test infrastructure](#test-infrastructure)
8. [Design decisions](#design-decisions)
9. [Extension points](#extension-points)
10. [Go Implementation](#go-implementation)
11. [Java Implementation](#java-implementation)
12. [Usage Guide](#usage-guide)
13. [Runbook](#runbook)
14. [Conceptual Overview](#conceptual-overview)

---

## Real-world problems this solves

Kubernetes was built for stateless HTTP where every request is self-contained. These are the failure modes that appear the moment you run anything that isn't.

**Live trading platform — order stream drops mid-session**

A trader has an open gRPC bidirectional stream for order entry. HPA scales down a pod at 3pm when load drops. The pod gets SIGKILL after 30s. The trader's stream dies mid-order. Did the last order go through or not? Without session awareness the client reconnects and replays — the order executes twice.

With this project: the stream is classified `UNSAFE`. On SIGTERM the server sends GOAWAY. The client pauses, queries the server for the last acknowledged order sequence, confirms no gap, then reconnects cleanly from the right point.

**Collaborative document editor — rolling deploy corrupts state**

10 users are editing a document over WebSocket. A rolling update starts. The pod hosting their connections gets SIGTERM. Without drain, all 10 users get a sudden TCP RST. Some reconnect to the new pod and start sending edits. Some don't reconnect at all. The document state diverges.

With this project: the pod sends `1001 Going Away` to all 10 connections before dying. Clients get a clean signal, finish their current operation, and reconnect to the new pod. Document state stays consistent.

**Log ingestion pipeline — duplicate records after node failure**

A service streams log events to a collector over gRPC. The node crashes (OOMKilled — no SIGTERM, no grace period). The client gets a sudden `RST_STREAM` and reconnects immediately, replaying from the last checkpoint. The collector on the new pod processes those events again. Downstream analytics double-counts them.

With this project: the stream is classified `UNSAFE`. The client does not auto-reconnect. It waits for the application to confirm the server's last acknowledged sequence before resuming — preventing duplicates.

**Metrics feed — reconnect storm on rolling deploy**

100 browser clients are subscribed to a live metrics WebSocket feed. A rolling deploy happens. All 100 connections drop simultaneously. All 100 clients reconnect at the same time to the new pod — thundering herd.

With this project: the stream is classified `SAFE` (read-only subscription). The server sends `1001 Going Away` per session. Clients reconnect gracefully and re-subscribe. No thundering herd, no gap in the feed.

**HPA scale-down kills a pod with 50 active gRPC streams**

HPA decides to scale from 5 pods to 3. It picks the oldest pod. That pod has 50 active streams. Without session awareness HPA just kills it. All 50 clients get errors.

With this project: the `EndpointWatcher` detects the pod IP disappearing from the Endpoints object and rebinds all 50 sessions to surviving pods before the TCP connection is forcibly closed. The `DrainHandler` gives in-flight RPCs time to complete. The PDB config prevents HPA from killing more pods than the system can absorb.

---

## Why this exists

### The intuition: HPA scale-down

Kubernetes was designed for stateless HTTP services. Scale up when load is high, scale down when it's low, kill any pod at any time — it doesn't matter because every request is self-contained. **HPA is the purest expression of that model.** It assumes pods are interchangeable and disposable.

gRPC streams and WebSockets break that assumption. They are stateful conversations. The pod is not interchangeable mid-stream. Killing it mid-conversation is like hanging up on someone mid-sentence and expecting them to call back and pick up where they left off — sometimes that works, often it doesn't.

**This project is the adapter between those two worlds.** HPA can still scale down. Rolling updates can still happen. Node pressure can still evict pods. But now there's a layer that says: before you close that TCP connection, let the other side know what's happening and give it a chance to finish gracefully.

Every feature in this project maps directly to the HPA problem:

| Feature | What it solves |
|---|---|
| **Drain handler** | Gives the pod time to finish before HPA's SIGKILL arrives |
| **RetryClass** | Tells the client whether it's safe to reconnect after HPA kills the pod |
| **Heartbeat / stale detection** | Cleans up sessions from pods that HPA killed without warning (OOMKill, crash) |
| **EndpointWatcher** | Handles the new pod IP that appears after HPA kills the old one |
| **PDB + custom metric** | Tells HPA "not yet — there is still active work here" |

> **The core question this project answers** — What does it take to make long-lived connections survive in an environment that was designed to kill pods freely? HPA scale-down is the most concrete and relatable trigger for that question, but the same problem applies to rolling updates, node pressure, and manual scaling.

---

## Does Istio fix this?

Partially. Istio's Envoy sidecar handles the **transport layer** — it sends HTTP/2 `GOAWAY` on pod termination and load-balances new connections away from terminating pods. That's real and it works. But Istio operates at layers 4–7 and has no concept of what's inside a stream.

| Capability | Istio | This project |
|---|---|---|
| GOAWAY on pod termination | ✓ Envoy | ✓ |
| Retry safe unary RPCs | ✓ VirtualService | ✓ |
| Retry classification — SAFE / UNSAFE / CONDITIONAL | ✗ | ✓ |
| Stateful stream replay coordination | ✗ | ✓ UNSAFE pattern |
| Session registry + heartbeat + stale detection | ✗ | ✓ |
| Proactive endpoint rebinding before connection breaks | ✗ | ✓ |
| WebSocket 1001 Going Away close frame | ✗ | ✓ |
| Language-agnostic deployment | ✓ sidecar | ✓ sidecar mode |

Istio fixes the transport drain. This project fixes the **session layer** above it — which logical conversations were in progress, whether they can be safely replayed, and how to coordinate the application-level handoff. The two are complementary: Istio + this project is a stronger answer than either alone.

> **Istio's retry policy is dangerous for streaming RPCs.** It applies uniformly — it doesn't know that `StreamIngest` is unsafe to retry while `Watch` is safe. Blanket retries on stateful streams can cause duplicates, out-of-order processing, or data corruption. The `RetryClassifier` in this project exists precisely because Istio can't make that distinction.

### Connection lifetime comparison

| Property | HTTP/1.1 | gRPC Unary | gRPC Stream | WebSocket |
|---|---|---|---|---|
| Connection lifetime | Per-request | Per-request | Long-lived | Long-lived |
| State on server | None | None | Yes | Yes |
| Safe to retry blindly | Yes | Usually | **No** | **No** |
| Affected by pod restart | No | No | **Yes** | **Yes** |

Kubernetes operates at layer 3/4. It has no concept of "this TCP connection is a gRPC stream mid-message-47." When it kills a pod, it closes the socket. This project implements the missing session layer as a controller.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                      Kubernetes Cluster                       │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │               session-controller pod                 │    │
│  │                                                      │    │
│  │  ┌──────────────┐  ┌─────────────┐  ┌───────────┐  │    │
│  │  │   Session    │  │    Drain    │  │ Endpoint  │  │    │
│  │  │   Tracker   │◄─│   Handler  │  │  Watcher  │  │    │
│  │  │  RWMutex    │  │ SIGTERM→   │  │ informer  │  │    │
│  │  │  session map│  │ notify→    │  │ diff IPs  │  │    │
│  │  │             │  │ poll drain │  │ → rebind  │  │    │
│  │  └──────────────┘  └─────────────┘  └───────────┘  │    │
│  │  ┌─────────────────────────────────────────────┐    │    │
│  │  │  Retry Classifier                            │    │    │
│  │  │  method pattern → SAFE / UNSAFE / CONDITIONAL│    │    │
│  │  └─────────────────────────────────────────────┘    │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
│  ┌────────────┐   ┌────────────┐   ┌────────────┐           │
│  │echoserver-0│   │echoserver-1│   │echoserver-2│           │
│  │  :50051    │   │  :50051    │   │  :50051    │           │
│  └────────────┘   └────────────┘   └────────────┘           │
│        ▲                ▲                ▲                   │
│        └────────────────┴────────────────┘                   │
│                   Endpoints object                           │
└──────────────────────────────────────────────────────────────┘
```

### Pod termination flow

```
Kubernetes          Controller           Client
    │                    │                    │
    │── SIGTERM ─────────►│                    │
    │                    │── ByPod() ─────────►│ find sessions
    │                    │── Notify() ────────►│ GOAWAY / WS close
    │                    │                    │── reconnect ──► new pod
    │                    │── poll 500ms ───────│ wait for drain
    │── SIGKILL ──────────►│                    │
    │                    │── force unregister ─│
```

### Rolling update flow

```
Kubernetes          Controller           Client
    │                    │                    │
    │── Endpoints MODIFIED►│                    │
    │   (old IP removed)  │── reconcile() ─────│ diff old vs new
    │                    │── Rebind() ────────►│ move to new IP
    │                    │── update session ───│
```

---

## Core concepts

### Session

The fundamental unit — one long-lived connection pinned to a specific pod.

| Field | Purpose |
|---|---|
| `ID` | Unique identifier from client or gateway |
| `Type` | `grpc` or `websocket` |
| `PodName / Namespace` | Which pod owns this session |
| `Endpoint` | Pod IP:port |
| `LastSeen` | Updated on heartbeat; detects stale sessions |
| `RetryClass` | Whether this RPC is safe to retry |

### RetryClass

| Class | Meaning | Examples |
|---|---|---|
| `SAFE` | Idempotent reads. Reconnect freely. | Watch, List, Get |
| `CONDITIONAL` | Mutations — send a dedup token on retry. | Create, Update |
| `UNSAFE` | Stateful streams. Do not retry without coordination. | StreamEcho, StreamIngest |

> ⚠ **Default is UNSAFE** — unclassified methods are assumed stateful. Fail safe, not fail open. A naive retry on an UNSAFE stream could send the same transaction twice.

### Drain vs Force-Close

```
terminationGracePeriodSeconds
│
├── SIGTERM → DrainHandler.Handle()
│   ├── Notify all sessions (GOAWAY / WS close frame)
│   ├── Poll every 500ms for sessions to self-close
│   └── Deadline exceeded → force unregister stragglers
│
└── SIGKILL → OS closes all TCP connections
```

### Endpoint Rebinding

During a rolling update the `Endpoints` object changes as old pods are removed and new pods become ready. The `EndpointWatcher` diffs old vs new subsets. When a pod IP disappears it finds all sessions pinned to that IP and calls `Rebind()` — the session follows the logical connection, not the physical pod IP.

---

## Kubernetes manifests

| Setting | Value | Why |
|---|---|---|
| `terminationGracePeriodSeconds` | 30 | Window for drain before SIGKILL. Match to longest expected stream. |
| `maxUnavailable` | 1 | At least replicas-1 pods always serving. |
| `readinessProbe.grpc.port` | 50051 | Pod only added to Endpoints when health check passes. |
| `imagePullPolicy: Never` | — | Required for kind. Remove for real clusters. |

RBAC grants the controller `get/list/watch` on `endpoints` and `pods` only — no write access.

---

## Test infrastructure

Run `./test/run-tests.sh` to execute all three scenarios end-to-end:

| Test | Mechanism | Checks |
|---|---|---|
| Retry classifier | Read controller startup logs | `Watch=safe`, `StreamEvents=unsafe` |
| Graceful drain | `kubectl delete pod --grace-period=10` | Pod terminates, deployment self-heals |
| Rolling update | `kubectl set env` triggers recreation | Rollout completes, endpoint watcher active |

---

## Design decisions

### Why interfaces for DrainNotifier and SessionRebinder?

The controller is transport-agnostic. You might use `grpc.ClientConn.Close()`, a WebSocket `1001 Going Away` frame, or a message queue. Swap the `noopNotifier` in `main.go` with your real implementation.

### Why poll instead of event-driven drain?

Sessions might crash without calling `Unregister`. The deadline must be enforced regardless of session behavior. Polling at 500ms is simple, predictable, and doesn't require sessions to signal back.

### Why kind over Docker Desktop's built-in Kubernetes?

Docker Desktop ships with a single-node cluster. kind gives multi-node clusters (real cross-node scheduling), multiple independent clusters, reproducible YAML configs, and easy teardown.

---

## Extension points

- Replace `noopNotifier` with real gRPC `GOAWAY` or WebSocket close frame
- Add a heartbeat loop + `Stale(ttl)` cleanup goroutine
- Migrate `EndpointWatcher` to `EndpointSlice` (k8s 1.21+)
- Watch `pod.DeletionTimestamp` for drain lead time instead of a timer
- Persist sessions as a CRD for controller-restart survivability

---

## Go Implementation

```
go/
├── cmd/controller/main.go          # entry point, wires all components
├── cmd/echoserver/main.go          # test gRPC server (unary + bidi stream)
├── internal/session/tracker.go    # sync.RWMutex session registry
├── internal/drain/handler.go      # graceful drain orchestration
├── internal/retry/classifier.go   # RPC retry safety classification
├── internal/endpoint/watcher.go   # k8s Endpoints informer + rebinding
└── pkg/api/types.go               # shared types: Session, DrainEvent, etc.
```

### Session Tracker — sync.RWMutex

```go
type Tracker struct {
    mu       sync.RWMutex
    sessions map[string]*api.Session
}
// Register/Unregister/Heartbeat → write lock
// ByPod/Stale → read lock (concurrent reads proceed in parallel)
```

`sync.RWMutex` over `sync.Map` because range iteration (`ByPod`, `Stale`) is awkward with `sync.Map` and reads vastly outnumber writes at runtime.

### Drain Handler — ticker + select

```go
ticker := time.NewTicker(500 * time.Millisecond)
for {
    select {
    case <-ctx.Done(): return          // controller shutting down
    case <-ticker.C:
        remaining := h.tracker.ByPod(...)
        if len(remaining) == 0 { return } // clean drain
        if time.Now().After(deadline) {
            // force close stragglers
        }
    }
}
```

### Retry Classifier — pattern matching

```go
// Default rules
{MethodPattern: "*/Watch",   Class: api.RetryClassSafe},
{MethodPattern: "*/List",    Class: api.RetryClassSafe},
{MethodPattern: "*/Create",  Class: api.RetryClassConditional},
{MethodPattern: "*/Stream*", Class: api.RetryClassUnsafe},
// fallback → RetryClassUnsafe (conservative default)
```

### Endpoint Watcher — shared informer

```go
factory := informers.NewSharedInformerFactoryWithOptions(client, 0,
    informers.WithNamespace(namespace))
informer := factory.Core().V1().Endpoints().Informer()
informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
    UpdateFunc: func(old, new interface{}) {
        w.reconcile(old.(*corev1.Endpoints), new.(*corev1.Endpoints))
    },
})
```

> **Note** — Kubernetes 1.33+ deprecates `v1 Endpoints`. Migrate to `factory.Discovery().V1().EndpointSlices().Informer()`.

### Controller wiring — signal.NotifyContext

```go
ctx, cancel := signal.NotifyContext(context.Background(),
    os.Interrupt, syscall.SIGTERM)
// ctx cancelled on SIGTERM → propagates to all components
cfg, _ := rest.InClusterConfig() // reads mounted ServiceAccount token
go epWatcher.Run(ctx, client, namespace)
<-ctx.Done()
```

### Go client — replace noopNotifier

```go
// Replace noopNotifier with real GOAWAY
type grpcNotifier struct {
    conns map[string]*grpc.ClientConn
}
func (n *grpcNotifier) Notify(s *api.Session, d time.Duration) error {
    conn, ok := n.conns[s.ID]
    if !ok { return nil }
    conn.Close() // sends HTTP/2 GOAWAY
    return nil
}

// Replace noopRebinder with real reconnect
type grpcRebinder struct {
    conns map[string]*grpc.ClientConn
}
func (r *grpcRebinder) Rebind(s *api.Session, change api.EndpointChange) error {
    old := r.conns[s.ID]
    if old != nil { old.Close() }
    newConn, err := grpc.Dial(change.NewEndpoint, grpc.WithInsecure())
    if err != nil { return err }
    r.conns[s.ID] = newConn
    s.Endpoint = change.NewEndpoint
    return nil
}

// Wire in main.go
notifier := &grpcNotifier{conns: make(map[string]*grpc.ClientConn)}
rebinder := &grpcRebinder{conns: make(map[string]*grpc.ClientConn)}
drainHandler := drain.NewHandler(tracker, notifier)
epWatcher    := endpoint.NewWatcher(tracker, rebinder)
```

---

## Java Implementation

```
java/src/main/java/io/session/
├── SessionController.java              // entry point — wires all packages
├── model/
│   ├── Session.java                    // domain model: Type, RetryClass enums + fields
│   └── SessionRegistry.java           // ConcurrentHashMap registry
├── drain/
│   ├── DrainHandler.java              // ScheduledExecutorService drain loop + DrainNotifier interface
│   ├── GrpcDrainNotifier.java         // sends HTTP/2 GOAWAY via channel.shutdown()
│   └── WebSocketDrainNotifier.java    // sends WS 1001 Going Away close frame
├── retry/
│   └── RetryClassifier.java           // record-based rule matching, defaults to UNSAFE
├── endpoint/
│   └── EndpointWatcher.java           // fabric8 watcher + SessionRebinder interface
├── transport/
│   ├── GrpcChannelRegistry.java       // ManagedChannel map keyed by session ID
│   ├── WebSocketSessionRegistry.java  // WebSocket map + close-future tracking
│   ├── StreamingEchoClient.java       // real bidi gRPC stream to echoserver
│   └── WebSocketEchoClient.java       // WS sender thread for drain testing
├── client/
│   ├── SessionAwareGrpcClient.java    // GOAWAY detection + retry-class-aware reconnect
│   ├── SessionAwareWebSocketClient.java // 1001 Going Away + retry-class-aware reconnect
│   └── ClientUsageExample.java        // runnable usage demo for all three retry classes
└── sidecar/
    ├── SidecarClient.java             // app-side REST client for the Go sidecar
    └── SidecarDrainEndpoint.java      // JDK HttpServer that receives POST /drain from sidecar
```

Dependencies: `io.fabric8:kubernetes-client:6.10.0`, `io.grpc:grpc-netty-shaded:1.62.2`, Java 21.

### Session — domain model

```java
// io.session.model.Session
public class Session {
    public enum Type       { GRPC, WEBSOCKET }
    public enum RetryClass { SAFE, UNSAFE, CONDITIONAL }

    private String id;
    private Type type;
    private String podName;
    private String namespace;
    private String endpoint;
    private Instant startedAt;   // set by SessionRegistry.register()
    private Instant lastSeen;    // updated by SessionRegistry.heartbeat()
    private RetryClass retryClass;
    private Map<String, String> metadata;
    // ... getters/setters
}
```

### SessionRegistry — ConcurrentHashMap

```java
// io.session.model.SessionRegistry
private final Map<String, Session> sessions = new ConcurrentHashMap<>();

public void register(Session session)       // sets startedAt + lastSeen = now()
public void heartbeat(String id)            // updates lastSeen = now()
public void unregister(String id)
public List<Session> byPod(String podName, String namespace)
public List<Session> stale(long ttlSeconds) // sessions where lastSeen < now() - ttl
public int size()
```

Segment-level locking — concurrent reads never block each other. Compare to Go's explicit `sync.RWMutex`: Java is simpler to write correctly; Go gives finer control (hold lock across multiple operations atomically).

### DrainHandler — one executor per drain invocation

```java
// io.session.drain.DrainHandler
public DrainHandler(SessionRegistry registry, DrainNotifier notifier)

public void handle(String podName, String namespace, long deadlineMs)
// Phase 1: notify all sessions synchronously (GOAWAY / WS close frame)
// Phase 2: poll every 500ms in background until drained or deadline hit
// One ScheduledExecutorService per handle() call — safe for concurrent pod drains
// AtomicBoolean guard prevents late executions after cancel

public interface DrainNotifier {
    void notify(Session session, long remainingMs) throws Exception;
}
```

The drain loop uses a `ScheduledFuture<?>[]` self-reference trick to cancel itself from inside the task, and registers a JVM shutdown hook to clean up the executor if the process exits mid-drain.

### GrpcDrainNotifier — HTTP/2 GOAWAY

```java
// io.session.drain.GrpcDrainNotifier
public GrpcDrainNotifier(GrpcChannelRegistry channels, SessionRegistry sessions)

// notify() calls:
//   channels.drain(sessionId, remainingMs)
//     → channel.shutdown()           // sends HTTP/2 GOAWAY
//     → channel.awaitTermination()   // waits for in-flight RPCs
//     → channel.shutdownNow()        // force-close if timeout
//   sessions.unregister(sessionId)
```

### WebSocketDrainNotifier — 1001 Going Away

```java
// io.session.drain.WebSocketDrainNotifier
public WebSocketDrainNotifier(WebSocketSessionRegistry wsRegistry, SessionRegistry sessions)

// notify() calls:
//   wsRegistry.drain(sessionId, remainingMs)
//     → ws.sendClose(1001, "pod terminating")
//     → closeFuture.get(remainingMs)  // wait for close handshake
//     → ws.abort()                    // force-close if timeout
//   sessions.unregister(sessionId)
```

### RetryClassifier — Java records + pattern matching

```java
// io.session.retry.RetryClassifier
public record Rule(String methodPattern, Session.RetryClass retryClass) {}

public RetryClassifier(List<Rule> rules)
public Session.RetryClass classify(String method)  // falls back to UNSAFE

public static List<Rule> defaultRules() {
    return List.of(
        new Rule("*/Watch",   Session.RetryClass.SAFE),
        new Rule("*/List",    Session.RetryClass.SAFE),
        new Rule("*/Get",     Session.RetryClass.SAFE),
        new Rule("*/Create",  Session.RetryClass.CONDITIONAL),
        new Rule("*/Update",  Session.RetryClass.CONDITIONAL),
        new Rule("*/Stream*", Session.RetryClass.UNSAFE)
        // unmatched → UNSAFE (conservative default)
    );
}
```

Pattern syntax: `*/MethodName` matches any service, `*/Prefix*` matches prefix.

### EndpointWatcher — fabric8 + SessionRebinder

```java
// io.session.endpoint.EndpointWatcher
public EndpointWatcher(SessionRegistry registry, SessionRebinder rebinder)

public void watch(KubernetesClient client, String namespace)
// Registers a fabric8 Watcher<Endpoints>
// On MODIFIED: diffs lastKnownAddresses vs current IPs
// For each removed IP: finds sessions byPod() and calls rebinder.rebind()

public interface SessionRebinder {
    void rebind(Session session, String oldEndpoint, String newEndpoint) throws Exception;
}
```

`lastKnownAddresses` is a `ConcurrentHashMap<String, String>` (IP → podName). fabric8 auto-reconnects on transient watcher errors via `onClose(WatcherException)`.

### GrpcChannelRegistry — ManagedChannel lifecycle

```java
// io.session.transport.GrpcChannelRegistry
public ManagedChannel connect(String sessionId, String target)
// ManagedChannelBuilder.forTarget(target).usePlaintext().build()

public void drain(String sessionId, long remainingMs) throws InterruptedException
// channel.shutdown() → awaitTermination(remainingMs) → shutdownNow() on timeout

public void forceClose(String sessionId)  // shutdownNow()
public boolean has(String sessionId)
public int size()
```

### WebSocketSessionRegistry — close-future tracking

```java
// io.session.transport.WebSocketSessionRegistry
public WebSocket connect(String sessionId, String wsUri) throws Exception
// Stores both the WebSocket and a CompletableFuture<Void> per session

public void drain(String sessionId, long remainingMs) throws Exception
// ws.sendClose(1001, "pod terminating")
// closeFuture.get(remainingMs) → ws.abort() on timeout

public void forceClose(String sessionId)  // ws.abort()
public boolean has(String sessionId)
```

### SessionAwareGrpcClient — GOAWAY detection

```java
// io.session.client.SessionAwareGrpcClient
public SessionAwareGrpcClient(
    String sessionId,
    String target,
    Session.RetryClass retryClass,
    Consumer<ReconnectEvent> onReconnect)

public void start()                    // builds channel, starts connectivity watcher
public ManagedChannel channel()        // current live channel for building stubs
public void reconnect(String dedupToken) // explicit reconnect for UNSAFE streams
public void stop()
```

GOAWAY detection uses `channel.notifyWhenStateChanged()` — a one-shot listener that re-registers itself on each state transition. When the channel reaches `TRANSIENT_FAILURE` or `SHUTDOWN`:

- `SAFE` → `scheduleReconnect(0ms)` → fires `onReconnect` with `event.safe() == true`
- `CONDITIONAL` → `scheduleReconnect(500ms)` → fires `onReconnect` with `event.safe() == true`
- `UNSAFE` → fires `onReconnect` with `event.safe() == false`, no automatic reconnect

A `SessionHeaderInterceptor` attaches `x-session-id` as a gRPC metadata header on every call so the server can correlate the connection to a registry entry.

```java
public record ReconnectEvent(
    String sessionId,
    boolean safe,       // false = UNSAFE, application must coordinate
    String dedupToken,  // non-null for CONDITIONAL retries
    int attempt,
    String reason
) {}
```

### SessionAwareWebSocketClient — 1001 Going Away detection

```java
// io.session.client.SessionAwareWebSocketClient
public SessionAwareWebSocketClient(
    String sessionId,
    String wsUri,
    Session.RetryClass retryClass,
    Consumer<String> onMessage,
    Consumer<ReconnectEvent> onReconnect)

public void connect() throws Exception
public void send(String message)
public void reconnect(String dedupToken) throws Exception  // explicit reconnect for UNSAFE
public void stop()
```

The `WebSocket.Listener.onClose()` callback fires when status code `1001` is received. Behaviour mirrors `SessionAwareGrpcClient`: SAFE reconnects immediately, CONDITIONAL after 500ms, UNSAFE notifies the application and waits. `onError()` is treated as a `1001` disconnect. The `x-session-id` header is attached via `newWebSocketBuilder().header(...)`.

### SessionController — wiring everything together

```java
// io.session.SessionController — entry point
// Env vars: ECHO_TARGET (default localhost:50051), WS_TARGET (optional),
//           NAMESPACE (default "default"), TARGET_POD

SessionRegistry registry = new SessionRegistry();
GrpcChannelRegistry grpcChannels = new GrpcChannelRegistry();
WebSocketSessionRegistry wsRegistry = new WebSocketSessionRegistry();

GrpcDrainNotifier grpcNotifier = new GrpcDrainNotifier(grpcChannels, registry);
WebSocketDrainNotifier wsNotifier = new WebSocketDrainNotifier(wsRegistry, registry);

// Unified notifier dispatches by session type
DrainHandler.DrainNotifier notifier = (session, remainingMs) -> {
    if (session.getType() == Session.Type.GRPC)           grpcNotifier.notify(session, remainingMs);
    else if (session.getType() == Session.Type.WEBSOCKET) wsNotifier.notify(session, remainingMs);
};
DrainHandler drainHandler = new DrainHandler(registry, notifier);

RetryClassifier classifier = new RetryClassifier(RetryClassifier.defaultRules());

EndpointWatcher.SessionRebinder rebinder = (session, oldEp, newEp) ->
    log.info("[rebind] session=" + session.getId() + " " + oldEp + " -> " + newEp);

// Open gRPC session
ManagedChannel channel = grpcChannels.connect("grpc-session-1", ECHO_TARGET);
// ... build Session, registry.register(session), new StreamingEchoClient(channel, ...).startStreaming(1000)

// Start Kubernetes endpoint watcher
try (KubernetesClient k8s = new KubernetesClientBuilder().build()) {
    new EndpointWatcher(registry, rebinder).watch(k8s, namespace);
    drainHandler.handle(podName, namespace, 30_000);
    grpcClient.awaitClose(35_000);
}
```

---

## Usage Guide

### SDK vs Sidecar

| | SDK (embedded) | Sidecar |
|---|---|---|
| Polyglot services | ✗ | ✓ |
| Zero app changes | ✗ | ✓ (just add `/drain`) |
| Direct channel access | ✓ | ✗ |
| Operational simplicity | ✓ | ✗ |
| Independent updates | ✗ | ✓ |
| Matches service mesh pattern | ✗ | ✓ |

Single-language shop → SDK. Polyglot platform team → Sidecar. Both patterns are implemented here.

### Java SDK — embedded controller

Wire all components directly in your application. The controller runs in-process alongside your gRPC/WS server.

```java
// 1. Create registries
SessionRegistry          registry     = new SessionRegistry();
GrpcChannelRegistry      grpcChannels = new GrpcChannelRegistry();
WebSocketSessionRegistry wsReg        = new WebSocketSessionRegistry();

// 2. Wire drain notifiers
GrpcDrainNotifier      grpcNotifier = new GrpcDrainNotifier(grpcChannels, registry);
WebSocketDrainNotifier wsNotifier   = new WebSocketDrainNotifier(wsReg, registry);

DrainHandler.DrainNotifier notifier = (session, remainingMs) -> {
    if (session.getType() == Session.Type.GRPC)           grpcNotifier.notify(session, remainingMs);
    else if (session.getType() == Session.Type.WEBSOCKET) wsNotifier.notify(session, remainingMs);
};
DrainHandler drain = new DrainHandler(registry, notifier);

// 3. Open a gRPC channel and register the session
ManagedChannel ch = grpcChannels.connect("session-1", "echoserver:50051");
Session s = new Session();
s.setId("session-1");
s.setType(Session.Type.GRPC);
s.setPodName("my-pod");
s.setNamespace("default");
s.setEndpoint("echoserver:50051");
s.setRetryClass(Session.RetryClass.UNSAFE);
registry.register(s);  // sets startedAt + lastSeen automatically

// 4. On SIGTERM — fire drain (non-blocking, polls in background)
Runtime.getRuntime().addShutdownHook(new Thread(() ->
    drain.handle("my-pod", "default", 25_000)));
```

### gRPC SAFE stream — Watch / List / Get

Idempotent reads. The client reconnects automatically and re-subscribes. No coordination needed.

```java
SessionAwareGrpcClient client = new SessionAwareGrpcClient(
    "watch-session-1",
    "echoserver:50051",
    Session.RetryClass.SAFE,
    event -> {
        // event.safe() == true, event.attempt() == reconnect count
        log.info("Watch stream reconnected attempt=" + event.attempt() + " — re-subscribing");
        // Re-subscribe: call Watch() again on the new channel
        // watchStub = EchoServiceGrpc.newStub(client.channel());
        // watchStub.watch(request, responseObserver);
    }
);
client.start();
ManagedChannel ch = client.channel(); // use for stubs
```

> **SAFE behaviour** — on GOAWAY, `SessionAwareGrpcClient` reconnects immediately (0ms delay) and fires the callback with `event.safe() == true`. Your app just re-calls the Watch/List RPC on `client.channel()`.

### gRPC CONDITIONAL stream — Create / Update

Mutations with side effects. Reconnect with an idempotency key so the server ignores duplicates.

```java
AtomicInteger seq = new AtomicInteger(0);

SessionAwareGrpcClient client = new SessionAwareGrpcClient(
    "create-session-1",
    "echoserver:50051",
    Session.RetryClass.CONDITIONAL,
    event -> {
        // event.safe() == true, event.dedupToken() == null (set your own key)
        String dedupToken = "idempotency-key-" + seq.get();
        log.info("Create stream reconnected — retrying with token=" + dedupToken);
        // Retry the mutation with the same idempotency key
        // createStub.create(request.toBuilder().setIdempotencyKey(dedupToken).build(), ...);
    }
);
client.start();
```

### gRPC UNSAFE stream — StreamEcho / StreamIngest

Stateful streams. The client does NOT reconnect automatically. Your app must confirm the last acknowledged sequence number before resuming.

```java
AtomicInteger lastAckedSeq = new AtomicInteger(0);

// Use a holder so the lambda can reference the client after construction
class ClientHolder { SessionAwareGrpcClient client; }
ClientHolder holder = new ClientHolder();

holder.client = new SessionAwareGrpcClient(
    "stream-session-1",
    "echoserver:50051",
    Session.RetryClass.UNSAFE,
    event -> {
        if (!event.safe()) {
            // event.reason() == "UNSAFE stream disconnected — coordinate replay before reconnecting"
            log.warning("UNSAFE stream disconnected at seq=" + lastAckedSeq.get()
                    + " — pausing, waiting for application coordination");

            // 1. Stop sending new messages (application responsibility)
            // 2. Query server for last acknowledged sequence number
            int serverLastAck = queryServerLastAck("stream-session-1");

            if (serverLastAck == lastAckedSeq.get()) {
                // Server processed everything — safe to resume
                holder.client.reconnect("resume-from-" + serverLastAck);
            } else {
                log.severe("sequence mismatch — manual intervention required"
                        + " clientAck=" + lastAckedSeq.get()
                        + " serverAck=" + serverLastAck);
            }
        }
    }
);
holder.client.start();

// Track acks in your stream observer
// responseObserver.onNext(r) → lastAckedSeq.set(r.getSeq())
```

> ⚠ **Two Generals Problem** — there is no automatic solution for UNSAFE streams. The app must decide: at-most-once (risk gaps) or at-least-once (risk duplicates). `queryServerLastAck()` is the coordination point.

### WebSocket SAFE stream

Read-only subscriptions — price feeds, live dashboards, event streams. Reconnect and re-subscribe freely.

```java
// Constructor: (sessionId, wsUri, retryClass, onMessage, onReconnect)
SessionAwareWebSocketClient ws = new SessionAwareWebSocketClient(
    "ws-feed-1",
    "ws://echoserver:8080/stream",
    Session.RetryClass.SAFE,
    message -> log.info("ws received: " + message),   // onMessage
    event -> {
        // event.safe() == true — auto-reconnected after 1001 Going Away
        log.info("WS reconnected attempt=" + event.attempt() + " — re-subscribing to feed");
        // Re-send subscription message after reconnect
        // ws.send("{\"type\":\"subscribe\",\"channel\":\"prices\"}");
    }
);
ws.connect();
```

### WebSocket UNSAFE stream

Stateful write streams — collaborative editing, order entry, log ingestion. Same coordination pattern as gRPC UNSAFE.

```java
AtomicInteger localSeq = new AtomicInteger(0);

SessionAwareWebSocketClient ws = new SessionAwareWebSocketClient(
    "ws-orders-1",
    "ws://echoserver:8080/orders",
    Session.RetryClass.UNSAFE,
    message -> handleAck(message),
    event -> {
        if (!event.safe()) {
            // 1001 Going Away received — do NOT auto-reconnect
            // event.reason() == "UNSAFE stream — coordinate replay before reconnecting"
            int serverSeq = queryServerLastAck("ws-orders-1");
            if (serverSeq == localSeq.get()) {
                try {
                    ws.reconnect("resume-" + serverSeq); // explicit reconnect
                } catch (Exception e) { log.severe("reconnect failed: " + e.getMessage()); }
            }
        }
    }
);
ws.connect();
ws.send("{\"seq\":1,\"order\":{\"symbol\":\"AAPL\",\"qty\":100}}");
```

### Sidecar deployment

Language-agnostic. The Go sidecar runs as a second container in the same pod. The app registers sessions via REST on `localhost:9090` and exposes a `/drain` endpoint on `localhost:8080`.

```
┌─────────────────────────────────────────────────────┐
│  Pod                                                 │
│                                                      │
│  ┌──────────────────┐   ┌─────────────────────────┐ │
│  │  App container   │   │  session-controller      │ │
│  │  :50051 (gRPC)   │◄──┤  sidecar                 │ │
│  │  :8080  (/drain) │   │  :9090 (REST API)        │ │
│  │                  │   │                          │ │
│  │  POST /sessions  │──►│  SessionRegistry         │ │
│  │  DELETE /sessions│──►│  EndpointWatcher         │ │
│  │                  │◄──│  DrainTrigger            │ │
│  └──────────────────┘   └─────────────────────────┘ │
└─────────────────────────────────────────────────────┘
```

```yaml
# k8s/echoserver-sidecar-deployment.yaml
containers:
  - name: echoserver          # your app
    image: echoserver:test
    ports: [{containerPort: 50051}, {containerPort: 8080}]
    env:
      - name: DRAIN_PORT
        value: "8080"         # sidecar calls this

  - name: session-controller  # sidecar
    image: session-controller:test
    env:
      - name: SIDECAR_PORT
        value: "9090"
      - name: APP_DRAIN_URL
        value: "http://localhost:8080/drain"
      - name: POD_NAME
        valueFrom:
          fieldRef: {fieldPath: metadata.name}
```

### Java app with sidecar

The app uses `SidecarClient` to register sessions and `SidecarDrainEndpoint` to receive drain signals from the Go sidecar.

```java
// 1. Start the drain endpoint — sidecar POSTs to /drain on SIGTERM
//    Body: { "podName": "...", "namespace": "...", "deadlineMs": 30000 }
//    Responds 202 Accepted immediately; drain polls in background
SidecarDrainEndpoint endpoint = new SidecarDrainEndpoint(registry, drainHandler, 8080);
endpoint.start();
// Also exposes GET /health → { "status": "ok", "sessions": N }

// 2. Connect to sidecar REST API
SidecarClient sidecar = new SidecarClient("http://localhost:9090");

// 3. When a gRPC stream opens — register with sidecar
Session s = new Session();
s.setId("stream-1");
s.setType(Session.Type.GRPC);
s.setPodName(System.getenv("POD_NAME"));
s.setNamespace(System.getenv("NAMESPACE"));
s.setEndpoint("echoserver:50051");
s.setRetryClass(Session.RetryClass.UNSAFE);
sidecar.register(s);  // POST /sessions

// 4. Keep session alive with heartbeats
sidecar.startHeartbeat("stream-1", 30, TimeUnit.SECONDS);  // POST /sessions/{id}/heartbeat

// 5. When stream closes cleanly — unregister
sidecar.unregister("stream-1");  // DELETE /sessions/stream-1

// 6. Shutdown the heartbeat scheduler on exit
sidecar.shutdown();
```

Sidecar REST API:
```
GET    /health                          → { "status": "ok", "sessions": N }
GET    /sessions                        → list all sessions
POST   /sessions                        → register (body: Session JSON)
DELETE /sessions/{id}                   → unregister
POST   /sessions/{id}/heartbeat         → update lastSeen
POST   /drain                           → trigger drain (called by sidecar on SIGTERM)
```

### HPA scale-down protection

HPA picks pods to delete by age — it has no awareness of active streams. Three defences:

```yaml
# 1. PodDisruptionBudget — always keep at least 1 pod alive
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: echoserver-pdb
  namespace: session-test
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: echoserver
```

```yaml
# 2. Custom metric HPA — scale on active session count
# Expose active_grpc_sessions from the controller as a Prometheus metric
metrics:
  - type: Pods
    pods:
      metric:
        name: active_grpc_sessions
      target:
        type: AverageValue
        averageValue: "10"   # won't scale down while sessions > 10
```

```yaml
# 3. Long grace period — gives drain time to complete
spec:
  terminationGracePeriodSeconds: 300  # 5 min — match your longest batch
  containers:
    - name: echoserver
      env:
        - name: DRAIN_DEADLINE_S
          value: "295"  # must be < terminationGracePeriodSeconds
```

> **Best practice** — use all three together: PDB prevents aggressive scale-down, custom metric prevents scale-down while sessions are active, long grace period gives drain time to complete if scale-down does happen.

---

## Runbook

### Prerequisites

| Tool | Version | Install |
|---|---|---|
| Docker Desktop | 4.x+ | docker.com |
| kubectl | 1.29+ | `brew install kubectl` |
| kind | 0.31+ | `brew install kind` |
| Go | 1.22+ | `brew install go` |
| Java (optional) | 21+ | `brew install openjdk@21` |

### 1 — Build

```bash
# Go
cd grpc-ws-session-k8s/go
go mod tidy && go build ./...
docker build -t echoserver:test -f cmd/echoserver/Dockerfile .
docker build -t session-controller:test -f Dockerfile .
```

### 2 — Create clusters

```bash
kind create cluster --config grpc-ws-session-k8s/kind-cluster-a.yaml --name session-cluster-a
kind create cluster --config grpc-ws-session-k8s/kind-cluster-b.yaml --name session-cluster-b
```

### 3 — Load images

```bash
kind load docker-image echoserver:test         --name session-cluster-a
kind load docker-image session-controller:test  --name session-cluster-a
kind load docker-image echoserver:test         --name session-cluster-b
kind load docker-image session-controller:test  --name session-cluster-b
```

### 4 — Deploy

```bash
for ctx in kind-session-cluster-a kind-session-cluster-b; do
  kubectl --context $ctx create namespace session-test --dry-run=client -o yaml | kubectl --context $ctx apply -f -
  kubectl --context $ctx apply -f grpc-ws-session-k8s/k8s/
done
```

### 5 — Verify

```bash
# Retry classifier
kubectl --context kind-session-cluster-a -n session-test logs deployment/session-controller | head -3

# Drain
kubectl -n session-test delete pod $(kubectl -n session-test get pods -l app=echoserver -o name | head -1) --grace-period=10

# Rolling update
kubectl --context kind-session-cluster-a -n session-test set env deployment/echoserver ROLLOUT_ID="$(date +%s)"

# RBAC check (should return "no")
kubectl auth can-i delete pods --as=system:serviceaccount:session-test:session-controller -n session-test

# Full script
./grpc-ws-session-k8s/test/run-tests.sh
```

### Cleanup

```bash
kind delete cluster --name session-cluster-a
kind delete cluster --name session-cluster-b
```

### Troubleshooting

**Pod stuck in ImagePullBackOff**
Images must be loaded via `kind load` — they can't be pulled from your local daemon automatically.
```bash
kind load docker-image echoserver:test --name session-cluster-a
```

**Controller crashes with "k8s config: ..."**
Uses `InClusterConfig` — must run inside the cluster. For local testing switch to `clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))`.

**Deprecation warnings about v1 Endpoints**
Expected on k8s 1.33+. Migrate to `factory.Discovery().V1().EndpointSlices().Informer()`.

---

## Conceptual Overview

### 1. The OSI Session Layer Gap

The OSI model defines seven layers of network communication. Kubernetes is a layer 3/4 system — it routes IP packets and manages TCP connections. It has no awareness of what those connections carry.

| Layer | Name | Notes |
|---|---|---|
| 7 | Application | gRPC, WebSocket, HTTP — your code lives here |
| 6 | Presentation | TLS, encoding |
| **5** | **Session ← this project** | **Connection state, drain, rebind, retry classification** |
| 4 | Transport | TCP — Kubernetes kube-proxy, Services |
| 3 | Network | IP — Kubernetes pod networking, CNI |
| 2 | Data Link | Ethernet, node networking |
| 1 | Physical | Hardware |

Layer 5 — the session layer — is responsible for establishing, managing, and terminating sessions between applications. It is the layer that knows a conversation is in progress and can coordinate a graceful end to it. Kubernetes provides nothing here. This project fills that gap.

### 2. The Problem in Detail

#### HTTP/1.1 is stateless — gRPC streams are not

A standard HTTP/1.1 request is a complete, self-contained unit. If the server pod restarts mid-request, the client gets a connection error and retries. The retry is safe because the request carries all its context.

A gRPC bidirectional stream is fundamentally different — a long-lived conversation:

```
Client                              Server (pod)
  │                                    │
  │── StreamEcho open ─────────────────►│
  │── msg #1 "hello" ──────────────────►│
  │◄─ msg #1 echo "hello" ─────────────│
  │── msg #2 "world" ──────────────────►│
  │                                    │  ← pod receives SIGTERM here
  │◄─ RST_STREAM (connection reset) ───│  ← TCP closed by OS
  │
  │  Client now has a choice:
  │  • Reconnect and replay from msg #1? (may duplicate)
  │  • Reconnect and replay from msg #3? (needs sequence tracking)
  │  • Give up?                         (data loss)
```

The client has no way to know how many messages the server processed before dying. This is the **Two Generals Problem** applied to streaming RPCs.

#### WebSockets have the same problem

A WebSocket connection is a persistent, full-duplex channel carrying application state — a chat session, a live dashboard subscription, a collaborative editing stream. When the pod is terminated, the client receives a TCP RST and must decide what to do. Unlike HTTP, there is no idempotent "retry the request" — the session state is gone.

#### Kubernetes makes this worse

Kubernetes terminates pods routinely — rolling updates, node pressure, health check failures, manual scaling. Any architecture that assumes a pod will live for the duration of a long-lived connection is fragile by design.

| | HTTP/1.1 | gRPC Stream / WebSocket |
|---|---|---|
| State | Stateless per request | Stateful conversation in progress |
| Retry | Client retries transparently | Retry may duplicate or lose messages |
| Routing | Load balancer can route to any pod | Pinned to a specific pod IP |
| Pod restart | Invisible to the user | Breaks the session |

### 3. Anatomy of a Session Failure

#### Scenario A — Rolling update (most common)

1. **t=0** — Rollout begins. ReplicaSet creates a new pod with the new image.
2. **t=5s** — New pod becomes Ready. New pod IP added to Endpoints. New connections route to new pod.
3. **t=6s** — Old pod receives SIGTERM. Process has `terminationGracePeriodSeconds` to finish up.
4. **t=6s** — Active gRPC streams on old pod. Clients still connected to old pod IP. Endpoints no longer lists old IP, but TCP connection is still alive.
5. **t=36s** — SIGKILL. OS kills the process. All open TCP connections are reset. Clients receive `RST_STREAM` or connection refused. Sessions are broken.

> ⚠ **The gap** — Between SIGTERM and SIGKILL, active streams are in a zombie state — the pod is dying but the TCP connection is still open. Without a drain mechanism, clients don't know to reconnect until the connection is forcibly closed.

#### Scenario B — Pod crash (OOMKilled, panic)

No SIGTERM. No grace period. The process dies instantly. All open connections are reset simultaneously. Clients get a sudden `RST_STREAM` with no warning. The session controller detects this via heartbeat timeout and cleans up orphaned session records.

#### Scenario C — Node failure

The entire node goes offline. Kubernetes marks the node `NotReady` after the node heartbeat timeout (default 40s). Pods on that node are evicted and rescheduled. Sessions pinned to those pods are orphaned for up to 40s before the controller can detect and clean them up via stale detection.

### 4. Retry Safety Classification

Not all RPCs are equal. The key question when a stream breaks is: **can the client safely reconnect and replay?**

| Class | Definition | Examples | On reconnect |
|---|---|---|---|
| `SAFE` | Idempotent reads. Server has no mutable state from this stream. | Watch, List, Get, Subscribe (read-only) | Reconnect and re-subscribe freely. No coordination needed. |
| `CONDITIONAL` | Mutations with side effects. Replay is safe only with a deduplication token. | Create, Update, Upsert | Reconnect with an idempotency key. Server checks if it already processed this request. |
| `UNSAFE` | Stateful streaming writes. Server maintains ordered state across messages. Replay from any point may corrupt state. | StreamIngest, StreamWrite, BidiChat, LogStream | Do not retry without application-level coordination. |

#### The Two Generals Problem

The UNSAFE class is a direct consequence of the Two Generals Problem — a classic distributed systems impossibility result:

- The client sends message #47 to the server
- The server processes it and is about to acknowledge
- The pod dies before the acknowledgement is sent
- The client doesn't know if message #47 was processed
- Replaying message #47 may cause a duplicate
- Not replaying may cause a gap

There is no general solution. The application must choose: at-most-once (risk gaps) or at-least-once (risk duplicates), and design accordingly.

#### Idempotency Keys (CONDITIONAL)

The CONDITIONAL class solves the problem for mutations by requiring the client to generate a unique key per logical operation before sending it. The server stores a map of `idempotency_key → result`. On retry, the server checks the map and returns the cached result instead of re-executing.

```
Client                              Server
  │                                    │
  │── Create(key="abc-123", data=...) ─►│ processes, stores result
  │◄─ OK (id=42) ──────────────────────│
  │                                    │  ← pod dies
  │── Create(key="abc-123", data=...) ─►│ new pod
  │                                    │  checks: key "abc-123" exists?
  │◄─ OK (id=42) ──────────────────────│  yes → return cached result
  │                                    │  (no duplicate created)
```

> **Default is UNSAFE** — the classifier returns UNSAFE for any unrecognized method. This is the conservative choice. An unclassified stream is assumed stateful. You must explicitly opt in to SAFE or CONDITIONAL by adding a rule.

### 5. The Solution — A Session-Layer Controller

The controller sits between Kubernetes events and your application sessions. It watches for pod lifecycle changes and acts on them before the TCP connection is forcibly closed.

**Four responsibilities:**

- **Drain** — On SIGTERM, notify all sessions on the dying pod. Give clients time to finish their current exchange and reconnect to a healthy pod before SIGKILL.
- **Rebind** — On rolling update, watch the Endpoints object. When a pod IP disappears, move pinned sessions to a surviving IP.
- **Classify** — Tag each session with its retry class. Downstream systems use this to decide whether to reconnect-and-replay, reconnect-with-dedup-token, or wait for application coordination.
- **Heartbeat** — Track LastSeen per session. Detect orphaned sessions (pod crashed without drain) via stale timeout.

**What the controller does NOT do:**
- Does not proxy or intercept traffic — it is a control plane component, not a data plane proxy
- Does not solve the Two Generals Problem for UNSAFE streams — it classifies them and defers to the application
- Does not replace a service mesh (Istio, Linkerd) — it complements them at the session layer
- Does not persist sessions across controller restarts (in the demo — a CRD extension point exists)

```
┌─────────────────────────────────────────────────────┐
│  Application Layer (your code)                       │
├─────────────────────────────────────────────────────┤
│  Session Layer ← this controller                     │
│  (drain, rebind, classify, heartbeat)                │
├─────────────────────────────────────────────────────┤
│  Service Mesh (Istio/Linkerd — optional)             │
│  (mTLS, retries, circuit breaking, load balancing)   │
├─────────────────────────────────────────────────────┤
│  Kubernetes (Services, Endpoints, kube-proxy)        │
│  (TCP routing, pod scheduling, health checks)        │
└─────────────────────────────────────────────────────┘
```

### 6. Graceful Drain — How It Works

Kubernetes gives every pod a configurable window between SIGTERM and SIGKILL via `terminationGracePeriodSeconds` (default 30s). The controller must complete its work within this window.

```
t=0    SIGTERM received by pod
  │
  ├── DrainHandler.Handle() called
  │   │
  │   ├── t=0    Notify all sessions on this pod
  │   │          gRPC: send GOAWAY frame
  │   │          WebSocket: send 1001 Going Away close frame
  │   │
  │   ├── t=0.5s Poll: any sessions still open?
  │   ├── t=1.0s Poll: any sessions still open?
  │   ├── ...    (500ms interval)
  │   │
  │   ├── t=Xs   All sessions closed → clean drain ✅
  │   │
  │   └── t=30s  Deadline hit → force unregister remaining sessions
  │
t=30s  SIGKILL — OS closes all TCP connections
```

**gRPC GOAWAY** — gRPC runs over HTTP/2. HTTP/2 has a built-in graceful shutdown mechanism: the `GOAWAY` frame. When a server sends `GOAWAY`, it tells the client: "I am going away. Don't send new streams. Finish your current streams and reconnect to another server." In Go, `grpc.Server.GracefulStop()` sends `GOAWAY` and waits for active RPCs to complete.

**WebSocket close frame** — The server sends a close frame with status code `1001 Going Away`, the client acknowledges with its own close frame, and both sides close the TCP connection cleanly.

**Why poll instead of waiting for callbacks?** — The drain handler polls every 500ms rather than waiting for sessions to call `Unregister`. Three reasons: crash safety (a session might crash without calling Unregister), deadline enforcement (the SIGKILL deadline must be respected), and simplicity (polling is decoupled and predictable).

### 7. Endpoint Rebinding During Rollouts

A gRPC stream or WebSocket connection is pinned to a specific pod IP. Kubernetes Services load-balance new connections across pods, but they don't move existing connections.

```
Before rollout:
  Endpoints: 10.244.1.4:50051, 10.244.2.4:50051

Step 1 — new pod starts, old pod gets SIGTERM:
  Endpoints: 10.244.1.4:50051, 10.244.2.4:50051, 10.244.1.5:50051

Step 2 — old pod removed from Endpoints:
  Endpoints: 10.244.2.4:50051, 10.244.1.5:50051
  ↑ 10.244.1.4 is gone — sessions pinned here need rebinding

Step 3 — second old pod gets SIGTERM, new pod starts:
  Endpoints: 10.244.2.4:50051, 10.244.1.5:50051, 10.244.2.5:50051

Step 4 — rollout complete:
  Endpoints: 10.244.1.5:50051, 10.244.2.5:50051
```

The watcher receives an `UpdateFunc` event every time the Endpoints object changes. It diffs the old and new address sets. For every IP that was present before but is absent now, it finds all sessions pinned to that IP and calls `Rebind()`.

`Rebind()` is an interface — the implementation depends on your transport:
- **gRPC**: close the existing `ClientConn` and open a new one to the new IP.
- **WebSocket**: send a close frame to the client with a redirect hint, or close the connection and let the client reconnect via the Service DNS name.

The watcher uses a Kubernetes shared informer rather than polling the API server. A shared informer maintains a local in-memory cache, uses a list+watch pattern, delivers events via a work queue, handles reconnection and cache resync automatically, and is shared across multiple controllers in the same process.

### 8. Heartbeat & Stale Session Detection

Drain and rebind handle planned events — SIGTERM and rolling updates. But pods can also die suddenly: OOMKilled, kernel panic, node failure. In these cases there is no SIGTERM, no drain window, no Endpoints update. The session record becomes an orphan.

Each active session sends a periodic heartbeat to the controller (e.g., every 30s). The controller records `LastSeen = now()` on each heartbeat. A background goroutine periodically scans for sessions where `now() - LastSeen > TTL` and removes them.

```
Session lifecycle with heartbeat:

  register() → LastSeen = t0
  heartbeat() → LastSeen = t0 + 30s
  heartbeat() → LastSeen = t0 + 60s
  ...
  pod crashes → no more heartbeats
  t0 + 90s + TTL → Stale() detects orphan → Unregister()
```

Recommended defaults:
- Heartbeat interval: 30s
- TTL: 90s (3 missed heartbeats)
- Stale scan interval: 60s

### 9. Design Tradeoffs

| Decision | Choice made | Alternative | Why this choice |
|---|---|---|---|
| Session storage | In-memory map + RWMutex | CRD, etcd, Redis | Zero latency, no external dependency. Tradeoff: lost on controller restart. CRD extension point provided. |
| Drain polling | 500ms ticker | Event-driven callbacks | Decoupled from session behavior. Enforces deadline regardless of session state. |
| Retry default | UNSAFE | SAFE | Conservative. An unclassified stream is assumed stateful. Fail safe, not fail open. |
| Endpoint watch | v1 Endpoints informer | EndpointSlice informer | Simpler API. Deprecated in k8s 1.33+ — migration path documented. |
| Rebind target selection | First available new IP | Load-balanced selection | Demo simplicity. Production should use weighted round-robin or least-connections. |
| Drain notification | Interface (noopNotifier) | Hardcoded gRPC/WS | Transport-agnostic. Swap in your real implementation without changing the controller. |

> **Not a data plane proxy** — This controller does not sit in the request path. It does not intercept, buffer, or replay messages. It is a control plane component that coordinates session lifecycle.

> **Not a solution to the Two Generals Problem** — For UNSAFE streams, the controller classifies the session and signals the client to not retry blindly. It does not solve the fundamental impossibility of knowing which messages were processed before the crash.

### 10. Further Reading

**Distributed systems theory**
- [Two Generals Problem](https://en.wikipedia.org/wiki/Two_generals%27_problem) — why UNSAFE streams can't safely retry
- [Fallacies of Distributed Computing](https://en.wikipedia.org/wiki/Fallacies_of_distributed_computing) — the network is not reliable
- [OSI Session Layer](https://en.wikipedia.org/wiki/Session_layer) — what this project implements

**gRPC**
- [gRPC Core Concepts](https://grpc.io/docs/what-is-grpc/core-concepts/) — streams, metadata, deadlines
- [HTTP/2 GOAWAY frame (RFC 7540)](https://httpwg.org/specs/rfc7540.html#GOAWAY) — graceful shutdown signal
- [gRPC Keepalive](https://grpc.io/docs/guides/keepalive/) — detecting dead connections

**Kubernetes**
- [Pod Lifecycle](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/) — SIGTERM, grace periods, termination
- [EndpointSlice](https://kubernetes.io/docs/concepts/services-networking/endpoint-slices/) — the modern replacement for Endpoints
- [client-go Shared Informers](https://pkg.go.dev/k8s.io/client-go/informers) — list+watch pattern

**Idempotency**
- [Stripe Idempotency Keys](https://stripe.com/docs/api/idempotent_requests) — the CONDITIONAL retry pattern in production
- [Making retries safe with idempotent APIs](https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/) — AWS Builders' Library

---

*Built and tested on kind 0.31.0 · Kubernetes 1.35.0 · Docker Desktop 28.0.4 · macOS*
