# gRPC / WebSocket Session Survivability for Kubernetes

## Learning Guide

This project teaches you how to build a **session-layer controller** for Kubernetes that keeps
long-lived gRPC streams and WebSocket connections alive across pod terminations, rolling updates,
and endpoint changes.

Normal HTTP/1.1 is stateless — every request is independent. gRPC streams and WebSockets are
**conversations**: they hold state, ordering, and flow control across many messages. Kubernetes
knows nothing about this. When it kills a pod, it just closes the TCP connection. This project
fills that gap.

---

## Table of Contents

1. [Background — Why This Problem Exists](#1-background)
2. [Architecture Overview](#2-architecture-overview)
3. [Core Concepts](#3-core-concepts)
4. [Go Implementation](#4-go-implementation)
5. [Java Implementation](#5-java-implementation)
6. [Kubernetes Manifests](#6-kubernetes-manifests)
7. [Test Infrastructure](#7-test-infrastructure)
8. [Running Locally with kind](#8-running-locally-with-kind)
9. [Key Design Decisions](#9-key-design-decisions)
10. [Extension Points](#10-extension-points)
11. [Further Reading](#11-further-reading)

---

## 1. Background

### HTTP/1.1 vs gRPC Streams vs WebSockets

| Property | HTTP/1.1 | gRPC Unary | gRPC Stream | WebSocket |
|---|---|---|---|---|
| Connection lifetime | Per-request | Per-request | Long-lived | Long-lived |
| State on server | None | None | Yes (stream context) | Yes (session) |
| Safe to retry blindly | Yes | Usually | **No** | **No** |
| Affected by pod restart | No | No | **Yes** | **Yes** |

When Kubernetes terminates a pod it sends `SIGTERM`, waits `terminationGracePeriodSeconds`,
then sends `SIGKILL`. Any open TCP connections are closed. For HTTP/1.1 the client just retries.
For a gRPC bidirectional stream mid-flight, the client gets an abrupt `RST_STREAM` or
`GOAWAY` and has to decide: is it safe to reconnect and replay? Often it is not.

### The Session Layer Gap

The OSI model has a session layer (layer 5) that manages connection state. Kubernetes operates
at layer 3/4 (IP routing, TCP load balancing). There is nothing in between that understands
"this TCP connection is a gRPC stream that has sent 47 messages and is waiting for message 48."

This project implements that missing session layer as a Kubernetes controller.

### What "Survivability" Means Here

- **Drain**: before a pod dies, tell its clients to finish up and reconnect elsewhere
- **Rebind**: when a pod's IP changes (rollout), move pinned sessions to the new endpoint
- **Retry classification**: know which RPCs can be safely retried vs which need dedup tokens
- **Heartbeat tracking**: detect zombie sessions that lost their pod silently

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Kubernetes Cluster                        │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                  session-controller pod                   │   │
│  │                                                           │   │
│  │  ┌─────────────┐   ┌──────────────┐   ┌──────────────┐  │   │
│  │  │   Session   │   │    Drain     │   │   Endpoint   │  │   │
│  │  │   Tracker   │◄──│   Handler   │   │   Watcher    │  │   │
│  │  │             │   │             │   │              │  │   │
│  │  │ in-memory   │   │ SIGTERM →   │   │ k8s informer │  │   │
│  │  │ session map │   │ notify →    │   │ diff old/new │  │   │
│  │  │ RWMutex     │   │ poll drain  │   │ IPs → rebind │  │   │
│  │  └─────────────┘   └──────────────┘   └──────────────┘  │   │
│  │                                                           │   │
│  │  ┌──────────────────────────────────────────────────┐    │   │
│  │  │              Retry Classifier                     │    │   │
│  │  │  method pattern → SAFE / UNSAFE / CONDITIONAL    │    │   │
│  │  └──────────────────────────────────────────────────┘    │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │ echoserver-0 │    │ echoserver-1 │    │ echoserver-2 │       │
│  │  pod         │    │  pod         │    │  pod (new)   │       │
│  │  :50051      │    │  :50051      │    │  :50051      │       │
│  └──────────────┘    └──────────────┘    └──────────────┘       │
│         ▲                   ▲                   ▲                │
│         └───────────────────┴───────────────────┘               │
│                    Endpoints object                              │
│              (watched by EndpointWatcher)                        │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow: Pod Termination

```
Kubernetes                Controller              Client
    │                         │                      │
    │── SIGTERM ──────────────►│                      │
    │                         │── ByPod() ──────────►│ (find sessions)
    │                         │◄─ [session list] ────│
    │                         │── Notify() ─────────►│ (GOAWAY / WS close)
    │                         │                      │── reconnect ──► new pod
    │                         │── poll ─────────────►│ (wait for drain)
    │── SIGKILL (after grace) ►│                      │
    │                         │── force unregister ──│
```

### Data Flow: Rolling Update

```
Kubernetes                Controller              Client
    │                         │                      │
    │── update Endpoints ─────►│                      │
    │   (old IP removed)       │── reconcile() ──────►│
    │                         │   diff old vs new     │
    │                         │── Rebind() ──────────►│ (move session to new IP)
    │                         │── update session ─────│
```

---

## 3. Core Concepts

### 3.1 Session

A `Session` is the fundamental unit. It represents one long-lived connection — either a gRPC
bidirectional stream or a WebSocket connection — pinned to a specific pod.

Key fields:

| Field | Purpose |
|---|---|
| `ID` | Unique identifier, generated by the client or a gateway |
| `Type` | `grpc` or `websocket` |
| `PodName` / `Namespace` | Which pod owns this session |
| `Endpoint` | Pod IP:port the session is connected to |
| `LastSeen` | Updated on each heartbeat; used to detect stale sessions |
| `RetryClass` | Whether this session's RPC is safe to retry |

### 3.2 RetryClass

This is the most important concept for correctness.

```
SAFE        — idempotent reads. Watch, List, Get.
              Client can reconnect and re-subscribe freely.

CONDITIONAL — mutations with side effects. Create, Update.
              Client must send a deduplication token (idempotency key)
              so the server can detect and ignore replays.

UNSAFE      — stateful streaming writes. StreamEcho, StreamIngest.
              Client MUST NOT retry without application-level coordination.
              Replaying messages could cause duplicates, corruption, or
              out-of-order processing.
```

**Why this matters**: a naive retry-on-reconnect for an `UNSAFE` stream could send the same
financial transaction twice, corrupt a log stream, or break a stateful protocol.

### 3.3 Drain vs Force-Close

```
terminationGracePeriodSeconds (k8s setting)
│
├── SIGTERM received
│   └── DrainHandler.Handle() called
│       ├── Notify all sessions (GOAWAY / WS close frame)
│       ├── Poll every 500ms for sessions to self-close
│       └── If deadline passes → force unregister remaining sessions
│
└── SIGKILL (after grace period)
    └── OS closes all TCP connections
```

The goal is to give clients time to finish their current message exchange and reconnect to a
healthy pod **before** the TCP connection is forcibly closed.

### 3.4 Endpoint Rebinding

During a rolling update, Kubernetes replaces pods one at a time. The `Endpoints` object is
updated as each old pod is removed and each new pod becomes ready.

The `EndpointWatcher` diffs the old and new `Endpoints` subsets. When a pod IP disappears,
it finds all sessions pinned to that IP and calls `Rebind()` to move them to a surviving IP.

This is "sticky stream" behavior — the session follows the logical connection, not the physical
pod IP.

### 3.5 Heartbeat and Stale Detection

Sessions call `Heartbeat(id)` periodically (e.g., every 30s). The tracker records `LastSeen`.
`Stale(ttl)` returns sessions that haven't heartbeated within the TTL — these are likely
orphaned (pod died without a clean drain event) and should be cleaned up.

---

## 4. Go Implementation

### Project Layout

```
go/
├── cmd/
│   ├── controller/
│   │   └── main.go          # entry point, wires all components
│   └── echoserver/
│       ├── main.go          # test gRPC server (unary + bidi streaming)
│       ├── Dockerfile
│       └── proto/
│           ├── echo.proto   # service definition
│           └── echo.pb.go   # hand-written stubs (no protoc needed)
├── internal/
│   ├── session/
│   │   └── tracker.go       # thread-safe session registry
│   ├── drain/
│   │   └── handler.go       # graceful drain orchestration
│   ├── retry/
│   │   └── classifier.go    # RPC retry safety classification
│   └── endpoint/
│       └── watcher.go       # k8s Endpoints informer + rebinding
├── pkg/
│   └── api/
│       └── types.go         # shared types: Session, DrainEvent, etc.
├── Dockerfile
└── go.mod
```

The `internal/` packages are private to this module. `pkg/api/` is the public contract —
types shared across packages.

---

### 4.1 `pkg/api/types.go` — Shared Types

```go
type Session struct {
    ID         string
    Type       SessionType   // "grpc" | "websocket"
    PodName    string
    Namespace  string
    Endpoint   string
    StartedAt  time.Time
    LastSeen   time.Time
    RetryClass RetryClass    // "safe" | "unsafe" | "conditional"
    Metadata   map[string]string
}
```

`DrainEvent` carries the pod name and the termination deadline duration.
`EndpointChange` carries old and new IP addresses for a rebind operation.

These types are intentionally simple structs — no interfaces, no embedding. Easy to serialize,
easy to test.

---

### 4.2 `internal/session/tracker.go` — Session Registry

```go
type Tracker struct {
    mu       sync.RWMutex
    sessions map[string]*api.Session
}
```

Uses `sync.RWMutex` for concurrent access:
- `Register`, `Unregister`, `Heartbeat` take a write lock (`Lock`)
- `ByPod`, `Stale` take a read lock (`RLock`) — multiple readers can proceed in parallel

**Why `RWMutex` over `sync.Mutex`?**
In a real controller, reads (ByPod lookups during drain, stale checks) vastly outnumber writes
(register/unregister). `RWMutex` allows concurrent reads, which reduces contention.

**Why not `sync.Map`?**
`sync.Map` is optimized for append-only or mostly-read workloads with disjoint keys per goroutine.
Here we need range iteration (`ByPod`, `Stale`) which is awkward with `sync.Map`. A plain map
with `RWMutex` is clearer and performs well at this scale.

```go
// ByPod — find all sessions on a specific pod (called during drain)
func (t *Tracker) ByPod(podName, namespace string) []*api.Session {
    t.mu.RLock()
    defer t.mu.RUnlock()
    // linear scan — acceptable for O(hundreds) of sessions per pod
    ...
}
```

---

### 4.3 `internal/drain/handler.go` — Graceful Drain

The drain sequence:

```go
func (h *Handler) Handle(ctx context.Context, event api.DrainEvent) {
    sessions := h.tracker.ByPod(event.PodName, event.Namespace)

    deadline := time.Now().Add(event.Deadline)

    // 1. Notify all sessions immediately
    for _, s := range sessions {
        h.notifier.Notify(s, time.Until(deadline))
    }

    // 2. Poll until drained or deadline
    ticker := time.NewTicker(500 * time.Millisecond)
    for {
        select {
        case <-ctx.Done():        // controller shutting down
            return
        case <-ticker.C:
            remaining := h.tracker.ByPod(...)
            if len(remaining) == 0 { return }   // clean drain
            if time.Now().After(deadline) {
                // force close stragglers
                for _, s := range remaining { h.tracker.Unregister(s.ID) }
                return
            }
        }
    }
}
```

The `DrainNotifier` interface is the integration point:

```go
type DrainNotifier interface {
    Notify(s *api.Session, deadline time.Duration) error
}
```

For gRPC: call `conn.Close()` or send a `GOAWAY` frame via the gRPC channel.
For WebSocket: send a `1001 Going Away` close frame.

The `noopNotifier` in `main.go` just logs — replace it with your real transport logic.

---

### 4.4 `internal/retry/classifier.go` — Retry Safety

```go
type Rule struct {
    MethodPattern string    // e.g. "*/Watch", "*/Stream*"
    Class         api.RetryClass
}
```

Pattern matching rules:
- `*/Watch` — matches any service's `Watch` method: `/foo.v1.FooService/Watch`
- `*/Stream*` — matches any method starting with `Stream`: `StreamEcho`, `StreamIngest`
- Exact string — matches only that full method path

```go
func (c *Classifier) Classify(method string) api.RetryClass {
    for _, r := range c.rules {
        if matchPattern(r.MethodPattern, method) {
            return r.Class
        }
    }
    return api.RetryClassUnsafe  // conservative default
}
```

The default is `UNSAFE` — if you haven't explicitly classified a method, assume it's not safe
to retry. This is the correct conservative stance for streaming RPCs.

---

### 4.5 `internal/endpoint/watcher.go` — Endpoint Rebinding

Uses `k8s.io/client-go` shared informers:

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

**Why shared informers?**
A shared informer maintains a local cache of Kubernetes objects and delivers events via a
work queue. It handles reconnection, list+watch semantics, and cache sync automatically.
You don't poll the API server — the informer pushes changes to you.

The `reconcile` function diffs old vs new endpoint subsets:

```go
for addr := range oldAddrs {
    if _, stillPresent := newAddrs[addr]; !stillPresent {
        // This IP was removed from the Endpoints object
        // Find sessions pinned to it and rebind them
        sessions := w.tracker.ByPod(oldAddrs[addr], namespace)
        for _, s := range sessions {
            w.rebinder.Rebind(s, EndpointChange{OldEndpoint: addr, NewEndpoint: newAddr})
        }
    }
}
```

**Note on EndpointSlice**: Kubernetes 1.33+ deprecates `v1 Endpoints` in favor of
`discovery.k8s.io/v1 EndpointSlice`. The watcher works but you'll see deprecation warnings.
Migrate by switching the informer to `factory.Discovery().V1().EndpointSlices()`.

---

### 4.6 `cmd/echoserver/main.go` — Test gRPC Server

The echo server exists purely for testing. It implements two RPCs:

```go
// Unary — safe to retry (stateless)
func (s *echoServer) Echo(ctx context.Context, req *pb.EchoRequest) (*pb.EchoResponse, error)

// Bidirectional streaming — unsafe to retry (stateful)
func (s *echoServer) StreamEcho(stream pb.EchoService_StreamEchoServer) error
```

On `SIGTERM` it calls `srv.GracefulStop()` which:
1. Stops accepting new connections
2. Waits for all active RPCs to complete
3. Then closes the listener

This is the gRPC-native drain mechanism. The `terminationGracePeriodSeconds` in the pod spec
gives it time to do this before `SIGKILL`.

---

### 4.7 `cmd/controller/main.go` — Wiring

```go
// Signal-aware context — cancelled on SIGTERM/SIGINT
ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

// In-cluster config — reads the ServiceAccount token mounted at
// /var/run/secrets/kubernetes.io/serviceaccount/
cfg, err := rest.InClusterConfig()

// Wire components
tracker     := session.NewTracker()
classifier  := retry.NewClassifier(retry.DefaultRules())
drainHandler := drain.NewHandler(tracker, &noopNotifier{})
epWatcher   := endpoint.NewWatcher(tracker, &noopRebinder{})

// Start endpoint watcher in background goroutine
go epWatcher.Run(ctx, client, namespace)

// Block until signal
<-ctx.Done()
```

`signal.NotifyContext` is the idiomatic Go way to handle graceful shutdown since Go 1.16.
When `SIGTERM` arrives, `ctx` is cancelled, which propagates to all components that accept
a `context.Context`.

---

## 5. Java Implementation

### Project Layout

```
java/
├── src/main/java/io/session/
│   ├── Session.java           # domain model (enums + getters/setters)
│   ├── SessionRegistry.java   # ConcurrentHashMap-based registry
│   ├── DrainHandler.java      # ScheduledExecutorService drain loop
│   ├── RetryClassifier.java   # pattern-based RPC classification
│   ├── EndpointWatcher.java   # fabric8 Kubernetes client watcher
│   └── SessionController.java # main entry point
└── pom.xml
```

Dependencies:
- `io.fabric8:kubernetes-client:6.10.0` — Kubernetes API client
- `io.grpc:grpc-netty-shaded:1.62.2` — gRPC runtime
- Java 21 (uses `record` types in `RetryClassifier`)

---

### 5.1 `Session.java` — Domain Model

```java
public class Session {
    public enum Type { GRPC, WEBSOCKET }

    public enum RetryClass {
        SAFE,         // idempotent, no side effects
        UNSAFE,       // stateful, must rebind
        CONDITIONAL   // retry with dedup token
    }
    // fields: id, type, podName, namespace, endpoint,
    //         startedAt, lastSeen, retryClass, metadata
}
```

The enums mirror the Go constants exactly. This matters if you ever need the two
implementations to interoperate via a shared API or message bus.

---

### 5.2 `SessionRegistry.java` — Thread-Safe Registry

```java
private final Map<String, Session> sessions = new ConcurrentHashMap<>();
```

`ConcurrentHashMap` provides thread-safe reads and writes without explicit locking.
It uses segment-level locking internally, so concurrent reads never block each other
and concurrent writes to different keys don't block each other either.

Compare to Go's approach:
- Go uses `sync.RWMutex` + plain `map` — explicit, fine-grained control
- Java uses `ConcurrentHashMap` — implicit, higher-level abstraction

Both are correct. The Go approach gives you more control (e.g., you can hold the read lock
across multiple operations atomically). The Java approach is simpler to write correctly.

```java
public List<Session> byPod(String podName, String namespace) {
    List<Session> result = new ArrayList<>();
    for (Session s : sessions.values()) {   // ConcurrentHashMap.values() is weakly consistent
        if (podName.equals(s.getPodName()) && namespace.equals(s.getNamespace())) {
            result.add(s);
        }
    }
    return result;
}
```

**Weakly consistent iteration**: `ConcurrentHashMap.values()` may or may not reflect
concurrent modifications during iteration. This is acceptable here — a session registered
during a drain scan will be caught on the next poll cycle.

---

### 5.3 `DrainHandler.java` — Drain with ScheduledExecutorService

```java
public void handle(String podName, String namespace, long deadlineMs) {
    Instant deadline = Instant.now().plusMillis(deadlineMs);

    // 1. Notify all sessions
    for (Session s : sessions) {
        notifier.notify(s, remaining);
    }

    // 2. Poll every 500ms
    scheduler.scheduleAtFixedRate(() -> {
        List<Session> remaining = registry.byPod(podName, namespace);
        if (remaining.isEmpty()) {
            scheduler.shutdown();   // clean drain
            return;
        }
        if (Instant.now().isAfter(deadline)) {
            remaining.forEach(s -> registry.unregister(s.getId()));
            scheduler.shutdown();   // force close
        }
    }, 500, 500, TimeUnit.MILLISECONDS);
}
```

The `DrainNotifier` is a functional interface — you can pass a lambda:

```java
DrainHandler drainHandler = new DrainHandler(registry, (session, remainingMs) -> {
    // send gRPC GOAWAY or WebSocket close frame here
    grpcChannel.shutdown();
});
```

**Difference from Go**: Go uses a `ticker` inside a `select` loop tied to `ctx.Done()`.
Java uses `ScheduledExecutorService` which is more idiomatic for Java but doesn't have
built-in context cancellation — you'd need to check a flag or use `CompletableFuture` for
that in production.

---

### 5.4 `RetryClassifier.java` — Java Records

```java
public record Rule(String methodPattern, Session.RetryClass retryClass) {}
```

`record` (Java 16+) is a concise immutable data carrier. It auto-generates constructor,
`equals`, `hashCode`, `toString`, and accessors. Equivalent to a Go struct with no methods.

```java
public static List<Rule> defaultRules() {
    return List.of(
        new Rule("*/Watch",   Session.RetryClass.SAFE),
        new Rule("*/Stream*", Session.RetryClass.UNSAFE),
        ...
    );
}
```

`List.of()` returns an immutable list — correct for a static rule set that should never
be modified at runtime.

Pattern matching supports trailing wildcards (`Stream*`) via:

```java
if (suffix.endsWith("*")) {
    String prefix = suffix.substring(0, suffix.length() - 1);
    String methodName = method.substring(method.lastIndexOf('/') + 1);
    return methodName.startsWith(prefix);
}
```

---

### 5.5 `EndpointWatcher.java` — fabric8 Kubernetes Client

```java
client.endpoints().inNamespace(namespace).watch(new Watcher<>() {
    @Override
    public void eventReceived(Action action, Endpoints endpoints) {
        if (action == Action.MODIFIED) {
            reconcile(endpoints, namespace);
        }
    }

    @Override
    public void onClose(WatcherException e) {
        // fabric8 auto-reconnects on transient errors
        if (e != null) log.warning("watcher closed: " + e.getMessage());
    }
});
```

fabric8's `Watcher` is the Java equivalent of `client-go`'s informer event handler.
fabric8 handles reconnection automatically when the watch stream drops.

The `reconcile` method maintains `lastKnownAddresses` (a plain `HashMap`) and diffs it
against the current state on each `MODIFIED` event — same logic as the Go implementation.

**Note**: `lastKnownAddresses` is not thread-safe. fabric8 delivers events on a single
thread per watcher, so this is safe. If you add multiple watchers, you'd need synchronization.

---

### 5.6 `SessionController.java` — Entry Point

```java
try (KubernetesClient client = new KubernetesClientBuilder().build()) {
    EndpointWatcher watcher = new EndpointWatcher(registry, rebinder);
    watcher.watch(client, namespace);

    Thread.sleep(5000);  // demo drain
    drainHandler.handle("myapp-pod-abc", namespace, 30_000);

    Thread.currentThread().join();  // block forever
} catch (InterruptedException e) {
    Thread.currentThread().interrupt();
}
```

`KubernetesClientBuilder().build()` auto-detects configuration:
1. In-cluster: reads `/var/run/secrets/kubernetes.io/serviceaccount/token`
2. Out-of-cluster: reads `~/.kube/config`

The `try-with-resources` ensures the client is closed cleanly on exit.
`Thread.currentThread().join()` blocks the main thread indefinitely — the watcher runs
on fabric8's internal thread pool.

---

## 6. Kubernetes Manifests

### 6.1 `echoserver-deployment.yaml`

Key settings explained:

```yaml
terminationGracePeriodSeconds: 30
```
Kubernetes waits 30 seconds after `SIGTERM` before sending `SIGKILL`. This is the window
your drain handler has to notify sessions and wait for them to close. Set this to match
your longest expected stream duration, not too short (sessions get killed) and not too long
(rollouts take forever).

```yaml
strategy:
  type: RollingUpdate
  rollingUpdate:
    maxUnavailable: 1   # at most 1 pod down at a time
    maxSurge: 1         # at most 1 extra pod during rollout
```
This controls how fast the rollout proceeds. `maxUnavailable: 1` means at least
`replicas - 1` pods are always serving. The endpoint watcher sees IPs disappear one at a time.

```yaml
readinessProbe:
  grpc:
    port: 50051
```
Kubernetes uses the gRPC health protocol (`grpc.health.v1.Health/Check`) to determine when
a pod is ready to receive traffic. The echoserver registers `health.NewServer()` for this.
A pod is only added to the `Endpoints` object when its readiness probe passes.

```yaml
imagePullPolicy: Never
```
Required for kind clusters — images are loaded via `kind load docker-image`, not pulled
from a registry. Remove this for real clusters.

```yaml
env:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
```
The Downward API injects the pod's own name as an environment variable. The echoserver
includes this in every response so you can see which pod handled each request.

---

### 6.2 `controller-deployment.yaml`

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
rules:
  - apiGroups: [""]
    resources: ["endpoints", "pods"]
    verbs: ["get", "list", "watch"]
```

The controller only needs read access to `endpoints` and `pods`. It never writes to the
Kubernetes API. This follows the principle of least privilege.

The `ClusterRoleBinding` grants this role to the `session-controller` ServiceAccount in
the `session-test` namespace. The controller pod runs as this ServiceAccount, and
`rest.InClusterConfig()` automatically uses its mounted token.

---

### 6.3 kind Cluster Configs

`kind-cluster-a.yaml` — 3 nodes (1 control-plane + 2 workers):
```yaml
nodes:
  - role: control-plane
  - role: worker
  - role: worker
```

With 2 workers and `replicas: 2`, each echoserver pod lands on a different worker node.
This tests real cross-node endpoint changes during rollouts.

`kind-cluster-b.yaml` — 2 nodes (1 control-plane + 1 worker):
A simpler cluster for baseline comparison. Both echoserver pods land on the same worker.

---

## 7. Test Infrastructure

### `test/run-tests.sh`

The test script drives three scenarios end-to-end:

**Test 1: Retry Classifier**
Checks controller startup logs for correct classification output:
```
method=/myapp.v1.MyService/Watch retryClass=safe
method=/myapp.v1.MyService/StreamEvents retryClass=unsafe
```

**Test 2: Graceful Drain**
```bash
kubectl delete pod "$pod" --grace-period=10
kubectl rollout status deployment/echoserver --timeout=60s
```
Deletes a pod with a 10-second grace period. Verifies the deployment self-heals (ReplicaSet
creates a replacement pod). In a real test you'd also verify the drain notification was sent
before the TCP connection closed.

**Test 3: Rolling Update / Endpoint Rebinding**
```bash
kubectl set env deployment/echoserver ROLLOUT_ID="$(date +%s)"
kubectl rollout status deployment/echoserver --timeout=120s
```
Triggers a rolling update by changing an environment variable (forces pod recreation).
The endpoint watcher observes the `Endpoints` object change as old pods are removed and
new pods are added.

### What the Tests Validate

| Scenario | What's Checked |
|---|---|
| Classifier | Log output matches expected retry classes |
| Drain | Pod terminates cleanly, deployment recovers |
| Rollout | Rolling update completes, endpoint watcher active |

### What's Not Tested (Production Gaps)

- Actual gRPC stream continuity across drain (would need a streaming client)
- Session rebind correctness (noopRebinder just logs)
- Stale session cleanup (no heartbeat loop in demo)
- Multi-cluster session federation

---

## 8. Running Locally with kind

### Prerequisites

```bash
brew install kind          # local k8s clusters in Docker
docker desktop             # must be running
kubectl                    # already installed
go 1.22+                   # for Go build
```

### Step by Step

**1. Create clusters**
```bash
kind create cluster --config kind-cluster-a.yaml --name session-cluster-a
kind create cluster --config kind-cluster-b.yaml --name session-cluster-b
```

**2. Verify contexts**
```bash
kubectl config get-contexts
# CURRENT   NAME                     CLUSTER
#           kind-session-cluster-a   kind-session-cluster-a
# *         kind-session-cluster-b   kind-session-cluster-b
```

**3. Build Go images**
```bash
cd go
go mod tidy
docker build -t echoserver:test -f cmd/echoserver/Dockerfile .
docker build -t session-controller:test -f Dockerfile .
```

**4. Load images into clusters**
```bash
kind load docker-image echoserver:test --name session-cluster-a
kind load docker-image session-controller:test --name session-cluster-a
kind load docker-image echoserver:test --name session-cluster-b
kind load docker-image session-controller:test --name session-cluster-b
```

**5. Deploy**
```bash
kubectl --context kind-session-cluster-a create namespace session-test
kubectl --context kind-session-cluster-a apply -f k8s/
kubectl --context kind-session-cluster-b create namespace session-test
kubectl --context kind-session-cluster-b apply -f k8s/
```

**6. Watch rollout**
```bash
kubectl --context kind-session-cluster-a -n session-test get pods -w
```

**7. Check controller logs**
```bash
kubectl --context kind-session-cluster-a -n session-test logs deployment/session-controller -f
```

**8. Trigger a drain test**
```bash
POD=$(kubectl --context kind-session-cluster-a -n session-test \
      get pods -l app=echoserver -o jsonpath='{.items[0].metadata.name}')
kubectl --context kind-session-cluster-a -n session-test delete pod $POD --grace-period=10
```

**9. Trigger a rollout**
```bash
kubectl --context kind-session-cluster-a -n session-test \
  set env deployment/echoserver ROLLOUT_ID="$(date +%s)"
kubectl --context kind-session-cluster-a -n session-test \
  rollout status deployment/echoserver
```

**10. Run the full test script**
```bash
./test/run-tests.sh
```

### Cleanup

```bash
kind delete cluster --name session-cluster-a
kind delete cluster --name session-cluster-b
```

### Switching Between Clusters

```bash
kubectl config use-context kind-session-cluster-a
kubectl config use-context kind-session-cluster-b

# Or use --context flag per command (recommended for scripts)
kubectl --context kind-session-cluster-a -n session-test get pods
```

---

## 9. Key Design Decisions

### Why interfaces for DrainNotifier and SessionRebinder?

```go
type DrainNotifier interface {
    Notify(s *api.Session, deadline time.Duration) error
}
```

The controller doesn't know how your sessions are implemented. You might use:
- `grpc.ClientConn.Close()` for gRPC channels
- `websocket.Conn.WriteMessage(websocket.CloseMessage, ...)` for WebSockets
- A message queue notification for async clients

By depending on an interface, the controller is transport-agnostic. The `noopNotifier`
in `main.go` is a placeholder — swap it with your real implementation.

### Why poll instead of event-driven drain?

The drain handler polls every 500ms rather than waiting for sessions to call `Unregister`.
This is intentional:

1. Sessions might crash without calling `Unregister`
2. The deadline must be enforced regardless of session behavior
3. Polling is simple and predictable; event-driven would require sessions to signal back

The 500ms interval is a tradeoff — fast enough to react quickly, slow enough not to hammer
the tracker's mutex.

### Why `RetryClassUnsafe` as the default?

When no rule matches, `Classify` returns `UNSAFE`. This is the conservative choice.
An unclassified stream is assumed to be stateful and not safe to retry. You must explicitly
opt in to `SAFE` or `CONDITIONAL` by adding a rule.

The alternative (default to `SAFE`) would silently allow retries on streams that might
cause data corruption. Fail safe, not fail open.

### Why separate Go and Java implementations?

Real organizations often have polyglot environments. A Java service team and a Go platform
team might both need this controller. Having both implementations:

1. Shows the same concepts expressed in each language's idioms
2. Lets you choose based on your existing stack
3. Demonstrates that the design is language-agnostic

The interfaces (`DrainNotifier`, `SessionRebinder`) are the same in both — they define
the contract regardless of language.

### Why kind over Docker Desktop's built-in Kubernetes?

Docker Desktop ships with a single-node Kubernetes cluster. kind gives you:
- Multi-node clusters (test real cross-node scheduling)
- Multiple independent clusters (test multi-cluster scenarios)
- Reproducible cluster configs in YAML
- Easy teardown and recreation

The tradeoff is that kind clusters run as Docker containers, so they share the Docker
Desktop VM's resources. For load testing you'd want real VMs or a cloud provider.

---

## 10. Extension Points

### Replace noopNotifier with real gRPC GOAWAY

```go
// Go
type grpcNotifier struct {
    conns map[string]*grpc.ClientConn  // sessionID -> connection
}

func (n *grpcNotifier) Notify(s *api.Session, deadline time.Duration) error {
    conn, ok := n.conns[s.ID]
    if !ok { return nil }
    // GracefulStop on the server side sends GOAWAY to clients
    // On the client side, close the connection to trigger reconnect
    return conn.Close()
}
```

```java
// Java
DrainHandler drainHandler = new DrainHandler(registry, (session, remainingMs) -> {
    ManagedChannel channel = channelRegistry.get(session.getId());
    if (channel != null) {
        channel.shutdown();
        channel.awaitTermination(remainingMs, TimeUnit.MILLISECONDS);
    }
});
```

### Add Heartbeat Loop

```go
// In your gRPC interceptor or WebSocket read loop:
go func() {
    ticker := time.NewTicker(30 * time.Second)
    for range ticker.C {
        tracker.Heartbeat(sessionID)
    }
}()

// Periodic stale cleanup:
go func() {
    ticker := time.NewTicker(60 * time.Second)
    for range ticker.C {
        for _, s := range tracker.Stale(90 * time.Second) {
            log.Printf("cleaning stale session=%s", s.ID)
            tracker.Unregister(s.ID)
        }
    }
}()
```

### Migrate to EndpointSlice (k8s 1.21+)

```go
// Replace in endpoint/watcher.go:
informer := factory.Discovery().V1().EndpointSlices().Informer()

// EndpointSlice has a different structure:
informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
    UpdateFunc: func(old, new interface{}) {
        oldSlice := old.(*discoveryv1.EndpointSlice)
        newSlice := new.(*discoveryv1.EndpointSlice)
        w.reconcileSlice(oldSlice, newSlice)
    },
})
```

### Add a Kubernetes Custom Resource (CRD)

Instead of an in-memory session map, persist sessions as CRDs:

```yaml
apiVersion: session.io/v1
kind: SessionBinding
metadata:
  name: demo-session-1
spec:
  sessionId: demo-session-1
  type: grpc
  podName: myapp-pod-abc
  namespace: session-test
  endpoint: 10.0.0.1:50051
  retryClass: unsafe
```

This makes sessions visible via `kubectl get sessionbindings` and survives controller restarts.

### Wire a Real Kubernetes Event Source for Drain

Instead of the demo 5-second timer, watch for pod deletion events:

```go
podInformer := factory.Core().V1().Pods().Informer()
podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
    UpdateFunc: func(old, new interface{}) {
        newPod := new.(*corev1.Pod)
        if newPod.DeletionTimestamp != nil {
            // Pod is being deleted — trigger drain
            deadline := 30 * time.Second
            if newPod.Spec.TerminationGracePeriodSeconds != nil {
                deadline = time.Duration(*newPod.Spec.TerminationGracePeriodSeconds) * time.Second
            }
            drainHandler.Handle(ctx, api.DrainEvent{
                PodName:   newPod.Name,
                Namespace: newPod.Namespace,
                Deadline:  deadline,
            })
        }
    },
})
```

`DeletionTimestamp` is set by Kubernetes the moment a pod enters the terminating state —
before `SIGTERM` is sent. This gives you maximum lead time for drain.

---

## 11. Further Reading

### gRPC

- [gRPC Core Concepts](https://grpc.io/docs/what-is-grpc/core-concepts/) — streams, metadata, deadlines
- [gRPC Health Checking Protocol](https://grpc.io/docs/guides/health-checking/) — how readiness probes work
- [gRPC Keepalive](https://grpc.io/docs/guides/keepalive/) — detecting dead connections
- [GOAWAY frame](https://httpwg.org/specs/rfc7540.html#GOAWAY) — HTTP/2 graceful shutdown signal

### Kubernetes

- [Pod Lifecycle](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/) — SIGTERM, grace periods, termination
- [Endpoints vs EndpointSlice](https://kubernetes.io/docs/concepts/services-networking/endpoint-slices/) — migration guide
- [client-go Informers](https://pkg.go.dev/k8s.io/client-go/informers) — shared informer factory
- [Downward API](https://kubernetes.io/docs/concepts/workloads/pods/downward-api/) — injecting pod metadata as env vars
- [RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/) — least-privilege service accounts

### Go Patterns

- [sync.RWMutex](https://pkg.go.dev/sync#RWMutex) — reader/writer mutex
- [signal.NotifyContext](https://pkg.go.dev/os/signal#NotifyContext) — graceful shutdown
- [context propagation](https://pkg.go.dev/context) — cancellation across goroutines

### Java Patterns

- [ConcurrentHashMap](https://docs.oracle.com/en/java/docs/api/java.base/java/util/concurrent/ConcurrentHashMap.html) — thread-safe map
- [ScheduledExecutorService](https://docs.oracle.com/en/java/docs/api/java.base/java/util/concurrent/ScheduledExecutorService.html) — periodic tasks
- [Java Records](https://openjdk.org/jeps/395) — immutable data carriers
- [fabric8 Kubernetes Client](https://github.com/fabric8io/kubernetes-client) — Java k8s client

### Session Layer Theory

- [OSI Session Layer (Layer 5)](https://en.wikipedia.org/wiki/Session_layer) — what this project implements
- [Idempotency Keys](https://stripe.com/docs/api/idempotent_requests) — the CONDITIONAL retry pattern
- [Two Generals Problem](https://en.wikipedia.org/wiki/Two_generals%27_problem) — why UNSAFE streams can't safely retry

---

*Built and tested on kind 0.31.0 / Kubernetes 1.35.0 / Docker Desktop 28.0.4 / macOS*
