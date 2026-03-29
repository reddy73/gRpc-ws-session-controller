# Build, Run & Verify Runbook

Everything here is copy-paste ready. Run from the repo root unless told otherwise.

---

## Prerequisites

| Tool | Min Version | Install |
|---|---|---|
| Docker Desktop | 4.x | https://www.docker.com/products/docker-desktop |
| kubectl | 1.29+ | `brew install kubectl` |
| kind | 0.31+ | `brew install kind` |
| Go | 1.22+ | `brew install go` |
| Java (optional) | 21+ | `brew install openjdk@21` |
| Maven (optional) | 3.9+ | `brew install maven` |

Verify everything is present:

```bash
docker info --format '{{.ServerVersion}}'
kubectl version --client --short
kind version
go version
```

---

## Part 1 — Build

### 1a. Go — resolve dependencies and compile

```bash
cd grpc-ws-session-k8s/go
go mod tidy
go build ./...
```

Expected output: no errors, no output. A clean `go build ./...` is silent.

### 1b. Go — build Docker images

Run from `grpc-ws-session-k8s/go`:

```bash
# gRPC echo server (used as the test workload)
docker build -t echoserver:test -f cmd/echoserver/Dockerfile .

# Session survivability controller
docker build -t session-controller:test -f Dockerfile .
```

Verify images exist:

```bash
docker images | grep -E 'echoserver|session-controller'
```

Expected:
```
echoserver          test    <id>   ...
session-controller  test    <id>   ...
```

### 1c. Java — compile (optional)

```bash
cd grpc-ws-session-k8s/java
mvn compile -q
```

Expected: `BUILD SUCCESS`. The Java implementation shares the same concepts as Go —
run either one, not both, in the cluster.

---

## Part 2 — Cluster Setup

### 2a. Create two kind clusters

```bash
kind create cluster --config grpc-ws-session-k8s/kind-cluster-a.yaml --name session-cluster-a
kind create cluster --config grpc-ws-session-k8s/kind-cluster-b.yaml --name session-cluster-b
```

- `session-cluster-a` — 1 control-plane + 2 workers (tests cross-node scheduling)
- `session-cluster-b` — 1 control-plane + 1 worker (baseline)

Each takes ~60s. Verify:

```bash
kind get clusters
# session-cluster-a
# session-cluster-b

kubectl config get-contexts
# kind-session-cluster-a
# kind-session-cluster-b
```

### 2b. Load images into both clusters

kind clusters are isolated Docker networks — they cannot pull from your local Docker daemon
automatically. You must load images explicitly:

```bash
kind load docker-image echoserver:test        --name session-cluster-a
kind load docker-image session-controller:test --name session-cluster-a

kind load docker-image echoserver:test        --name session-cluster-b
kind load docker-image session-controller:test --name session-cluster-b
```

Each line prints which nodes received the image. Verify on cluster-a:

```bash
docker exec session-cluster-a-worker crictl images 2>/dev/null | grep echoserver
```

---

## Part 3 — Deploy

### 3a. Deploy to cluster-a

```bash
kubectl --context kind-session-cluster-a \
  create namespace session-test --dry-run=client -o yaml \
  | kubectl --context kind-session-cluster-a apply -f -

kubectl --context kind-session-cluster-a apply -f grpc-ws-session-k8s/k8s/echoserver-deployment.yaml
kubectl --context kind-session-cluster-a apply -f grpc-ws-session-k8s/k8s/controller-deployment.yaml
```

### 3b. Deploy to cluster-b

```bash
kubectl --context kind-session-cluster-b \
  create namespace session-test --dry-run=client -o yaml \
  | kubectl --context kind-session-cluster-b apply -f -

kubectl --context kind-session-cluster-b apply -f grpc-ws-session-k8s/k8s/echoserver-deployment.yaml
kubectl --context kind-session-cluster-b apply -f grpc-ws-session-k8s/k8s/controller-deployment.yaml
```

### 3c. Wait for rollout

```bash
kubectl --context kind-session-cluster-a -n session-test \
  rollout status deployment/echoserver --timeout=120s

kubectl --context kind-session-cluster-a -n session-test \
  rollout status deployment/session-controller --timeout=60s

kubectl --context kind-session-cluster-b -n session-test \
  rollout status deployment/echoserver --timeout=120s

kubectl --context kind-session-cluster-b -n session-test \
  rollout status deployment/session-controller --timeout=60s
```

Expected for each:
```
deployment "echoserver" successfully rolled out
deployment "session-controller" successfully rolled out
```

---

## Part 4 — Verify

### Verify 1: Pod placement

```bash
kubectl --context kind-session-cluster-a -n session-test get pods -o wide
```

Expected — 2 echoserver pods on **different** worker nodes, 1 controller:
```
NAME                                  READY   STATUS    NODE
echoserver-xxx-yyy                    1/1     Running   session-cluster-a-worker
echoserver-xxx-zzz                    1/1     Running   session-cluster-a-worker2
session-controller-xxx-aaa            1/1     Running   session-cluster-a-worker
```

```bash
kubectl --context kind-session-cluster-b -n session-test get pods -o wide
```

Expected — both echoserver pods on the single worker:
```
NAME                                  READY   STATUS    NODE
echoserver-xxx-yyy                    1/1     Running   session-cluster-b-worker
echoserver-xxx-zzz                    1/1     Running   session-cluster-b-worker
session-controller-xxx-bbb            1/1     Running   session-cluster-b-worker
```

---

### Verify 2: Retry classifier

```bash
kubectl --context kind-session-cluster-a -n session-test \
  logs deployment/session-controller | head -5
```

Expected — first two lines must show correct classification:
```
method=/myapp.v1.MyService/Watch retryClass=safe
method=/myapp.v1.MyService/StreamEvents retryClass=unsafe
session controller running namespace=session-test
```

`Watch` → `safe` (idempotent read, reconnect freely)
`StreamEvents` → `unsafe` (stateful stream, must not blindly retry)

---

### Verify 3: Drain on pod deletion

Pick a pod and delete it with a short grace period:

```bash
POD=$(kubectl --context kind-session-cluster-a -n session-test \
      get pods -l app=echoserver -o jsonpath='{.items[0].metadata.name}')

echo "Deleting: $POD"

kubectl --context kind-session-cluster-a -n session-test \
  delete pod "$POD" --grace-period=10
```

Immediately watch the controller logs in a second terminal:

```bash
kubectl --context kind-session-cluster-a -n session-test \
  logs deployment/session-controller -f
```

Expected log lines (within ~5s of deletion):
```
[drain] pod=session-test/myapp-pod-abc sessions=1 deadline=30s
[notify] session=demo-session-1 type=grpc deadline=29.999s
[drain] deadline hit, 1 sessions force-closed
```

Verify the deployment self-healed:

```bash
kubectl --context kind-session-cluster-a -n session-test \
  rollout status deployment/echoserver --timeout=60s
# deployment "echoserver" successfully rolled out

kubectl --context kind-session-cluster-a -n session-test get pods -o wide
# new pod appears on the same or different worker
```

---

### Verify 4: Rolling update + endpoint rebinding

Trigger a rolling update by patching an env var (forces pod recreation):

```bash
kubectl --context kind-session-cluster-a -n session-test \
  set env deployment/echoserver ROLLOUT_ID="$(date +%s)"
```

Watch the rollout in real time:

```bash
kubectl --context kind-session-cluster-a -n session-test \
  rollout status deployment/echoserver --timeout=120s
```

Watch endpoint changes as pods cycle:

```bash
kubectl --context kind-session-cluster-a -n session-test \
  get endpoints echoserver -w
```

Expected — you'll see the IP list change as old pods are removed and new ones become ready:
```
NAME         ENDPOINTS                         AGE
echoserver   10.244.1.4:50051,10.244.2.4:50051  5m
echoserver   10.244.1.4:50051                   5m   ← one pod terminating
echoserver   10.244.1.4:50051,10.244.2.5:50051  5m   ← new pod ready
echoserver   10.244.2.5:50051                   5m   ← second pod terminating
echoserver   10.244.1.5:50051,10.244.2.5:50051  5m   ← rollout complete
```

Check controller logs for endpoint watcher activity:

```bash
kubectl --context kind-session-cluster-a -n session-test \
  logs deployment/session-controller --tail=20
```

---

### Verify 5: RBAC is correct

Confirm the controller can read endpoints and pods but cannot write:

```bash
# Should succeed
kubectl --context kind-session-cluster-a \
  auth can-i list endpoints \
  --as=system:serviceaccount:session-test:session-controller \
  -n session-test
# yes

kubectl --context kind-session-cluster-a \
  auth can-i watch pods \
  --as=system:serviceaccount:session-test:session-controller \
  -n session-test
# yes

# Should be denied
kubectl --context kind-session-cluster-a \
  auth can-i delete pods \
  --as=system:serviceaccount:session-test:session-controller \
  -n session-test
# no
```

---

### Verify 6: gRPC health check

Port-forward to an echoserver pod and hit the health endpoint:

```bash
kubectl --context kind-session-cluster-a -n session-test \
  port-forward deployment/echoserver 50051:50051 &

# Install grpc_health_probe if not present
brew install grpc-health-probe 2>/dev/null || \
  go install github.com/grpc-ecosystem/grpc-health-probe@latest

grpc_health_probe -addr=localhost:50051
# status: SERVING

kill %1   # stop port-forward
```

---

### Verify 7: Run the full automated test script

```bash
chmod +x grpc-ws-session-k8s/test/run-tests.sh
./grpc-ws-session-k8s/test/run-tests.sh
```

Expected final output:
```
[HH:MM:SS] all tests passed
```

---

## Part 5 — Observe Live Behavior

### Watch both clusters simultaneously

Open two terminals:

```bash
# Terminal 1 — cluster-a
watch -n2 'kubectl --context kind-session-cluster-a -n session-test get pods -o wide'

# Terminal 2 — cluster-b
watch -n2 'kubectl --context kind-session-cluster-b -n session-test get pods -o wide'
```

### Stream controller logs from both clusters

```bash
# Terminal 1
kubectl --context kind-session-cluster-a -n session-test \
  logs deployment/session-controller -f

# Terminal 2
kubectl --context kind-session-cluster-b -n session-test \
  logs deployment/session-controller -f
```

### Describe a pod to see events

```bash
kubectl --context kind-session-cluster-a -n session-test \
  describe pod -l app=echoserver | tail -20
```

Look for `Killing` and `Started` events that show the drain/replace cycle.

---

## Part 6 — Cleanup

### Remove deployments only (keep clusters)

```bash
kubectl --context kind-session-cluster-a delete namespace session-test
kubectl --context kind-session-cluster-b delete namespace session-test
```

### Tear down clusters entirely

```bash
kind delete cluster --name session-cluster-a
kind delete cluster --name session-cluster-b
```

Verify gone:

```bash
kind get clusters   # should be empty
kubectl config get-contexts | grep kind-session   # should be empty
```

---

## Troubleshooting

**Pod stuck in `ImagePullBackOff`**
```bash
# imagePullPolicy: Never requires the image to be loaded via kind load
kind load docker-image echoserver:test --name session-cluster-a
kind load docker-image session-controller:test --name session-cluster-a
```

**Controller crashes with `k8s config: ...`**
```bash
# The controller uses InClusterConfig — it must run inside the cluster
# If testing locally, switch to kubeconfig:
# cfg, err := clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
```

**`rollout status` times out**
```bash
# Check pod events for the real error
kubectl --context kind-session-cluster-a -n session-test describe pods
```

**`grpc_health_probe` not found**
```bash
go install github.com/grpc-ecosystem/grpc-health-probe@latest
export PATH=$PATH:$(go env GOPATH)/bin
```

**kind cluster creation fails**
```bash
# Ensure Docker Desktop is running and has enough resources
# Recommended: 4 CPU, 8GB RAM in Docker Desktop settings
docker info | grep -E 'CPUs|Total Memory'
```
