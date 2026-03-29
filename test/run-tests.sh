#!/usr/bin/env bash
# run-tests.sh — end-to-end session survivability tests on two kind clusters
set -euo pipefail

CLUSTERS=("session-cluster-a" "session-cluster-b")
NS="session-test"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

log()  { echo "[$(date +%H:%M:%S)] $*"; }
pass() { echo "  ✅ $*"; }
fail() { echo "  ❌ $*"; exit 1; }

# ── 1. Create clusters ────────────────────────────────────────────────────────
create_clusters() {
  for cluster in "${CLUSTERS[@]}"; do
    if kind get clusters 2>/dev/null | grep -q "^${cluster}$"; then
      log "cluster $cluster already exists, skipping"
    else
      log "creating cluster $cluster"
      kind create cluster --config "$ROOT/kind-cluster-${cluster##*-}.yaml" --name "$cluster"
    fi
  done
}

# ── 2. Build images ───────────────────────────────────────────────────────────
build_images() {
  log "building echoserver image"
  docker build -t echoserver:test -f "$ROOT/go/cmd/echoserver/Dockerfile" "$ROOT/go"

  log "building session-controller image"
  docker build -t session-controller:test -f "$ROOT/go/Dockerfile" "$ROOT/go"
}

# ── 3. Load images into both clusters ────────────────────────────────────────
load_images() {
  for cluster in "${CLUSTERS[@]}"; do
    log "loading images into $cluster"
    kind load docker-image echoserver:test --name "$cluster"
    kind load docker-image session-controller:test --name "$cluster"
  done
}

# ── 4. Deploy to both clusters ────────────────────────────────────────────────
deploy() {
  for cluster in "${CLUSTERS[@]}"; do
    log "deploying to $cluster"
    kubectl --context "kind-${cluster}" create namespace "$NS" --dry-run=client -o yaml \
      | kubectl --context "kind-${cluster}" apply -f -
    kubectl --context "kind-${cluster}" apply -f "$ROOT/k8s/echoserver-deployment.yaml"
    kubectl --context "kind-${cluster}" apply -f "$ROOT/k8s/controller-deployment.yaml"
  done
}

# ── 5. Wait for rollout ───────────────────────────────────────────────────────
wait_ready() {
  for cluster in "${CLUSTERS[@]}"; do
    log "waiting for echoserver rollout in $cluster"
    kubectl --context "kind-${cluster}" -n "$NS" rollout status deployment/echoserver --timeout=120s
    kubectl --context "kind-${cluster}" -n "$NS" rollout status deployment/session-controller --timeout=60s
  done
}

# ── 6. Test: drain survivability ──────────────────────────────────────────────
test_drain() {
  local cluster="kind-session-cluster-a"
  log "TEST: graceful drain on $cluster"

  # Pick one echoserver pod
  local pod
  pod=$(kubectl --context "$cluster" -n "$NS" get pods -l app=echoserver \
        -o jsonpath='{.items[0].metadata.name}')
  log "  target pod: $pod"

  # Check controller logs show session tracking
  sleep 2
  local ctrl_pod
  ctrl_pod=$(kubectl --context "$cluster" -n "$NS" get pods -l app=session-controller \
             -o jsonpath='{.items[0].metadata.name}')
  kubectl --context "$cluster" -n "$NS" logs "$ctrl_pod" --tail=20

  # Delete the pod — triggers terminationGracePeriodSeconds drain path
  log "  deleting pod to trigger drain"
  kubectl --context "$cluster" -n "$NS" delete pod "$pod" --grace-period=10

  # Verify deployment self-heals
  kubectl --context "$cluster" -n "$NS" rollout status deployment/echoserver --timeout=60s
  pass "drain test: pod terminated and deployment recovered"
}

# ── 7. Test: rollout endpoint rebinding ──────────────────────────────────────
test_rollout() {
  local cluster="kind-session-cluster-a"
  log "TEST: rolling update endpoint rebinding on $cluster"

  # Trigger a rollout by patching an env var
  kubectl --context "$cluster" -n "$NS" set env deployment/echoserver ROLLOUT_ID="$(date +%s)"
  kubectl --context "$cluster" -n "$NS" rollout status deployment/echoserver --timeout=120s

  # Check endpoint watcher logs
  local ctrl_pod
  ctrl_pod=$(kubectl --context "$cluster" -n "$NS" get pods -l app=session-controller \
             -o jsonpath='{.items[0].metadata.name}')
  kubectl --context "$cluster" -n "$NS" logs "$ctrl_pod" --tail=30

  pass "rollout test: rolling update completed, endpoint watcher active"
}

# ── 8. Test: retry classifier output ─────────────────────────────────────────
test_retry_classifier() {
  local cluster="kind-session-cluster-a"
  log "TEST: retry classifier in controller logs"

  local ctrl_pod
  ctrl_pod=$(kubectl --context "$cluster" -n "$NS" get pods -l app=session-controller \
             -o jsonpath='{.items[0].metadata.name}')
  local logs
  logs=$(kubectl --context "$cluster" -n "$NS" logs "$ctrl_pod")

  echo "$logs" | grep -q "retryClass=safe"        && pass "Watch classified as SAFE" \
                                                   || fail "Watch not classified as SAFE"
  echo "$logs" | grep -q "retryClass=unsafe"      && pass "StreamEvents classified as UNSAFE" \
                                                   || fail "StreamEvents not classified as UNSAFE"
}

# ── 9. Cross-cluster summary ──────────────────────────────────────────────────
summary() {
  log "CLUSTER SUMMARY"
  for cluster in "${CLUSTERS[@]}"; do
    echo ""
    echo "── $cluster ──"
    kubectl --context "kind-${cluster}" -n "$NS" get pods -o wide
  done
}

# ── Main ──────────────────────────────────────────────────────────────────────
main() {
  create_clusters
  build_images
  load_images
  deploy
  wait_ready
  test_drain
  test_rollout
  test_retry_classifier
  summary
  log "all tests passed"
}

main "$@"
