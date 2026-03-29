package endpoint

import (
	"context"
	"log"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/grpc-ws-session-k8s/internal/session"
	"github.com/grpc-ws-session-k8s/pkg/api"
)

// Watcher monitors Kubernetes Endpoints objects and triggers session rebinding
// when pod IPs change during rollouts.
type Watcher struct {
	tracker  *session.Tracker
	rebinder SessionRebinder
}

// SessionRebinder moves a session from an old endpoint to a new one
type SessionRebinder interface {
	Rebind(s *api.Session, change api.EndpointChange) error
}

func NewWatcher(tracker *session.Tracker, rebinder SessionRebinder) *Watcher {
	return &Watcher{tracker: tracker, rebinder: rebinder}
}

// Run starts the Endpoints informer and processes changes until ctx is cancelled
func (w *Watcher) Run(ctx context.Context, client kubernetes.Interface, namespace string) {
	factory := informers.NewSharedInformerFactoryWithOptions(
		client,
		0,
		informers.WithNamespace(namespace),
	)
	informer := factory.Core().V1().Endpoints().Informer()

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(old, new interface{}) {
			oldEp := old.(*corev1.Endpoints)
			newEp := new.(*corev1.Endpoints)
			w.reconcile(oldEp, newEp)
		},
	})

	factory.Start(ctx.Done())
	cache.WaitForCacheSync(ctx.Done(), informer.HasSynced)
	<-ctx.Done()
}

// reconcile diffs old vs new endpoint subsets and triggers rebind for affected sessions
func (w *Watcher) reconcile(old, new *corev1.Endpoints) {
	oldAddrs := endpointAddresses(old)
	newAddrs := endpointAddresses(new)

	for addr := range oldAddrs {
		if _, stillPresent := newAddrs[addr]; !stillPresent {
			// This address was removed — find sessions pinned to it
			sessions := w.tracker.ByPod(oldAddrs[addr], new.Namespace)
			for _, s := range sessions {
				// Pick any new address as rebind target (real impl would use load balancing)
				for newAddr := range newAddrs {
					change := api.EndpointChange{
						OldEndpoint: addr,
						NewEndpoint: newAddr,
						PodName:     s.PodName,
						Namespace:   s.Namespace,
					}
					log.Printf("[endpoint] rebinding session=%s %s -> %s", s.ID, addr, newAddr)
					if err := w.rebinder.Rebind(s, change); err != nil {
						log.Printf("[endpoint] rebind error session=%s err=%v", s.ID, err)
					}
					break
				}
			}
		}
	}
}

// endpointAddresses returns a map of address -> podName from an Endpoints object
func endpointAddresses(ep *corev1.Endpoints) map[string]string {
	result := make(map[string]string)
	for _, subset := range ep.Subsets {
		for _, addr := range subset.Addresses {
			podName := ""
			if addr.TargetRef != nil {
				podName = addr.TargetRef.Name
			}
			result[addr.IP] = podName
		}
	}
	return result
}
