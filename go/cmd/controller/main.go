package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/grpc-ws-session-k8s/internal/drain"
	"github.com/grpc-ws-session-k8s/internal/endpoint"
	"github.com/grpc-ws-session-k8s/internal/retry"
	"github.com/grpc-ws-session-k8s/internal/session"
	"github.com/grpc-ws-session-k8s/pkg/api"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Build k8s client from in-cluster config
	cfg, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("k8s config: %v", err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("k8s client: %v", err)
	}

	namespace := envOrDefault("NAMESPACE", "default")

	// Wire up components
	tracker := session.NewTracker()
	classifier := retry.NewClassifier(retry.DefaultRules())
	drainHandler := drain.NewHandler(tracker, &noopNotifier{})
	epWatcher := endpoint.NewWatcher(tracker, &noopRebinder{})

	// Demonstrate classifier
	for _, method := range []string{"/myapp.v1.MyService/Watch", "/myapp.v1.MyService/StreamEvents"} {
		log.Printf("method=%s retryClass=%s", method, classifier.Classify(method))
	}

	// Register a demo session
	tracker.Register(&api.Session{
		ID:        "demo-session-1",
		Type:      api.SessionTypeGRPC,
		PodName:   "myapp-pod-abc",
		Namespace: namespace,
		Endpoint:  "10.0.0.1:50051",
	})

	// Simulate a drain event after 5s (for demo purposes)
	go func() {
		time.Sleep(5 * time.Second)
		drainHandler.Handle(ctx, api.DrainEvent{
			PodName:   "myapp-pod-abc",
			Namespace: namespace,
			Deadline:  30 * time.Second,
		})
	}()

	// Start endpoint watcher
	go epWatcher.Run(ctx, client, namespace)

	log.Printf("session controller running namespace=%s", namespace)
	<-ctx.Done()
	log.Println("shutting down")
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// noopNotifier is a placeholder — replace with real gRPC GOAWAY / WS close logic
type noopNotifier struct{}

func (n *noopNotifier) Notify(s *api.Session, deadline time.Duration) error {
	log.Printf("[notify] session=%s type=%s deadline=%s", s.ID, s.Type, deadline)
	return nil
}

// noopRebinder is a placeholder — replace with real client reconnect logic
type noopRebinder struct{}

func (r *noopRebinder) Rebind(s *api.Session, change api.EndpointChange) error {
	log.Printf("[rebind] session=%s %s -> %s", s.ID, change.OldEndpoint, change.NewEndpoint)
	return nil
}
