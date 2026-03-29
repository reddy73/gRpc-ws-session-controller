package drain

import (
	"context"
	"log"
	"time"

	"github.com/grpc-ws-session-k8s/internal/session"
	"github.com/grpc-ws-session-k8s/pkg/api"
)

// Handler orchestrates graceful drain of sessions before pod termination
type Handler struct {
	tracker  *session.Tracker
	notifier DrainNotifier
}

// DrainNotifier sends drain signals to active sessions (gRPC GOAWAY / WS close frame)
type DrainNotifier interface {
	Notify(s *api.Session, deadline time.Duration) error
}

func NewHandler(tracker *session.Tracker, notifier DrainNotifier) *Handler {
	return &Handler{tracker: tracker, notifier: notifier}
}

// Handle processes a DrainEvent: notifies sessions and waits for graceful close
// within the pod's termination deadline.
func (h *Handler) Handle(ctx context.Context, event api.DrainEvent) {
	sessions := h.tracker.ByPod(event.PodName, event.Namespace)
	if len(sessions) == 0 {
		return
	}

	log.Printf("[drain] pod=%s/%s sessions=%d deadline=%s",
		event.Namespace, event.PodName, len(sessions), event.Deadline)

	deadline := time.Now().Add(event.Deadline)
	for _, s := range sessions {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			log.Printf("[drain] deadline exceeded, forcing close session=%s", s.ID)
			h.tracker.Unregister(s.ID)
			continue
		}
		if err := h.notifier.Notify(s, remaining); err != nil {
			log.Printf("[drain] notify error session=%s err=%v", s.ID, err)
		}
	}

	// Wait for sessions to drain or deadline to expire
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			remaining := h.tracker.ByPod(event.PodName, event.Namespace)
			if len(remaining) == 0 {
				log.Printf("[drain] all sessions drained pod=%s/%s", event.Namespace, event.PodName)
				return
			}
			if time.Now().After(deadline) {
				log.Printf("[drain] deadline hit, %d sessions force-closed", len(remaining))
				for _, s := range remaining {
					h.tracker.Unregister(s.ID)
				}
				return
			}
		}
	}
}
