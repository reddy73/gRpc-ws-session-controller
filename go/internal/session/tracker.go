package session

import (
	"sync"
	"time"

	"github.com/grpc-ws-session-k8s/pkg/api"
)

// Tracker maintains the registry of all active long-lived sessions
type Tracker struct {
	mu       sync.RWMutex
	sessions map[string]*api.Session
}

func NewTracker() *Tracker {
	return &Tracker{
		sessions: make(map[string]*api.Session),
	}
}

func (t *Tracker) Register(s *api.Session) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s.StartedAt = time.Now()
	s.LastSeen = time.Now()
	t.sessions[s.ID] = s
}

func (t *Tracker) Heartbeat(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if s, ok := t.sessions[id]; ok {
		s.LastSeen = time.Now()
	}
}

func (t *Tracker) Unregister(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.sessions, id)
}

// ByPod returns all sessions pinned to a specific pod
func (t *Tracker) ByPod(podName, namespace string) []*api.Session {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var result []*api.Session
	for _, s := range t.sessions {
		if s.PodName == podName && s.Namespace == namespace {
			result = append(result, s)
		}
	}
	return result
}

// Stale returns sessions that haven't sent a heartbeat within ttl
func (t *Tracker) Stale(ttl time.Duration) []*api.Session {
	t.mu.RLock()
	defer t.mu.RUnlock()
	cutoff := time.Now().Add(-ttl)
	var result []*api.Session
	for _, s := range t.sessions {
		if s.LastSeen.Before(cutoff) {
			result = append(result, s)
		}
	}
	return result
}
