package api

import "time"

// SessionType distinguishes gRPC streams from WebSocket connections
type SessionType string

const (
	SessionTypeGRPC      SessionType = "grpc"
	SessionTypeWebSocket SessionType = "websocket"
)

// RetryClass classifies whether a stream RPC is safe to retry
type RetryClass string

const (
	RetryClassSafe        RetryClass = "safe"        // idempotent, no side effects
	RetryClassUnsafe      RetryClass = "unsafe"       // stateful, must rebind
	RetryClassConditional RetryClass = "conditional"  // retry with dedup token
)

// Session represents a long-lived connection tracked by the controller
type Session struct {
	ID          string
	Type        SessionType
	PodName     string
	Namespace   string
	Endpoint    string
	StartedAt   time.Time
	LastSeen    time.Time
	RetryClass  RetryClass
	Metadata    map[string]string
}

// DrainEvent signals a pod is being terminated
type DrainEvent struct {
	PodName   string
	Namespace string
	Deadline  time.Duration
}

// EndpointChange tracks pod endpoint mutations during rollouts
type EndpointChange struct {
	OldEndpoint string
	NewEndpoint string
	PodName     string
	Namespace   string
}
