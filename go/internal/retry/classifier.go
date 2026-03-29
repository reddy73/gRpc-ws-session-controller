package retry

import (
	"strings"

	"github.com/grpc-ws-session-k8s/pkg/api"
)

// Rule defines classification criteria for a gRPC method or WS route
type Rule struct {
	MethodPattern string
	Class         api.RetryClass
}

// Classifier assigns retry safety to streaming RPCs based on method rules
type Classifier struct {
	rules []Rule
}

func NewClassifier(rules []Rule) *Classifier {
	return &Classifier{rules: rules}
}

// Classify returns the retry class for a given gRPC method or WS path.
// Falls back to RetryClassUnsafe if no rule matches (safe default for streams).
func (c *Classifier) Classify(method string) api.RetryClass {
	for _, r := range c.rules {
		if matchPattern(r.MethodPattern, method) {
			return r.Class
		}
	}
	return api.RetryClassUnsafe
}

// DefaultRules provides sensible defaults for common gRPC patterns
func DefaultRules() []Rule {
	return []Rule{
		// Health checks and reads are safe to retry
		{MethodPattern: "*/Watch", Class: api.RetryClassSafe},
		{MethodPattern: "*/List", Class: api.RetryClassSafe},
		{MethodPattern: "*/Get", Class: api.RetryClassSafe},
		// Mutations need dedup tokens
		{MethodPattern: "*/Create", Class: api.RetryClassConditional},
		{MethodPattern: "*/Update", Class: api.RetryClassConditional},
		// Streaming writes are unsafe by default
		{MethodPattern: "*/Stream*", Class: api.RetryClassUnsafe},
	}
}

func matchPattern(pattern, method string) bool {
	if strings.HasPrefix(pattern, "*/") {
		suffix := pattern[2:]
		return strings.HasSuffix(method, "/"+suffix) || strings.HasSuffix(method, suffix)
	}
	return pattern == method
}
