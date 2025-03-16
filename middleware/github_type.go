package middleware

import (
	"encoding/json"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"
)

// GitHub event type constants
const (
	EventTypeDeployment = "deployment"
	EventTypeUnknown    = "unknown"
)

// GitHubEventTypeHeader is the header key to store event type
const GitHubEventTypeHeader = "X-GitHub-Event-Type"

// GitHubEventType middleware determines the type of GitHub event
func GitHubEventType() Middleware {
	return func(next MessageHandler) MessageHandler {
		return func(record *kgo.Record) error {
			// Determine if this is a deployment event
			eventType := determineGitHubEventType(record.Value)
			
			// Add the event type as a header to the record for downstream middlewares
			record.Headers = append(record.Headers, kgo.RecordHeader{
				Key:   GitHubEventTypeHeader,
				Value: []byte(eventType),
			})
			
			if eventType == EventTypeDeployment {
				log.Printf("Detected GitHub deployment event")
			} else {
				log.Printf("Skipping non-deployment event")
			}
			
			// Continue the middleware chain
			return next(record)
		}
	}
}

// determineGitHubEventType determines if this is a deployment event
func determineGitHubEventType(payload []byte) string {
	var data map[string]interface{}
	if err := json.Unmarshal(payload, &data); err != nil {
		// If we can't parse it, let's assume it's not a recognized format
		return EventTypeUnknown
	}

	// Check if this is a deployment event
	if _, hasDeployment := data["deployment"]; hasDeployment {
		return EventTypeDeployment
	}

	// Not a deployment event
	return EventTypeUnknown
} 