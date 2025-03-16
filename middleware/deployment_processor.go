package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/yourusername/kafka-consumer/model"
	"github.com/yourusername/kafka-consumer/producer"
)

// DeploymentProcessor middleware processes successful GitHub deployment events
func DeploymentProcessor(kafkaProducer *producer.KafkaProducer, outputTopic string) Middleware {
	return func(next MessageHandler) MessageHandler {
		return func(record *kgo.Record) error {
			// Get the event type from record headers
			eventType := getEventTypeFromHeader(record)
			
			// Only process deployment events
			if eventType != EventTypeDeployment {
				return next(record) // Skip non-deployment events
			}
			
			log.Printf("Processing deployment event")
			
			// Process the deployment event
			var deployEvent model.DeploymentEvent
			if err := json.Unmarshal(record.Value, &deployEvent); err != nil {
				log.Printf("Error parsing deployment event: %v", err)
				return next(record) // Continue the chain even if we can't parse
			}

			// Process the deployment
			processDeploymentEvent(kafkaProducer, outputTopic, &deployEvent)

			// Continue the middleware chain
			return next(record)
		}
	}
}

// processDeploymentEvent handles the business logic for processing deployment events
func processDeploymentEvent(kafkaProducer *producer.KafkaProducer, outputTopic string, deployEvent *model.DeploymentEvent) {
	// Check if this is a deployment status update with 'success' state
	if deployEvent.DeploymentStatus.State == "success" {
		log.Printf("Found successful deployment: ID=%d, Repo=%s, Env=%s", 
			deployEvent.Deployment.ID,
			deployEvent.Repository.FullName, 
			deployEvent.Deployment.Environment)

		// Create the success event to be produced
		successEvent := model.DeploymentSuccessEvent{
			DeploymentID: deployEvent.Deployment.ID,
			RepositoryID: deployEvent.Repository.ID,
			Environment:  deployEvent.Deployment.Environment,
			Ref:          deployEvent.Deployment.Ref,
			CreatedAt:    deployEvent.Deployment.CreatedAt,
			CompletedAt:  deployEvent.DeploymentStatus.UpdatedAt,
		}

		// Produce the success event to the output topic
		key := []byte(fmt.Sprintf("%d", deployEvent.Repository.ID))
		if err := kafkaProducer.ProduceJSON(outputTopic, key, successEvent); err != nil {
			log.Printf("Error producing success event: %v", err)
		}
	} else {
		log.Printf("Skipping deployment with state: %s", deployEvent.DeploymentStatus.State)
	}
}

// getEventTypeFromHeader extracts the GitHub event type from record headers
func getEventTypeFromHeader(record *kgo.Record) string {
	for _, header := range record.Headers {
		if string(header.Key) == GitHubEventTypeHeader {
			return string(header.Value)
		}
	}
	return EventTypeUnknown
} 