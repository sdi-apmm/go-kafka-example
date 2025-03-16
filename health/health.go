package health

import (
	"encoding/json"
	"net/http"
	"time"
	
	"github.com/yourusername/kafka-consumer/consumer"
	"github.com/yourusername/kafka-consumer/producer"
)

// SystemHealth represents the complete health status of the application
type SystemHealth struct {
	Status    string                 `json:"status"`
	Timestamp string                 `json:"timestamp"`
	Version   string                 `json:"version"`
	Details   map[string]interface{} `json:"details"`
}

// Status constants
const (
	StatusHealthy   = "healthy"
	StatusDegraded  = "degraded"
	StatusUnhealthy = "unhealthy"
)

// AppVersion should be set from build information
var AppVersion = "1.0.0"

// SystemHealthHandler returns a handler that reports complete system health
func SystemHealthHandler(producer *producer.KafkaProducer, consumer *consumer.KafkaConsumer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		health := SystemHealth{
			Status:    StatusHealthy,
			Timestamp: time.Now().Format(time.RFC3339),
			Version:   AppVersion,
			Details:   make(map[string]interface{}),
		}
		
		// Check Kafka producer health
		producerMetrics := producer.GetMetrics()
		producerHealth := map[string]interface{}{
			"status":       StatusHealthy,
			"messagesSent": producerMetrics.MessagesSent,
			"messageErrors": producerMetrics.MessageErrors,
			"avgLatencyMs": producerMetrics.AvgLatencyMs,
			"maxLatencyMs": producerMetrics.MaxLatencyMs,
			"bytesSent":    producerMetrics.BytesSent,
		}
		
		// Determine producer health status
		if producerMetrics.MessagesSent > 0 && 
		   float64(producerMetrics.MessageErrors)/float64(producerMetrics.MessagesSent+producerMetrics.MessageErrors) >= 0.1 {
			producerHealth["status"] = StatusDegraded
			health.Status = StatusDegraded
		}
		
		if producerMetrics.MessageErrors > 10 && 
		   float64(producerMetrics.MessageErrors)/float64(producerMetrics.MessagesSent+producerMetrics.MessageErrors) >= 0.5 {
			producerHealth["status"] = StatusUnhealthy
			health.Status = StatusUnhealthy
		}
		
		health.Details["kafka_producer"] = producerHealth
		
		// Check consumer health with metrics
		consumerMetrics := consumer.GetMetrics()
		consumerHealth := map[string]interface{}{
			"status":            StatusHealthy,
			"topics":            consumer.GetTopics(),
			"group":             consumer.GetConsumerGroup(),
			"messagesReceived":  consumerMetrics.MessagesReceived,
			"messagesProcessed": consumerMetrics.MessagesProcessed,
			"messageErrors":     consumerMetrics.MessageErrors,
			"avgLatencyMs":      consumerMetrics.AvgLatencyMs,
			"maxLatencyMs":      consumerMetrics.MaxLatencyMs,
			"bytesReceived":     consumerMetrics.BytesReceived,
			"topicPartitions":   consumerMetrics.TopicPartitions,
		}
		
		// Determine consumer health status based on error rate
		if consumerMetrics.MessagesReceived > 0 && 
		   float64(consumerMetrics.MessageErrors)/float64(consumerMetrics.MessagesReceived) >= 0.1 {
			consumerHealth["status"] = StatusDegraded
			if health.Status != StatusUnhealthy { // Don't override unhealthy
				health.Status = StatusDegraded
			}
		}
		
		health.Details["kafka_consumer"] = consumerHealth
		
		// Check HTTP server health
		health.Details["http_server"] = map[string]interface{}{
			"status": StatusHealthy,
		}
		
		// Return appropriate status code based on overall health
		statusCode := http.StatusOK
		if health.Status == StatusUnhealthy {
			statusCode = http.StatusServiceUnavailable
		} else if health.Status == StatusDegraded {
			statusCode = http.StatusOK // Still 200 but with degraded status in body
		}
		
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		json.NewEncoder(w).Encode(health)
	}
}

// KafkaHealthHandler returns a handler that reports Kafka health
func KafkaHealthHandler(producer *producer.KafkaProducer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		metrics := producer.GetMetrics()
		
		// Consider the service healthy if we've successfully sent messages
		healthy := metrics.MessagesSent > 0 && 
			(metrics.MessageErrors == 0 || 
			 float64(metrics.MessageErrors)/float64(metrics.MessagesSent+metrics.MessageErrors) < 0.1)
		
		status := StatusHealthy
		statusCode := http.StatusOK
		
		if !healthy {
			status = StatusUnhealthy
			statusCode = http.StatusServiceUnavailable
		}
		
		response := map[string]interface{}{
			"status": status,
			"kafka": map[string]interface{}{
				"producer": map[string]interface{}{
					"messagesSent": metrics.MessagesSent,
					"messageErrors": metrics.MessageErrors,
					"avgLatencyMs": metrics.AvgLatencyMs,
					"maxLatencyMs": metrics.MaxLatencyMs,
					"bytesSent": metrics.BytesSent,
				},
			},
		}
		
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		json.NewEncoder(w).Encode(response)
	}
} 