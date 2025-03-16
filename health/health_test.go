package health

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yourusername/kafka-consumer/consumer"
	"github.com/yourusername/kafka-consumer/producer"
)

func TestHealthHandler(t *testing.T) {
	// Create mock producer and consumer
	mockProducer := &producer.KafkaProducer{
		// Initialize with test metrics
	}
	
	mockConsumer := &consumer.KafkaConsumer{
		// Initialize with test metrics
	}

	// Create health handler
	handler := HealthHandler(mockProducer, mockConsumer, "1.0.0")

	// Create a test request
	req, err := http.NewRequest("GET", "/health", nil)
	assert.NoError(t, err)

	// Create a response recorder
	rr := httptest.NewRecorder()

	// Serve the request
	handler.ServeHTTP(rr, req)

	// Check status code
	assert.Equal(t, http.StatusOK, rr.Code)

	// Parse response
	var response HealthResponse
	err = json.Unmarshal(rr.Body.Bytes(), &response)
	assert.NoError(t, err)

	// Verify response structure
	assert.Equal(t, "1.0.0", response.Version)
	assert.Equal(t, StatusHealthy, response.Status)
	assert.Contains(t, response.Details, "kafka_producer")
	assert.Contains(t, response.Details, "kafka_consumer")
	assert.Contains(t, response.Details, "http_server")
}

func TestDegradedHealth(t *testing.T) {
	// Create a producer with high error rate
	mockProducer := &producer.KafkaProducer{
		// Initialize with high error rate metrics
	}
	
	// Create consumer
	mockConsumer := &consumer.KafkaConsumer{
		// Initialize with metrics
	}

	// Create health handler
	handler := HealthHandler(mockProducer, mockConsumer, "1.0.0")

	// Create a test request
	req, err := http.NewRequest("GET", "/health", nil)
	assert.NoError(t, err)

	// Create a response recorder
	rr := httptest.NewRecorder()

	// Serve the request
	handler.ServeHTTP(rr, req)

	// Parse response
	var response HealthResponse
	err = json.Unmarshal(rr.Body.Bytes(), &response)
	assert.NoError(t, err)

	// Verify degraded status
	assert.Equal(t, StatusDegraded, response.Status)
} 