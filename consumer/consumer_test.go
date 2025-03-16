package consumer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/twmb/franz-go/pkg/kgo"
)

// MockKafkaClient is a mock implementation of the Kafka client interface
type MockKafkaClient struct {
	mock.Mock
}

func (m *MockKafkaClient) AssignedPartitions() []int {
	args := m.Called()
	return args.Get(0).([]int)
}

func TestConsumerMetrics(t *testing.T) {
	// Create a mock client
	mockClient := new(MockKafkaClient)
	mockClient.On("AssignedPartitions").Return([]int{0, 1, 2})

	// Create a consumer with the mock client
	consumer := &KafkaConsumer{
		client:      mockClient,
		metrics:     ConsumerMetrics{},
		metricsLock: &sync.RWMutex{},
	}

	// Update lag metrics
	consumer.UpdateLagMetrics()

	// Verify metrics
	metrics := consumer.GetMetrics()
	assert.Equal(t, 3, metrics.TopicPartitions)
}

func TestUpdateMetrics(t *testing.T) {
	// Create a consumer
	consumer := &KafkaConsumer{
		metrics:     ConsumerMetrics{},
		metricsLock: &sync.RWMutex{},
	}

	// Create a test record
	record := &kgo.Record{
		Value: []byte("test message"),
		Topic: "test-topic",
	}

	// Test successful message processing
	startTime := time.Now().Add(-100 * time.Millisecond)
	consumer.updateMetrics(startTime, record, nil)

	// Verify metrics
	metrics := consumer.GetMetrics()
	assert.Equal(t, int64(1), metrics.MessagesReceived)
	assert.Equal(t, int64(1), metrics.MessagesProcessed)
	assert.Equal(t, int64(0), metrics.MessageErrors)
	assert.Greater(t, metrics.TotalLatencyMs, int64(0))
	assert.Greater(t, metrics.AvgLatencyMs, int64(0))
	assert.Greater(t, metrics.MaxLatencyMs, int64(0))
	assert.Equal(t, int64(len(record.Value)), metrics.BytesReceived)

	// Test error message processing
	consumer.updateMetrics(startTime, record, errors.New("test error"))

	// Verify updated metrics
	metrics = consumer.GetMetrics()
	assert.Equal(t, int64(2), metrics.MessagesReceived)
	assert.Equal(t, int64(1), metrics.MessagesProcessed)
	assert.Equal(t, int64(1), metrics.MessageErrors)
}

func TestProcessWithObservability(t *testing.T) {
	// Create a consumer
	consumer := &KafkaConsumer{
		metrics:     ConsumerMetrics{},
		metricsLock: &sync.RWMutex{},
	}

	// Create a test record
	record := &kgo.Record{
		Value: []byte("test message"),
		Topic: "test-topic",
	}

	// Create a test handler
	successHandler := func(r *kgo.Record) error {
		return nil
	}

	errorHandler := func(r *kgo.Record) error {
		return errors.New("test error")
	}

	// Test successful processing
	err := consumer.processWithObservability(successHandler, record)
	assert.NoError(t, err)

	// Test error processing
	err = consumer.processWithObservability(errorHandler, record)
	assert.Error(t, err)
	assert.Equal(t, "test error", err.Error())

	// Verify metrics were updated
	metrics := consumer.GetMetrics()
	assert.Equal(t, int64(2), metrics.MessagesReceived)
	assert.Equal(t, int64(1), metrics.MessagesProcessed)
	assert.Equal(t, int64(1), metrics.MessageErrors)
} 