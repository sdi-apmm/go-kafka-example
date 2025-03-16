package middleware

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/yourusername/kafka-consumer/model"
	"github.com/yourusername/kafka-consumer/producer"
)

// MockProducer mocks the producer interface
type MockProducer struct {
	mock.Mock
}

func (m *MockProducer) Produce(ctx context.Context, topic string, key, value []byte) error {
	args := m.Called(ctx, topic, key, value)
	return args.Error(0)
}

func TestDeploymentProcessor(t *testing.T) {
	// Create mock producer
	mockProducer := new(MockProducer)
	
	// Create test deployment event
	deploymentEvent := model.GitHubDeploymentStatusEvent{
		Action: "created",
		DeploymentStatus: model.DeploymentStatus{
			ID:    12345,
			State: "success",
		},
		Deployment: model.Deployment{
			ID:          67890,
			Environment: "production",
			Ref:         "main",
		},
		Repository: model.Repository{
			ID:   98765,
			Name: "test-repo",
		},
	}
	
	// Marshal event to JSON
	eventJSON, _ := json.Marshal(deploymentEvent)
	
	// Create test Kafka record
	record := &kgo.Record{
		Value: eventJSON,
		Headers: []kgo.RecordHeader{
			{Key: "X-GitHub-Event", Value: []byte("deployment_status")},
		},
	}
	
	// Setup expectations
	mockProducer.On("Produce", mock.Anything, "deployment-success", mock.Anything, mock.Anything).Return(nil)
	
	// Create middleware
	middleware := DeploymentProcessor(mockProducer, "deployment-success")
	
	// Create a next handler that just returns nil
	nextHandler := func(r *kgo.Record) error {
		return nil
	}
	
	// Process the record
	handler := middleware(nextHandler)
	err := handler(record)
	
	// Verify
	assert.NoError(t, err)
	mockProducer.AssertExpectations(t)
}

func TestDeploymentProcessor_NonDeploymentEvent(t *testing.T) {
	// Create mock producer
	mockProducer := new(MockProducer)
	
	// Create test Kafka record (not a deployment event)
	record := &kgo.Record{
		Value: []byte(`{"type": "other_event"}`),
		Headers: []kgo.RecordHeader{
			{Key: "X-GitHub-Event", Value: []byte("push")},
		},
	}
	
	// Create middleware
	middleware := DeploymentProcessor(mockProducer, "deployment-success")
	
	// Create a next handler that just returns nil
	nextHandler := func(r *kgo.Record) error {
		return nil
	}
	
	// Process the record
	handler := middleware(nextHandler)
	err := handler(record)
	
	// Verify - should pass through to next handler without error
	assert.NoError(t, err)
	
	// Ensure producer was not called
	mockProducer.AssertNotCalled(t, "Produce")
} 