package producer

import (
	"context"
	"fmt"
	"log"
	"time"
	
	"github.com/twmb/franz-go/pkg/kgo"
)

// ProducerMetrics contains performance metrics for the producer
type ProducerMetrics struct {
	MessagesSent        int64
	MessageErrors       int64
	TotalLatencyMs      int64
	AvgLatencyMs        int64
	MaxLatencyMs        int64
	BytesSent           int64
}

// GetMetrics returns the current metrics
func (p *KafkaProducer) GetMetrics() ProducerMetrics {
	// Return a copy of the current metrics
	p.metricsLock.RLock()
	defer p.metricsLock.RUnlock()
	
	return p.metrics
}

// updateMetrics updates the producer metrics with a new message
func (p *KafkaProducer) updateMetrics(startTime time.Time, record *kgo.Record, err error) {
	p.metricsLock.Lock()
	defer p.metricsLock.Unlock()
	
	latencyMs := time.Since(startTime).Milliseconds()
	
	// Update message count metrics
	if err == nil {
		p.metrics.MessagesSent++
		p.metrics.BytesSent += int64(len(record.Value))
	} else {
		p.metrics.MessageErrors++
	}
	
	// Update latency metrics
	p.metrics.TotalLatencyMs += latencyMs
	if p.metrics.MessagesSent > 0 {
		p.metrics.AvgLatencyMs = p.metrics.TotalLatencyMs / p.metrics.MessagesSent
	}
	if latencyMs > p.metrics.MaxLatencyMs {
		p.metrics.MaxLatencyMs = latencyMs
	}
}

// produceWithObservability wraps the Kafka production with observability
func (p *KafkaProducer) produceWithObservability(ctx context.Context, record *kgo.Record) error {
	startTime := time.Now()
	topic := record.Topic
	
	// Log the attempt
	log.Printf("Producing message to topic %s with key %s (payload size: %d bytes)",
		topic, string(record.Key), len(record.Value))
	
	// Create a channel for the result
	result := make(chan error, 1)
	
	// Produce the message
	p.client.Produce(ctx, record, func(r *kgo.Record, err error) {
		result <- err
	})
	
	// Wait for the result
	err := <-result
	
	// Update metrics
	p.updateMetrics(startTime, record, err)
	
	// Log the outcome
	if err != nil {
		log.Printf("ERROR producing to topic %s: %v (latency: %d ms)",
			topic, err, time.Since(startTime).Milliseconds())
		return fmt.Errorf("error producing to Kafka: %w", err)
	}
	
	log.Printf("Successfully produced message to topic %s in %d ms",
		topic, time.Since(startTime).Milliseconds())
	return nil
} 