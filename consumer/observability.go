package consumer

import (
	"log"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ConsumerMetrics contains performance metrics for the consumer
type ConsumerMetrics struct {
	MessagesReceived   int64
	MessagesProcessed  int64
	MessageErrors      int64
	TotalLatencyMs     int64
	AvgLatencyMs       int64
	MaxLatencyMs       int64
	BytesReceived      int64
	OffsetLag          int64  // How far behind we are from latest
	TopicPartitions    int    // Number of partitions being consumed
}

// GetMetrics returns the current metrics
func (k *KafkaConsumer) GetMetrics() ConsumerMetrics {
	// Return a copy of the current metrics
	k.metricsLock.RLock()
	defer k.metricsLock.RUnlock()
	
	return k.metrics
}

// updateMetrics updates the consumer metrics with a new message
func (k *KafkaConsumer) updateMetrics(startTime time.Time, record *kgo.Record, err error) {
	k.metricsLock.Lock()
	defer k.metricsLock.Unlock()
	
	latencyMs := time.Since(startTime).Milliseconds()
	
	// Update message count metrics
	k.metrics.MessagesReceived++
	k.metrics.BytesReceived += int64(len(record.Value))
	
	if err == nil {
		k.metrics.MessagesProcessed++
	} else {
		k.metrics.MessageErrors++
	}
	
	// Update latency metrics
	k.metrics.TotalLatencyMs += latencyMs
	if k.metrics.MessagesProcessed > 0 {
		k.metrics.AvgLatencyMs = k.metrics.TotalLatencyMs / k.metrics.MessagesProcessed
	}
	if latencyMs > k.metrics.MaxLatencyMs {
		k.metrics.MaxLatencyMs = latencyMs
	}
}

// processWithObservability wraps message processing with observability
func (k *KafkaConsumer) processWithObservability(handler MessageHandler, record *kgo.Record) error {
	startTime := time.Now()
	
	// Log the message receipt
	log.Printf("Consuming message from topic %s, partition %d, offset %d (payload size: %d bytes)",
		record.Topic, record.Partition, record.Offset, len(record.Value))
	
	// Process the message
	err := handler(record)
	
	// Update metrics
	k.updateMetrics(startTime, record, err)
	
	// Log the outcome
	if err != nil {
		log.Printf("ERROR processing message from topic %s: %v (latency: %d ms)",
			record.Topic, err, time.Since(startTime).Milliseconds())
	} else {
		log.Printf("Successfully processed message from topic %s in %d ms",
			record.Topic, time.Since(startTime).Milliseconds())
	}
	
	return err
}

// UpdateLagMetrics updates the consumer lag metrics
// This should be called periodically to update lag information
func (k *KafkaConsumer) UpdateLagMetrics() {
	// This would require fetching high watermarks from the broker
	// and comparing with current offsets
	// For a simplified version, we'll just track partitions
	
	// This is a placeholder - actual implementation would use client API
	// to get real lag information
	partitions := k.client.AssignedPartitions()
	
	k.metricsLock.Lock()
	defer k.metricsLock.Unlock()
	
	k.metrics.TopicPartitions = len(partitions)
} 