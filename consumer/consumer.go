package consumer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/yourusername/kafka-consumer/config"
	"github.com/yourusername/kafka-consumer/middleware"
)

// KafkaConsumer wraps the franz-go client
type KafkaConsumer struct {
	client       *kgo.Client
	middlewares  []middleware.Middleware
	handler      middleware.MessageHandler
	consumerGroup string
	topics        []string
	metrics      ConsumerMetrics
	metricsLock  sync.RWMutex
}

// NewKafkaConsumer creates a new Kafka consumer with the provided configuration
func NewKafkaConsumer(brokers []string, groupID string) (*KafkaConsumer, error) {
	return NewKafkaConsumerWithConfig(&config.KafkaConfig{
		Brokers:             brokers,
		ConsumerGroup:       groupID,
		OffsetResetStrategy: "earliest", // Default
	})
}

// NewKafkaConsumerWithConfig creates a new Kafka consumer using the full configuration
func NewKafkaConsumerWithConfig(cfg *config.KafkaConfig) (*KafkaConsumer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.AutoCommitInterval(5 * time.Second), // Set auto commit interval
		kgo.BlockRebalanceOnPoll(),              // Block rebalances while processing records
	}

	// Set the appropriate offset reset strategy based on config
	offsetResetOpt := kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()) // Default to earliest
	if cfg.OffsetResetStrategy == "latest" {
		offsetResetOpt = kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd())
		log.Println("Using 'latest' offset reset strategy")
	} else {
		log.Println("Using 'earliest' offset reset strategy")
	}
	opts = append(opts, offsetResetOpt)

	// Configure SASL if enabled
	if cfg.SASLEnabled {
		var mechanism sasl.Mechanism
		var err error

		switch cfg.SASLMechanism {
		case "PLAIN":
			mechanism = plain.New(cfg.SASLUsername, cfg.SASLPassword)
		case "SCRAM-SHA-256":
			mechanism, err = scram.NewSHA256(cfg.SASLUsername, cfg.SASLPassword)
		case "SCRAM-SHA-512":
			mechanism, err = scram.NewSHA512(cfg.SASLUsername, cfg.SASLPassword)
		default:
			return nil, fmt.Errorf("unsupported SASL mechanism: %s", cfg.SASLMechanism)
		}

		if err != nil {
			return nil, fmt.Errorf("failed to create SASL mechanism: %w", err)
		}

		opts = append(opts, kgo.SASL(mechanism))
	}

	// Configure TLS if enabled
	if cfg.TLSEnabled {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: false, // Set to true only for testing
		}

		// Add certificate if provided
		if cfg.CertificateBase64 != "" {
			certData, err := cfg.GetDecodedCertificate()
			if err != nil {
				return nil, fmt.Errorf("failed to decode certificate: %w", err)
			}

			certPool := x509.NewCertPool()
			if ok := certPool.AppendCertsFromPEM(certData); !ok {
				return nil, fmt.Errorf("failed to parse certificate")
			}

			tlsConfig.RootCAs = certPool
		}

		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	// Initialize with a default handler that logs messages
	return &KafkaConsumer{
		client:       client,
		middlewares:  []middleware.Middleware{},
		handler:      DefaultHandler,
		consumerGroup: cfg.ConsumerGroup,
		topics:       cfg.Topics,
		metrics:      ConsumerMetrics{},
		metricsLock:  sync.RWMutex{},
	}, nil
}

// Use adds middleware to the processing chain
func (k *KafkaConsumer) Use(middleware middleware.Middleware) {
	k.middlewares = append(k.middlewares, middleware)
}

// SetHandler sets the final message handler
func (k *KafkaConsumer) SetHandler(handler middleware.MessageHandler) {
	k.handler = handler
}

// buildChain builds the middleware chain
func (k *KafkaConsumer) buildChain() middleware.MessageHandler {
	finalHandler := k.handler
	for i := len(k.middlewares) - 1; i >= 0; i-- {
		finalHandler = k.middlewares[i](finalHandler)
	}
	return finalHandler
}

// Consume starts consuming messages from the specified topics
func (k *KafkaConsumer) Consume(ctx context.Context, topics []string) error {
	// Subscribe to the topics
	k.client.AddConsumeTopics(topics...)
	k.topics = topics  // Store the topics for metrics/health checks

	// Build the middleware chain
	handlerChain := k.buildChain()
	
	// Setup periodic lag metrics updates
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	// Process messages in a loop
	for {
		select {
		case <-ctx.Done():
			log.Println("Context canceled, stopping consumer")
			return nil
		case <-ticker.C:
			// Update lag metrics periodically
			k.UpdateLagMetrics()
		default:
			// Poll for messages
			fetches := k.client.PollFetches(ctx)
			if fetches.IsClientClosed() {
				return fmt.Errorf("client closed")
			}

			if errs := fetches.Errors(); len(errs) > 0 {
				// Handle errors, but continue processing
				for _, err := range errs {
					log.Printf("Error fetching from %s: %v", err.Topic, err.Err)
				}
			}

			// Process successful fetches
			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				p.EachRecord(func(record *kgo.Record) {
					// Use our observability wrapper
					if err := k.processWithObservability(handlerChain, record); err != nil {
						log.Printf("Error processing message: %v", err)
					}
				})
			})
		}

		// Small pause to avoid tight loop
		time.Sleep(100 * time.Millisecond)
	}
}

// Close closes the Kafka client
func (k *KafkaConsumer) Close() {
	k.client.Close()
}

// DefaultHandler logs basic information about the message and acknowledges it
// This can be used as the final handler in middleware chains or as a standalone handler
func DefaultHandler(record *kgo.Record) error {
	log.Printf("Received message from topic %s, partition %d, offset %d", 
		record.Topic, record.Partition, record.Offset)
	log.Printf("Message key: %s", string(record.Key))
	log.Printf("Message value: %s", string(record.Value))
	return nil
}

// SilentHandler simply acknowledges messages without logging
// Useful as a final handler when all processing is done in middleware
func SilentHandler(record *kgo.Record) error {
	// Just acknowledge the message without logging
	return nil
}

// GetConsumerGroup returns the name of the consumer group
func (k *KafkaConsumer) GetConsumerGroup() string {
	return k.consumerGroup
}

// GetTopics returns the topics the consumer is subscribed to
func (k *KafkaConsumer) GetTopics() []string {
	return k.topics
}

// processWithObservability wraps message processing with observability
func (k *KafkaConsumer) processWithObservability(handler middleware.MessageHandler, record *kgo.Record) error {
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