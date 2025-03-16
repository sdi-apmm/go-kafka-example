package producer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/yourusername/kafka-consumer/config"
)

// KafkaProducer wraps the franz-go client
type KafkaProducer struct {
	client      *kgo.Client
	metrics     ProducerMetrics
	metricsLock sync.RWMutex
}

// NewKafkaProducer creates a new Kafka producer with minimal configuration
func NewKafkaProducer(brokers []string) (*KafkaProducer, error) {
	return NewKafkaProducerWithConfig(&config.ProducerConfig{
		Brokers: brokers,
	})
}

// NewKafkaProducerWithConfig creates a new Kafka producer using the full configuration
func NewKafkaProducerWithConfig(cfg *config.ProducerConfig) (*KafkaProducer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
	}

	// Configure SASL if enabled
	if cfg.SASLEnabled {
		var mechanism sasl.Mechanism
		var err error

		switch cfg.SASLMechanism {
		case "PLAIN":
			mechanism = plain.New(cfg.SASLUsername, cfg.SASLPassword)
		case "SCRAM-SHA-256":
			mechanism, err = scram.New(scram.SHA256, cfg.SASLUsername, cfg.SASLPassword)
		case "SCRAM-SHA-512":
			mechanism, err = scram.New(scram.SHA512, cfg.SASLUsername, cfg.SASLPassword)
		default:
			return nil, fmt.Errorf("unsupported SASL mechanism: %s", cfg.SASLMechanism)
		}

		if err != nil {
			return nil, fmt.Errorf("failed to create SASL mechanism: %w", err)
		}

		opts = append(opts, kgo.SASL(mechanism))
		log.Printf("Configured producer SASL authentication with mechanism %s", cfg.SASLMechanism)
	}

	// Configure TLS if enabled
	if cfg.TLSEnabled {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}

		// Add CA certificate if provided
		if cfg.CertificateBase64 != "" {
			caCert, err := cfg.GetDecodedCertificate()
			if err != nil {
				return nil, fmt.Errorf("failed to decode producer CA certificate: %w", err)
			}

			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return nil, fmt.Errorf("failed to add producer CA certificate to pool")
			}

			tlsConfig.RootCAs = caCertPool
			log.Println("Configured producer TLS with provided CA certificate")
		} else {
			log.Println("Configured producer TLS with system CA certificate pool")
		}

		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	return &KafkaProducer{
		client:      client,
		metrics:     ProducerMetrics{},
		metricsLock: sync.RWMutex{},
	}, nil
}

// Close closes the Kafka client
func (p *KafkaProducer) Close() {
	p.client.Close()
}

// GetClient returns the underlying Kafka client
func (p *KafkaProducer) GetClient() *kgo.Client {
	return p.client
} 