package config

import (
	"encoding/base64"
	"os"
	"strconv"
	"strings"
)

// KafkaConfig holds Kafka-related configuration
type KafkaConfig struct {
	Brokers            []string
	ConsumerGroup      string
	Topics             []string
	SASLEnabled        bool
	SASLMechanism      string
	SASLUsername       string
	SASLPassword       string
	TLSEnabled         bool
	CertificateBase64  string
	OffsetResetStrategy string
	DeploymentSuccessTopic string
}

// ProducerConfig holds Kafka producer specific configuration
type ProducerConfig struct {
	Brokers            []string
	DeploymentSuccessTopic string
	SASLEnabled        bool
	SASLMechanism      string
	SASLUsername       string
	SASLPassword       string
	TLSEnabled         bool
	CertificateBase64  string
}

// HTTPConfig holds HTTP server configuration
type HTTPConfig struct {
	Port int
}

// AppConfig holds the application configuration
type AppConfig struct {
	Kafka   KafkaConfig
	Producer ProducerConfig
	HTTP    HTTPConfig
}

// NewConfig returns a new configuration with defaults and overrides from environment
func NewConfig() *AppConfig {
	config := &AppConfig{
		Kafka: KafkaConfig{
			Brokers:             []string{"localhost:9092"},
			ConsumerGroup:       "my-consumer-group",
			Topics:              []string{"my-topic"},
			SASLEnabled:         false,
			SASLMechanism:       "PLAIN",
			SASLUsername:        "",
			SASLPassword:        "",
			TLSEnabled:          false,
			CertificateBase64:   "",
			OffsetResetStrategy: "earliest",
			DeploymentSuccessTopic: "deployment-success-topic",
		},
		Producer: ProducerConfig{
			Brokers:           []string{"localhost:9092"},
			DeploymentSuccessTopic: "deployment-success-topic",
			SASLEnabled:       false,
			SASLMechanism:     "PLAIN",
			SASLUsername:      "",
			SASLPassword:      "",
			TLSEnabled:        false,
			CertificateBase64: "",
		},
		HTTP: HTTPConfig{
			Port: 8080,
		},
	}

	// Override with environment variables if present
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		config.Kafka.Brokers = strings.Split(brokers, ",")
	}

	if group := os.Getenv("KAFKA_CONSUMER_GROUP"); group != "" {
		config.Kafka.ConsumerGroup = group
	}

	if topics := os.Getenv("KAFKA_TOPICS"); topics != "" {
		config.Kafka.Topics = strings.Split(topics, ",")
	}

	// SASL Configuration
	if saslEnabled := os.Getenv("KAFKA_SASL_ENABLED"); saslEnabled == "true" {
		config.Kafka.SASLEnabled = true
	}

	if mechanism := os.Getenv("KAFKA_SASL_MECHANISM"); mechanism != "" {
		config.Kafka.SASLMechanism = mechanism
	}

	if username := os.Getenv("KAFKA_SASL_USERNAME"); username != "" {
		config.Kafka.SASLUsername = username
	}

	if password := os.Getenv("KAFKA_SASL_PASSWORD"); password != "" {
		config.Kafka.SASLPassword = password
	}

	// TLS Configuration
	if tlsEnabled := os.Getenv("KAFKA_TLS_ENABLED"); tlsEnabled == "true" {
		config.Kafka.TLSEnabled = true
	}

	if certBase64 := os.Getenv("KAFKA_CERTIFICATE_BASE64"); certBase64 != "" {
		config.Kafka.CertificateBase64 = certBase64
	}

	// Offset reset strategy
	if strategy := os.Getenv("KAFKA_OFFSET_RESET_STRATEGY"); strategy != "" {
		// Only accept valid values
		if strategy == "earliest" || strategy == "latest" {
			config.Kafka.OffsetResetStrategy = strategy
		}
	}

	if outputTopic := os.Getenv("KAFKA_DEPLOYMENT_SUCCESS_TOPIC"); outputTopic != "" {
		config.Kafka.DeploymentSuccessTopic = outputTopic
	}

	// Load Producer configuration from environment variables
	if brokers := os.Getenv("KAFKA_PRODUCER_BROKERS"); brokers != "" {
		config.Producer.Brokers = strings.Split(brokers, ",")
	}

	if outputTopic := os.Getenv("KAFKA_DEPLOYMENT_SUCCESS_TOPIC"); outputTopic != "" {
		config.Producer.DeploymentSuccessTopic = outputTopic
	}

	// SASL configuration for Producer
	if saslEnabled := os.Getenv("KAFKA_PRODUCER_SASL_ENABLED"); saslEnabled == "true" {
		config.Producer.SASLEnabled = true
	}

	if saslMechanism := os.Getenv("KAFKA_PRODUCER_SASL_MECHANISM"); saslMechanism != "" {
		config.Producer.SASLMechanism = saslMechanism
	}

	if saslUsername := os.Getenv("KAFKA_PRODUCER_SASL_USERNAME"); saslUsername != "" {
		config.Producer.SASLUsername = saslUsername
	}

	if saslPassword := os.Getenv("KAFKA_PRODUCER_SASL_PASSWORD"); saslPassword != "" {
		config.Producer.SASLPassword = saslPassword
	}

	// TLS configuration for Producer
	if tlsEnabled := os.Getenv("KAFKA_PRODUCER_TLS_ENABLED"); tlsEnabled == "true" {
		config.Producer.TLSEnabled = true
	}

	if certBase64 := os.Getenv("KAFKA_PRODUCER_CERTIFICATE_BASE64"); certBase64 != "" {
		config.Producer.CertificateBase64 = certBase64
	}

	// Override with environment variables
	if portStr := os.Getenv("HTTP_PORT"); portStr != "" {
		if port, err := strconv.Atoi(portStr); err == nil {
			config.HTTP.Port = port
		}
	}

	return config
}

// GetDecodedCertificate decodes the base64-encoded certificate
func (c *KafkaConfig) GetDecodedCertificate() ([]byte, error) {
	if c.CertificateBase64 == "" {
		return nil, nil
	}
	
	return base64.StdEncoding.DecodeString(c.CertificateBase64)
}

// GetDecodedCertificate decodes the base64-encoded certificate
func (c *ProducerConfig) GetDecodedCertificate() ([]byte, error) {
	if c.CertificateBase64 == "" {
		return nil, nil
	}
	
	return base64.StdEncoding.DecodeString(c.CertificateBase64)
} 