package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/yourusername/kafka-consumer/config"
	"github.com/yourusername/kafka-consumer/consumer"
	"github.com/yourusername/kafka-consumer/middleware"
	"github.com/yourusername/kafka-consumer/producer"
	"github.com/yourusername/kafka-consumer/server"
)

func main() {
	// Create context that will be canceled on SIGINT or SIGTERM
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Load configuration
	appConfig := config.NewConfig()

	// Initialize the Kafka consumer with full config
	kafkaConsumer, err := consumer.NewKafkaConsumerWithConfig(&appConfig.Kafka)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	defer kafkaConsumer.Close()

	// Create a Kafka producer for the deployment processor middleware
	kafkaProducer, err := producer.NewKafkaProducerWithConfig(&appConfig.Producer)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer kafkaProducer.Close()

	// Add middleware for processing messages
	kafkaConsumer.Use(middleware.Logging)
	kafkaConsumer.Use(middleware.Metrics)

	// Add GitHub event processing middlewares
	kafkaConsumer.Use(middleware.GitHubEventType()) // First identify the event type
	kafkaConsumer.Use(middleware.DeploymentProcessor(
		kafkaProducer, 
		appConfig.Producer.DeploymentSuccessTopic,
	))

	// Set the final handler
	kafkaConsumer.SetHandler(consumer.DefaultHandler)

	// Create the HTTP server with configured port
	httpServer := server.NewServer(appConfig.HTTP.Port, kafkaProducer, kafkaConsumer)
	httpServer.Start()
	
	// Make sure to add graceful shutdown for the HTTP server
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		httpServer.Shutdown(shutdownCtx)
	}()

	// Start consuming messages
	log.Println("Starting Kafka consumer. Press Ctrl+C to exit.")
	if err := kafkaConsumer.Consume(ctx, appConfig.Kafka.Topics); err != nil {
		log.Fatalf("Error consuming messages: %v", err)
	}
}