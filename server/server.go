package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/yourusername/kafka-consumer/consumer"
	"github.com/yourusername/kafka-consumer/health"
	"github.com/yourusername/kafka-consumer/producer"
)

// Server represents the HTTP server for health checks and metrics
type Server struct {
	server   *http.Server
	producer *producer.KafkaProducer
	consumer *consumer.KafkaConsumer
}

// NewServer creates a new HTTP server for health checks and metrics
func NewServer(port int, producer *producer.KafkaProducer, consumer *consumer.KafkaConsumer) *Server {
	mux := http.NewServeMux()
	
	// Create the server
	srv := &Server{
		server: &http.Server{
			Addr:         fmt.Sprintf(":%d", port),
			Handler:      mux,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  15 * time.Second,
		},
		producer: producer,
		consumer: consumer,
	}
	
	// Register handlers - simplified to just health and metrics
	mux.HandleFunc("/health", health.SystemHealthHandler(producer, consumer))
	mux.HandleFunc("/metrics", srv.metricsHandler)
	
	return srv
}

// Start starts the HTTP server
func (s *Server) Start() {
	go func() {
		log.Printf("Starting HTTP server on %s", s.server.Addr)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()
}

// Shutdown gracefully shuts down the HTTP server
func (s *Server) Shutdown(ctx context.Context) error {
	log.Println("Shutting down HTTP server...")
	return s.server.Shutdown(ctx)
}

// metricsHandler exposes application metrics
func (s *Server) metricsHandler(w http.ResponseWriter, r *http.Request) {
	producerMetrics := s.producer.GetMetrics()
	consumerMetrics := s.consumer.GetMetrics()
	
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	
	fmt.Fprintf(w, `{
		"producer": {
			"messages_sent": %d,
			"messages_errors": %d,
			"avg_latency_ms": %d,
			"max_latency_ms": %d,
			"bytes_sent": %d
		},
		"consumer": {
			"messages_received": %d,
			"messages_processed": %d,
			"messages_errors": %d,
			"avg_latency_ms": %d,
			"max_latency_ms": %d,
			"bytes_received": %d,
			"topic_partitions": %d
		}
	}`, 
	producerMetrics.MessagesSent,
	producerMetrics.MessageErrors,
	producerMetrics.AvgLatencyMs,
	producerMetrics.MaxLatencyMs,
	producerMetrics.BytesSent,
	consumerMetrics.MessagesReceived,
	consumerMetrics.MessagesProcessed,
	consumerMetrics.MessageErrors,
	consumerMetrics.AvgLatencyMs,
	consumerMetrics.MaxLatencyMs,
	consumerMetrics.BytesReceived,
	consumerMetrics.TopicPartitions)
} 