# Kafka Event Processor

A robust Golang service for consuming GitHub deployment events from Kafka, processing them, and producing success events to another Kafka topic. Built with observability, health monitoring, and reliability in mind.

## Overview

This service consumes GitHub deployment events from a Kafka topic, identifies successful deployments, and produces standardized deployment success events to a configured output topic. It provides comprehensive observability through metrics, health endpoints, and detailed logging.

## Architecture

The service follows a pipeline architecture:

```
┌─────────────┐     ┌──────────────────────────────────────┐    ┌─────────────┐
│             │     │                                      │    │             │
│  Kafka      │     │  Kafka Event Processor               │    │  Kafka      │
│  (Input)    │───> │                                      │───>│  (Output)   │
│  Topic      │     │  ┌──────────┐     ┌─────────────┐    │    │  Topic      │
│             │     │  │Consumer  │────>│Middleware   │    │    │             │
└─────────────┘     │  └──────────┘     └─────┬───────┘    │    └─────────────┘
                    │                         │            │
                    │                         ▼            │
                    │                    ┌────────────┐    │    ┌─────────────┐
                    │                    │Producer    │    │    │  HTTP       │
                    │                    └────────────┘    │<───│  Monitoring │
                    │                                      │    │  API        │
                    └──────────────────────────────────────┘    └─────────────┘
```

1. **Kafka Consumer** - Reads deployment events from input Kafka topic
2. **Middleware Chain** - Processes events with specialized handlers
3. **Kafka Producer** - Writes processed events to output Kafka topic
4. **HTTP Server** - Provides health and metrics endpoints

## Components

- **Consumer**: Consumes messages from Kafka with middleware processing pipeline
- **Middleware**: Processes GitHub events and handles business logic
- **Producer**: Produces standardized deployment events with observability
- **HTTP Server**: Provides health and metrics endpoints for monitoring
- **Observability**: Comprehensive metrics and logging throughout the system

## Key Features

- **Middleware Pattern**: Flexible processing pipeline with middleware components
- **Robust Error Handling**: Graceful handling of errors with detailed logging
- **Comprehensive Observability**: 
  - Transaction IDs for request tracing
  - Detailed metrics for both consumer and producer
  - Structured logging
  - Health checks with degradation detection
- **Production-Ready Configuration**: 
  - TLS support 
  - SASL authentication
  - Environment variable configuration
- **Health Monitoring**: Rich health API with component-level status

## Configuration

The service is configured via environment variables:

### Kafka Configuration

Consumer configuration:
- `KAFKA_BROKERS=kafka-1:9092,kafka-2:9092`
- `KAFKA_CONSUMER_GROUP=github-deployment-processor`
- `KAFKA_TOPICS=github-events`
- `KAFKA_OFFSET_RESET_STRATEGY=earliest`

Authentication (if needed):
- `KAFKA_SASL_ENABLED=true`
- `KAFKA_SASL_MECHANISM=PLAIN` (or SCRAM-SHA-256, SCRAM-SHA-512)
- `KAFKA_SASL_USERNAME=username`
- `KAFKA_SASL_PASSWORD=password`

TLS (if needed):
- `KAFKA_TLS_ENABLED=true`
- `KAFKA_CERTIFICATE_BASE64=base64_encoded_certificate`

Producer configuration:
- `KAFKA_PRODUCER_BROKERS=kafka-1:9092,kafka-2:9092`
- `KAFKA_DEPLOYMENT_SUCCESS_TOPIC=deployment-success`

### HTTP Server Configuration

- `HTTP_PORT=8080`

## API Endpoints

### Health Endpoint

`GET /health`

Returns a comprehensive health status of the application with information about the Kafka producer, consumer, and HTTP server health.

Example response:

```json
{
  "status": "healthy",
  "timestamp": "2023-08-17T15:30:45Z",
  "version": "1.0.0",
  "details": {
    "kafka_producer": {
      "status": "healthy",
      "messagesSent": 245,
      "messageErrors": 2,
      "avgLatencyMs": 12,
      "maxLatencyMs": 67,
      "bytesSent": 34560
    },
    "kafka_consumer": {
      "status": "healthy",
      "topics": ["github-events"],
      "group": "github-deployment-processor",
      "messagesReceived": 248,
      "messagesProcessed": 248,
      "messageErrors": 0,
      "avgLatencyMs": 8,
      "maxLatencyMs": 45,
      "bytesReceived": 42560,
      "topicPartitions": 3
    },
    "http_server": {
      "status": "healthy"
    }
  }
}
```

Health Status can be:
- `healthy`: All systems operating normally
- `degraded`: System is operational but with issues (e.g., increased error rates)
- `unhealthy`: System is not functioning properly

HTTP Status Codes:
- `200 OK`: If status is "healthy" or "degraded"
- `503 Service Unavailable`: If status is "unhealthy"

### Metrics Endpoint

`GET /metrics`

Returns detailed performance metrics about the producer and consumer, including messages processed, error rates, and latency information.

Example response:

```json
{
  "producer": {
    "messages_sent": 245,
    "messages_errors": 2,
    "avg_latency_ms": 12,
    "max_latency_ms": 67,
    "bytes_sent": 34560
  },
  "consumer": {
    "messages_received": 248,
    "messages_processed": 248,
    "messages_errors": 0,
    "avg_latency_ms": 8,
    "max_latency_ms": 45,
    "bytes_received": 42560,
    "topic_partitions": 3
  }
}
```

## Message Processing Flow

1. **Input Message**: GitHub deployment event received from Kafka
2. **Event Type Middleware**: Identifies the GitHub event type from headers
3. **Deployment Processor Middleware**: For deployment events, checks for successful status
4. **Output Production**: Creates and produces standardized deployment success events
5. **Observability**: Records metrics and detailed logs throughout the process

## Data Models

### Input: GitHub Deployment Event

Simplified example of a GitHub deployment event:

```json
{
  "action": "created",
  "deployment_status": {
    "id": 12345,
    "state": "success",
    "description": "Deployment completed successfully",
    "created_at": "2023-08-17T15:30:45Z",
    "updated_at": "2023-08-17T15:31:15Z"
  },
  "deployment": {
    "id": 67890,
    "ref": "main",
    "task": "deploy",
    "environment": "production",
    "created_at": "2023-08-17T15:30:00Z",
    "updated_at": "2023-08-17T15:30:45Z"
  },
  "repository": {
    "id": 98765,
    "name": "my-service",
    "full_name": "organization/my-service",
    "html_url": "https://github.com/organization/my-service"
  }
}
```

### Output: Deployment Success Event

```json
{
  "deployment_id": 67890,
  "repository_id": 98765,
  "environment": "production",
  "ref": "main",
  "created_at": "2023-08-17T15:30:00Z",
  "completed_at": "2023-08-17T15:31:15Z"
}
```

## Running the Service

### Using Docker

docker run -p 8080:8080 \
  -e KAFKA_BROKERS=kafka-1:9092 \
  -e KAFKA_CONSUMER_GROUP=github-deployment-processor \
  -e KAFKA_TOPICS=github-events \
  -e KAFKA_DEPLOYMENT_SUCCESS_TOPIC=deployment-success \
  yourusername/kafka-consumer:latest

### Building from Source

```
# Clone the repository
git clone https://github.com/yourusername/kafka-consumer
cd kafka-consumer

# Build
go build -o kafka-consumer

# Run
./kafka-consumer
```

## Monitoring

The service is designed to integrate with standard monitoring systems:

- **Kubernetes**: Use the `/health` endpoint for liveness and readiness probes
- **Prometheus**: The metrics endpoint is compatible with Prometheus scraping
- **ELK Stack**: Logs are structured for easy integration with log aggregation systems
- **Tracing Systems**: Use the transaction IDs in logs for distributed tracing

## Development

### Adding New Event Types

To add a new event type:

1. Add the appropriate model in `model/` directory
2. Create a new middleware in `middleware/` directory
3. Register the middleware in `main.go`

### Middleware Pattern

The service uses a middleware pattern for message processing that allows for flexible pipeline configuration.

## Testing

The service includes comprehensive unit and integration tests to ensure reliability.

### Running Tests

```
go test -v ./...
```

### Test Coverage

```
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### Unit Tests

Key components with unit tests:

- **Consumer**: Tests for message processing, error handling, and metrics collection
- **Producer**: Tests for message production and retry logic
- **Middleware**: Tests for each middleware component
- **Health Checks**: Tests for health status determination logic
