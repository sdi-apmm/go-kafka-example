package middleware

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// LoggingConfig allows customizing the logging behavior
type LoggingConfig struct {
	IncludePayload     bool // Whether to include message payload in logs
	MaxPayloadLogBytes int  // Maximum bytes of payload to log (if included)
	LogLevel           string // "debug", "info", "error"
	GenerateTransactionID bool // Whether to generate transaction IDs for tracing
}

// DefaultLoggingConfig provides sensible defaults
var DefaultLoggingConfig = LoggingConfig{
	IncludePayload:     false,
	MaxPayloadLogBytes: 256,
	LogLevel:           "info",
}

// Logging is a middleware that logs message details
func Logging(next MessageHandler) MessageHandler {
	return LoggingWithConfig(DefaultLoggingConfig, next)
}

// LoggingWithConfig is a middleware that logs message details with custom configuration
func LoggingWithConfig(config LoggingConfig, next MessageHandler) MessageHandler {
	return func(record *kgo.Record) error {
		// Record the start time
		startTime := time.Now()
		
		// Log basic message information
		log.Printf("MESSAGE RECEIVED: Topic=%s, Partition=%d, Offset=%d", 
			record.Topic, record.Partition, record.Offset)
		
		// Log timestamp information
		log.Printf("TIMESTAMP: Record=%v, Processing=%v", 
			time.Unix(0, record.Timestamp.UnixNano()), startTime)
		
		// Log key if present
		if len(record.Key) > 0 {
			log.Printf("KEY: %s", string(record.Key))
		}
		
		// Log headers if present
		if len(record.Headers) > 0 {
			log.Printf("HEADERS:")
			for _, header := range record.Headers {
				log.Printf("  %s: %s", string(header.Key), string(header.Value))
			}
		}
		
		// Log message size
		log.Printf("MESSAGE SIZE: %d bytes", len(record.Value))
		
		// Log payload sample if configured
		if config.IncludePayload && len(record.Value) > 0 {
			payloadToLog := record.Value
			if len(payloadToLog) > config.MaxPayloadLogBytes {
				payloadToLog = payloadToLog[:config.MaxPayloadLogBytes]
			}
			
			// Try to pretty-print if it's JSON
			var prettyJSON map[string]interface{}
			if err := json.Unmarshal(payloadToLog, &prettyJSON); err == nil {
				prettyBytes, _ := json.MarshalIndent(prettyJSON, "", "  ")
				log.Printf("PAYLOAD (JSON): \n%s", string(prettyBytes))
				if len(record.Value) > config.MaxPayloadLogBytes {
					log.Printf("... (truncated, %d more bytes)", len(record.Value) - config.MaxPayloadLogBytes)
				}
			} else {
				// Not valid JSON, log as string
				log.Printf("PAYLOAD: %s", string(payloadToLog))
				if len(record.Value) > config.MaxPayloadLogBytes {
					log.Printf("... (truncated, %d more bytes)", len(record.Value) - config.MaxPayloadLogBytes)
				}
			}
		}
		
		if config.GenerateTransactionID {
			// Create and add a transaction ID header
			txID := generateTransactionID()
			record.Headers = append(record.Headers, kgo.RecordHeader{
				Key:   []byte("X-Transaction-ID"),
				Value: []byte(txID),
			})
			log.Printf("TRANSACTION: %s", txID)
		}
		
		// Call the next handler in the chain
		err := next(record)
		
		// Record the end time and calculate duration
		endTime := time.Now()
		duration := endTime.Sub(startTime)
		
		// Log the result
		if err != nil {
			log.Printf("ERROR PROCESSING: Topic=%s, Partition=%d, Offset=%d, Duration=%v, Error=%v", 
				record.Topic, record.Partition, record.Offset, duration, err)
		} else {
			log.Printf("PROCESSING COMPLETE: Topic=%s, Partition=%d, Offset=%d, Duration=%v", 
				record.Topic, record.Partition, record.Offset, duration)
		}
		
		return err
	}
}

func generateTransactionID() string {
	// Create a random transaction ID for tracing
	// Format: TX-{timestamp}-{random_hex}
	return fmt.Sprintf("TX-%d-%x", 
		time.Now().UnixNano(), 
		rand.Intn(0xffffff)) // 6 hex digits
} 