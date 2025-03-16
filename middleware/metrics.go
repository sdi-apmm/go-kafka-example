package middleware

import (
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Metrics is a middleware that tracks message processing time
func Metrics(next MessageHandler) MessageHandler {
	return func(record *kgo.Record) error {
		start := time.Now()
		err := next(record)
		duration := time.Since(start)
		log.Printf("Message processing took %v", duration)
		return err
	}
} 