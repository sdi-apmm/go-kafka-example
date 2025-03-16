package producer

import (
	"context"
	"encoding/json"
	"fmt"
	
	"github.com/twmb/franz-go/pkg/kgo"
)

// ProduceJSON produces a JSON-encoded message to a topic with the specified key
func (p *KafkaProducer) ProduceJSON(topic string, key []byte, value interface{}) error {
	// Serialize to JSON
	jsonData, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("error serializing to JSON: %w", err)
	}
	
	// Create the record
	record := &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: jsonData,
	}
	
	// Produce the message
	return p.ProduceRecord(record)
}

// ProduceRecord produces a pre-configured record to Kafka
func (p *KafkaProducer) ProduceRecord(record *kgo.Record) error {
	ctx := context.Background()
	return p.produceWithObservability(ctx, record)
} 