package middleware

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

// MessageHandler defines the function signature for processing messages
type MessageHandler func(*kgo.Record) error

// Middleware defines a function that wraps a MessageHandler to add functionality
type Middleware func(MessageHandler) MessageHandler 