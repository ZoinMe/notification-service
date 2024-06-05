package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

// MessageProcessor processes messages from different topics.
type MessageProcessor struct{}

func (p *MessageProcessor) ProcessMessage(topic string, message *sarama.ConsumerMessage) {
	switch topic {
	case "request":
		p.handleRequest(message)
	case "comment":
		p.handleComment(message)
	default:
		fmt.Printf("Unknown topic: %s\n", topic)
	}
}

func (p *MessageProcessor) handleRequest(message *sarama.ConsumerMessage) {
	fmt.Printf("Received Request: %s\n", string(message.Value))
}

func (p *MessageProcessor) handleComment(message *sarama.ConsumerMessage) {
	fmt.Printf("Received Comment: %s\n", string(message.Value))
}

func main() {
	// Kafka broker addresses
	brokers := []string{"kafka:9092"}

	// Kafka topics to consume
	topics := []string{"request", "comment"}

	// Create new consumer group
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0 // Ensure compatibility with your Kafka version
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true

	group, err := sarama.NewConsumerGroup(brokers, "notification_group", config)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}
	defer group.Close()

	processor := &MessageProcessor{}

	// Create a new context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Trap SIGINT and SIGTERM signals to trigger a graceful shutdown
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for {
			if err := group.Consume(ctx, topics, &consumerGroupHandler{processor: processor}); err != nil {
				log.Fatalf("Error from consumer: %v", err)
			}
			// Check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
		}
	}()

	// Log errors from the consumer
	go func() {
		for err := range group.Errors() {
			log.Printf("Error: %v", err)
		}
	}()

	// Wait for a termination signal
	<-sigterm
	fmt.Println("Terminating: via signal")
	cancel()
}

// consumerGroupHandler handles messages for the consumer group.
type consumerGroupHandler struct {
	processor *MessageProcessor
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (h *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
func (h *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		h.processor.ProcessMessage(message.Topic, message)
		session.MarkMessage(message, "")
	}
	return nil
}
