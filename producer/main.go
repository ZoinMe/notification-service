package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
)

// Comment struct represents a comment or notification.
type Comment struct {
	Text string `json:"text"`
}

func main() {
	// Initialize Fiber app
	app := fiber.New()
	api := app.Group("/api/v1") // /api/v1

	// Define separate endpoints for each type of notification
	api.Post("/join-team", joinTeam)
	api.Post("/comment-post", commentPost)
	api.Post("/comment-comment", commentComment)

	// Start server
	log.Fatal(app.Listen(":6061"))
}

// ConnectProducer connects to Kafka and returns a synchronous producer.
func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// PushCommentToQueue sends a comment message to the Kafka topic.
func PushCommentToQueue(topic string, message []byte) error {
	brokersUrl := []string{"kafka:9092"} // Use the default Kafka port 9092
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		return err
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message), // Use ByteEncoder instead of StringEncoder
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}

// joinTeam handles notifications for users joining a team.
func joinTeam(c *fiber.Ctx) error {
	return handleNotification(c, "team_notifications")
}

// commentPost handles notifications for users commenting on a post.
func commentPost(c *fiber.Ctx) error {
	return handleNotification(c, "post_notifications")
}

// commentComment handles notifications for users commenting on a comment.
func commentComment(c *fiber.Ctx) error {
	return handleNotification(c, "comment_notifications")
}

// handleNotification is a helper function to process notifications.
func handleNotification(c *fiber.Ctx, topic string) error {
	// Instantiate new Comment struct
	cmt := new(Comment)

	// Parse body into comment struct
	if err := c.BodyParser(cmt); err != nil {
		log.Println(err)
		return c.Status(fiber.StatusBadRequest).JSON(&fiber.Map{
			"success": false,
			"message": "Invalid request body",
		})
	}

	// Convert comment to bytes and send it to Kafka
	cmtInBytes, err := json.Marshal(cmt)
	if err != nil {
		log.Println(err)
		return c.Status(fiber.StatusInternalServerError).JSON(&fiber.Map{
			"success": false,
			"message": "Failed to marshal comment",
		})
	}

	err = PushCommentToQueue(topic, cmtInBytes)
	if err != nil {
		log.Println(err)
		return c.Status(fiber.StatusInternalServerError).JSON(&fiber.Map{
			"success": false,
			"message": "Failed to push comment to queue",
		})
	}

	// Return Comment in JSON format
	return c.JSON(&fiber.Map{
		"success": true,
		"message": "Comment pushed successfully",
		"comment": cmt,
	})
}
