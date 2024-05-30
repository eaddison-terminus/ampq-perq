package amqp_perq

import (
	"fmt"
	"log"
	"os/exec"

	"github.com/streadway/amqp"
)

// AmqpClient This implementation sets up a sendChan channel for sending requests, a replyChan channel for receiving responses,
// and two goroutines: one for sending requests and one for handling responses.
// When the SendRequest method is called, the method creates a new Request object with a unique correlation ID,
// sends the request on the sendChan channel, and waits for a response with the same correlation ID on the replyChan channel.
// The sendRequests goroutine sets up an exclusive reply queue and consumes messages from it.
// When a request is received on the sendChan channel, the goroutine creates a new channel for receiving the response,
// publishes the request to the main queue with the reply-to header set to the exclusive reply queue,
// and waits for a response on the newly created response channel. When a response is received on the reply queue,
// the goroutine sends the response on the response channel for the corresponding correlation ID.
type AmqpClient struct {
	conn      *amqp.Connection
	sendChan  chan *Request
	replyChan chan *Response
}

type Request struct {
	CorrelationID string
	Payload       []byte
}

type Response struct {
	CorrelationID string
	Payload       []byte
}

func NewAmqpClient(url string) (*AmqpClient, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	sendChan := make(chan *Request)
	replyChan := make(chan *Response)

	client := &AmqpClient{
		conn:      conn,
		sendChan:  sendChan,
		replyChan: replyChan,
	}

	// Start the send and receive goroutine
	go client.forwardRequests()

	return client, nil
}

func (c *AmqpClient) Close() error {
	return c.conn.Close()
}

func (c *AmqpClient) SendRequest(payload []byte) (*Response, error) {
	// Generate a unique correlation ID
	correlationID := generateCorrelationID()

	// Create the request object
	request := &Request{
		CorrelationID: correlationID,
		Payload:       payload,
	}

	// Send the request on the send channel
	c.sendChan <- request

	// Wait for a response with the same correlation ID on the reply channel
	for {
		select {
		case response := <-c.replyChan:
			if response.CorrelationID == correlationID {
				return response, nil
			}
		}
	}
}

func (c *AmqpClient) forwardRequests() {
	// Create a channel for sending AMQP messages
	sendCh, err := c.conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}

	// Declare the reply queue
	replyQ, err := sendCh.QueueDeclare("", false, false, true, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare a reply queue: %v", err)
	}

	// Consume from the reply queue
	replyCh, err := sendCh.Consume(replyQ.Name, "", false, true, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to consume from reply queue: %v", err)
	}

	// Map of correlation IDs to channels for responses
	responseChMap := make(map[string]chan *Response)

	for {
		select {
		case request := <-c.sendChan:
			// Create a new channel for responses to this request
			responseChMap[request.CorrelationID] = c.replyChan

			// Publish the request on the main queue with the reply-to header set to the reply queue
			err := sendCh.Publish("", "main_queue", false, false, amqp.Publishing{
				CorrelationId: request.CorrelationID,
				ReplyTo:       replyQ.Name,
				Body:          request.Payload,
			})

			if err != nil {
				log.Printf("Failed to publish message: %v", err)
				continue
			}

		case delivery := <-replyCh:
			// Parse the correlation ID from the delivery headers
			correlationID, ok := delivery.Headers["correlation_id"].(string)
			if !ok {
				//log.Printf("Invalid correlation ID in response: %v", delivery.Headers)
				continue
			}

			// Find the response channel for this correlation ID
			responseCh, ok := responseChMap[correlationID]
			if !ok {
				log.Printf("No response channel for correlation ID %s", correlationID)
				continue
			}

			// Send the response on the response channel
			responseCh <- &Response{
				CorrelationID: correlationID,
				Payload:       delivery.Body,
			}

			// Basic ack
			err = sendCh.Ack(delivery.DeliveryTag, false)
			if err != nil {
				log.Fatalf("failed to send Ack: #{err}")
			}

			// Delete the response channel from the map
			delete(responseChMap, correlationID)
		}
	}
}

// generateCorrelationID generates a random string to use as a correlation ID
func generateCorrelationID() string {
	newUUID, err := exec.Command("uuidgen").Output()
	if err != nil {
		log.Fatal(err)
	}
	//formatted, err := fmt.Printf("%s", string(newUUID))
	formatted := fmt.Sprint(newUUID)
	return formatted
}
