package amqp_perq_test

import (
	amqp_perq "ampq-perq"
	"bytes"
	"github.com/streadway/amqp"
	"log"
	"testing"
)

func TestAMPQLibrary(t *testing.T) {
	// Start a new RabbitMQ server for testing
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		t.Fatalf("Failed to connect to RabbitMQ server: %v", err)
	}
	defer func(conn *amqp.Connection) {
		err := conn.Close()
		if err != nil {
			t.Fatalf("Failed to close to RabbitMQ connection: %v", err)
		}
	}(conn)

	// Start a new channel for testing
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Failed to open RabbitMQ channel: %v", err)
	}
	defer func(ch *amqp.Channel) {
		err := ch.Close()
		if err != nil {
			t.Fatalf("Failed to close to RabbitMQ connection: %v", err)
		}
	}(ch)

	// Declare the request and response queues for testing
	reqQueue, err := ch.QueueDeclare("main_queue", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Failed to declare request queue: %v", err)
	}

	// Consume from the request queue
	reqCh, err := ch.Consume(reqQueue.Name, "", false, true, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to consume from reply queue: %v", err)
	}

	// Create a new AMPQ client for testing
	client, err := amqp_perq.NewAmqpClient("amqp://guest:guest@localhost:5672/")
	if err != nil {
		t.Fatalf("Failed to create AMPQ client: %v", err)
	}
	defer func(client *amqp_perq.AmqpClient) {
		err := client.Close()
		if err != nil {
			t.Fatalf("Failed to close to RabbitMQ connection: %v", err)
		}
	}(client)

	// Make a stop channel for go routine signaling
	stopCh := make(chan bool)

	// Test sending and receiving a single request and response with a valid correlation ID
	expectedPayload := []byte("test-payload")
	go func() {
		for {
			select {
			case <-stopCh:
				return
			case request := <-reqCh:
				correlationID := request.CorrelationId
				if len(correlationID) == 0 {
					continue
				}

				payload := request.Body
				err = ch.Publish("", request.ReplyTo, false, false, amqp.Publishing{
					Headers: amqp.Table{"correlation_id": correlationID},
					Body:    payload,
				})
				if err != nil {
					t.Errorf("Failed to send reply: %v", err)
					return
				}
				// Basic ack
				err = ch.Ack(request.DeliveryTag, false)
				if err != nil {
					log.Fatalf("failed to send Ack: #{err}")
				}
			}
		}
	}()

	res, err := client.SendRequest(expectedPayload)
	if err != nil {
		t.Fatalf("Failed to get response: %v", err)
	}

	if !bytes.Equal(res.Payload, expectedPayload) {
		t.Fatalf("Unexpected response payload: expected %v, got %v", expectedPayload, res.Payload)
	}

	// Test sending and receiving multiple requests and responses with valid correlation IDs
	numRequests := 3
	expectedPayloads := [][]byte{
		[]byte("test-payload-1"),
		[]byte("test-payload-2"),
		[]byte("test-payload-3"),
	}
	for i := 0; i < numRequests; i++ {
		res, err := client.SendRequest(expectedPayloads[i])
		if err != nil {
			t.Fatalf("Failed to get response: %v", err)
		}
		if !bytes.Equal(res.Payload, expectedPayloads[i]) {
			t.Fatalf("Unexpected response payload: expected %v, got %v", expectedPayloads[i], res.Payload)
		}
	}

	stopCh <- true

	// Test sending a request with an invalid correlation ID and expect an error
	//invalidCorrelationID := "invalid-correlation-id"
	//err = ch.Publish("", reqQueue.Name, false, false, amqp.Publishing{
	//	Headers: amqp.Table{"correlation_id": invalidCorrelationID},
	//	Body:    expectedPayload,
	//})
	//if err != nil {
	//	t.Fatalf("Failed to send request: %v", err)
	//}
	//_, err = client.SendRequest()
	//if err == nil {
	//	t.Fatalf("Expected error when getting response with invalid correlation ID")
	//}

	// Test sending a request and not receiving a response within the specified timeout and expect a timeout error
	//longTimeout := 5 * time.Second
	//err = ch.Publish("", reqQueue.Name, false, false, amqp.Publishing{
	//	Headers: amqp.Table{"correlation_id": correlationID},
	//	Body:    expectedPayload,
	//})
	//if err != nil {
	//	t.Fatalf("Failed to send request: %v", err)
	//}
	//_, err = client.SendRequest()
	//if err == nil {
	//	t.Fatalf("Expected timeout error when getting response with long timeout")
	//}
}
