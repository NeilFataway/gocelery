// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/streadway/amqp"
)

/* AMQPCeleryBackend CeleryBackend for AMQP
The difference between amqpbackend and rpcbackend:
amqpbackend => reply to task_id queue
rpcbackend => reply to celery_backend exchange and route the message to the client
side who have waiting for it through the {oid}_result queue which is binding to
celery_backend exchange.
*/
type AMQPCeleryBackend struct {
	*AMQPSession
	ExpireDuration time.Duration
}

// NewAMQPCeleryBackend creates new AMQPCeleryBackend
func NewAMQPCeleryBackend(host string) *AMQPCeleryBackend {
	session, err := NewAMQPSession(host)
	if err != nil {
		panic(err)
	}
	backend := NewAMQPCeleryBackendByAMQPSession(session)
	return backend
}

// NewAMQPCeleryBackendByConnAndChannel creates new AMQPCeleryBackend by AMQP connection and channel
func NewAMQPCeleryBackendByAMQPSession(session *AMQPSession) *AMQPCeleryBackend {
	backend := &AMQPCeleryBackend{
		AMQPSession:    session,
		ExpireDuration: 24 * time.Hour,
	}
	return backend
}

// GetResult retrieves result from AMQP queue
func (b *AMQPCeleryBackend) GetResult(taskID string) (*ResultMessage, error) {

	queueName := strings.Replace(taskID, "-", "", -1)

	args := amqp.Table{"x-expires": int32(b.ExpireDuration.Microseconds())}

	_, err := b.QueueDeclare(
		queueName, // name
		true,      // durable
		true,      // autoDelete
		false,     // exclusive
		false,     // noWait
		args,      // args
	)
	if err != nil {
		return nil, err
	}

	err = b.ExchangeDeclare(
		"default",
		"direct",
		true,
		true,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	// open channel temporarily
	channel, err := b.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	var resultMessage ResultMessage

	select {
	case delivery := <-channel:
		deliveryAck(delivery)
		if err = json.Unmarshal(delivery.Body, &resultMessage); err != nil {
			return nil, err
		}
	default:
		return nil, ResultNotAvailableYet
	}

	return &resultMessage, nil
}

// SetResult sets result back to AMQP queue
func (b *AMQPCeleryBackend) SetResult(taskID string, result *ResultMessage) error {

	result.ID = taskID

	//queueName := taskID
	queueName := strings.Replace(taskID, "-", "", -1)

	// autodelete is automatically set to true by python
	// (406) PRECONDITION_FAILED - inequivalent arg 'durable' for queue 'bc58c0d895c7421eb7cb2b9bbbd8b36f' in vhost '/': received 'true' but current is 'false'

	args := amqp.Table{"x-expires": int32(b.ExpireDuration.Microseconds())}
	_, err := b.QueueDeclare(
		queueName, // name
		true,      // durable
		true,      // autoDelete
		false,     // exclusive
		false,     // noWait
		args,      // args
	)
	if err != nil {
		return err
	}

	err = b.ExchangeDeclare(
		"default",
		"direct",
		true,
		true,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	resBytes, err := json.Marshal(result)
	if err != nil {
		return err
	}

	message := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "application/json",
		Body:         resBytes,
	}
	return b.Publish(
		"",
		queueName,
		false,
		false,
		message,
	)
}

func (b *AMQPCeleryBackend) Init(string) error {
	// amqp backend do nothing through init phase
	return nil
}
