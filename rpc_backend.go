// Copyright (c) 2022 Neil Feng
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"encoding/json"
	"fmt"
	"github.com/patrickmn/go-cache"
	"log"
	"time"

	"github.com/streadway/amqp"
)

/* RpcCeleryBackend CeleryBackend for AMQP rpc
The difference between amqpbackend and rpcbackend:
amqpbackend => reply to task_id queue
rpcbackend => reply to celery_backend exchange and route the message to the client
side who have waiting for it through the {oid}_result queue which is binding to
celery_backend exchange.
*/
type RpcCeleryBackend struct {
	*amqp.Channel
	Connection *amqp.Connection
	Queue      *AMQPQueue
	Exchange   *AMQPExchange

	oid            string
	task2Reply     *cache.Cache
	Initialized    bool
	ExpireDuration time.Duration
}

// NewRpcCeleryBackend creates new RpcCeleryBackend
func NewRpcCeleryBackend(host string) *RpcCeleryBackend {
	return NewRpcCeleryBackendByConnAndChannel(NewAMQPConnection(host))
}

// NewRpcCeleryBackendByConnAndChannel creates new RpcCeleryBackend by AMQP connection and channel
func NewRpcCeleryBackendByConnAndChannel(conn *amqp.Connection, channel *amqp.Channel) *RpcCeleryBackend {
	backend := &RpcCeleryBackend{
		Channel:    channel,
		Connection: conn,
		Queue: &AMQPQueue{
			Durable:    true,
			AutoDelete: true,
		},
		Exchange: &AMQPExchange{
			Name:       "celery_backend",
			Type:       "fanout",
			Durable:    true,
			AutoDelete: true,
		},
		ExpireDuration: 24 * time.Hour,
		task2Reply:     cache.New(24*time.Hour, 5*time.Minute),
	}
	return backend
}

func (b *RpcCeleryBackend) Init(oid string) error {
	// autodelete is automatically set to true by python
	// (406) PRECONDITION_FAILED - inequivalent arg 'durable' for queue 'bc58c0d895c7421eb7cb2b9bbbd8b36f' in vhost '/': received 'true' but current is 'false'

	args := amqp.Table{"x-expires": int32(b.ExpireDuration.Microseconds())}
	b.Queue.Name = fmt.Sprintf("%s_%s", oid, "result")
	_, err := b.QueueDeclare(
		b.Queue.Name,       // name
		b.Queue.Durable,    // durable
		b.Queue.AutoDelete, // autoDelete
		false,              // exclusive
		false,              // noWait
		args,               // args
	)
	if err != nil {
		return err
	}

	err = b.ExchangeDeclare(
		b.Exchange.Name,
		b.Exchange.Type,
		b.Exchange.Durable,
		b.Exchange.AutoDelete,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	err = b.QueueBind(b.Queue.Name,
		"",
		b.Exchange.Name,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	b.Initialized = true
	return b.startConsume()
}

func (b *RpcCeleryBackend) startConsume() error {
	// open channel temporarily
	channel, err := b.Consume(b.Queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case delivery := <-channel:
				deliveryAck(delivery)
				var resultMessage ResultMessage
				if err = json.Unmarshal(delivery.Body, &resultMessage); err != nil {
					log.Print("Error: unserialize result failed.")
					continue
				}
				// use cache map to ensure there no resource leaked after long-time running.
				// 'cause all task_result are broadcast to every client queue.
				// So there will be lots of task_result which belongs to other client received.
				// We must ensure that those task_result which our client wouldn't care can
				// be purged after expiration time.
				b.task2Reply.SetDefault(delivery.CorrelationId, &resultMessage)
			}
		}
	}()
	return nil
}

// GetResult retrieves result from queue named by oid
func (b *RpcCeleryBackend) GetResult(taskID string) (*ResultMessage, error) {
	if b.Initialized == false {
		return nil, fmt.Errorf("consumming on an unintialized broker is rejected")
	}

	// First we lookup the task2Reply Map. If we found what we need, remove it from the map, then return the result.
	if result, ok := b.task2Reply.Get(taskID); ok {
		b.task2Reply.Delete(taskID)
		return result.(*ResultMessage), nil
	} else {
		return nil, ResultNotAvailableYet
	}
}

// SetResult sets result back to result exchange
func (b *RpcCeleryBackend) SetResult(taskID string, result *ResultMessage) error {
	result.ID = taskID

	resBytes, err := json.Marshal(result)
	if err != nil {
		return err
	}

	message := amqp.Publishing{
		DeliveryMode:  amqp.Persistent,
		Timestamp:     time.Now(),
		ContentType:   "application/json",
		CorrelationId: taskID,
		Body:          resBytes,
	}

	return b.Publish(
		b.Exchange.Name,
		"",
		false,
		false,
		message,
	)
}
