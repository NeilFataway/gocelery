// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"fmt"
	"github.com/streadway/amqp"
	"time"
)

// AMQPExchange stores AMQP Exchange configuration
type AMQPExchange struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
}

// AMQPQueue stores AMQP RpcQueue configuration
type AMQPQueue struct {
	Name       string
	Durable    bool
	AutoDelete bool
}

//AMQPCeleryBroker is RedisBroker for AMQP
type AMQPCeleryBroker struct {
	*amqp.Channel
	Connection     *amqp.Connection
	DirectExchange *AMQPExchange
	RpcQueue       *AMQPQueue
	DispatchQueue  *AMQPQueue
	channel        <-chan amqp.Delivery
	Initialized    bool
	Rate           int
}

// NewAMQPConnection creates new AMQP channel
func NewAMQPConnection(host string) (*amqp.Connection, *amqp.Channel) {
	connection, err := amqp.Dial(host)
	if err != nil {
		panic(err)
	}

	channel, err := connection.Channel()
	if err != nil {
		panic(err)
	}
	return connection, channel
}

// NewAMQPCeleryBroker creates new AMQPCeleryBroker
func NewAMQPCeleryBroker(host string) *AMQPCeleryBroker {
	return NewAMQPCeleryBrokerByConnAndChannel(NewAMQPConnection(host))
}

// NewAMQPCeleryBrokerByConnAndChannel creates new AMQPCeleryBroker using AMQP conn and channel
func NewAMQPCeleryBrokerByConnAndChannel(conn *amqp.Connection, channel *amqp.Channel) *AMQPCeleryBroker {
	broker := &AMQPCeleryBroker{
		Channel:    channel,
		Connection: conn,
		DirectExchange: &AMQPExchange{
			Name:       "celery_rpc",
			Type:       "direct",
			Durable:    true,
			AutoDelete: true,
		},
		DispatchQueue: &AMQPQueue{
			Name:       "celery_dispatch",
			Durable:    true,
			AutoDelete: true,
		},
		Rate: 4,
	}

	return broker
}

// StartConsumingChannel spawns receiving channel on AMQP queue
func (b *AMQPCeleryBroker) StartConsumingChannel() error {
	if b.Initialized == false {
		return fmt.Errorf("consumming on an unintialized broker is rejected")
	}

	if b.RpcQueue != nil {
		// Broker is in RPC mod when RPCQueue is not nil
		channel, err := b.Consume(b.RpcQueue.Name, "", false, false, false, false, nil)
		if err != nil {
			return err
		}
		b.channel = channel
	} else {
		// Broker is in Dispatch mod when RPCQueue is not nil
		channel, err := b.Consume(b.DispatchQueue.Name, "", false, false, false, false, nil)
		if err != nil {
			return err
		}
		b.channel = channel
	}

	return nil
}

func (b *AMQPCeleryBroker) Init(oid string) error {
	/* AMQP broker create two exchange, one with direct type, another with fanout type.
	AMQP broker crate only one queue meanwhile. Both exchange should bind to the specied queue.
	Direct type exchange should bind to the queue with routing key {oid}.
	*/
	if len(oid) > 0 {
		// broker is in RPC mode when oid is not null
		b.RpcQueue = &AMQPQueue{
			Name:       fmt.Sprintf("%s_rpc_quue", oid),
			Durable:    true,
			AutoDelete: true,
		}

		if err := b.ExchangeDeclare(
			b.DirectExchange.Name,
			b.DirectExchange.Type,
			b.DirectExchange.Durable,
			b.DirectExchange.AutoDelete,
			false,
			false,
			nil,
		); err != nil {
			return err
		}

		if _, err := b.QueueDeclare(
			b.RpcQueue.Name,
			b.RpcQueue.Durable,
			b.RpcQueue.AutoDelete,
			false,
			false,
			amqp.Table{
				"x-max-priority": 9,
			},
		); err != nil {
			return err
		}

		if err := b.QueueBind(
			b.RpcQueue.Name,
			oid,
			b.DirectExchange.Name,
			false,
			nil,
		); err != nil {
			return err
		}
	} else {
		// broker is in dispatch mode when oid is null
		if _, err := b.QueueDeclare(
			b.DispatchQueue.Name,
			b.DispatchQueue.Durable,
			b.DispatchQueue.AutoDelete,
			false,
			false,
			amqp.Table{
				"x-max-priority": 9,
			},
		); err != nil {
			return err
		}
	}

	if err := b.Qos(b.Rate, 0, false); err != nil {
		return err
	}

	b.Initialized = true

	if err := b.StartConsumingChannel(); err != nil {
		return err
	}

	return nil
}

// SendCeleryMessage sends CeleryMessage to broker
func (b *AMQPCeleryBroker) SendCeleryMessage(message *CeleryMessage) error {
	publishMessage := amqp.Publishing{
		DeliveryMode:    message.Properties.DeliveryMode,
		Timestamp:       time.Now(),
		ContentType:     message.ContentType,
		Body:            []byte(message.Body),
		Headers:         message.Headers,
		ReplyTo:         message.Properties.ReplyTo,
		CorrelationId:   message.Properties.CorrelationID,
		ContentEncoding: message.ContentEncoding,
		Priority:        message.Properties.DeliveryInfo.Priority,
	}

	if message.Properties.DeliveryInfo.RoutingKey == "" {
		// The message should be deliver to dispatch queue when the routing key is empty,
		message.Properties.DeliveryInfo.RoutingKey = b.DispatchQueue.Name
		message.Properties.DeliveryInfo.Exchange = ""
	} else {
		// The message should be deliver to rpc queue when the routing key is not empty,
		// message.Properties.DeliveryInfo.RoutingKey = b.RpcQueue.Name
		message.Properties.DeliveryInfo.Exchange = b.DirectExchange.Name
	}

	return b.Publish(
		message.Properties.DeliveryInfo.Exchange,
		message.Properties.DeliveryInfo.RoutingKey,
		true,
		false,
		publishMessage,
	)
}

func (b *AMQPCeleryBroker) GetCeleryMessage() (*CeleryMessage, error) {
	select {
	case delivery := <-b.channel:
		deliveryAck(delivery)
		message := &CeleryMessage{
			Body:            string(delivery.Body),
			Headers:         delivery.Headers,
			ContentType:     delivery.ContentType,
			ContentEncoding: delivery.ContentEncoding,
			Properties: CeleryProperties{
				BodyEncoding:  "base64",
				CorrelationID: delivery.CorrelationId,
				ReplyTo:       delivery.ReplyTo,
				DeliveryInfo: &CeleryDeliveryInfo{
					Priority:   delivery.Priority,
					Exchange:   delivery.Exchange,
					RoutingKey: delivery.RoutingKey,
				},
				DeliveryMode: delivery.DeliveryMode,
				DeliveryTag:  fmt.Sprintf("%d%", delivery.DeliveryTag),
			},
		}
		return message, nil
	default:
		return nil, fmt.Errorf("consuming broker channel is empty")
	}
}
