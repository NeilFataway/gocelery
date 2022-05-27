// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"github.com/streadway/amqp"
	"log"
)

// deliveryAck acknowledges delivery message with retries on error
func deliveryAck(delivery amqp.Delivery) error {
	var err error
	for retryCount := 3; retryCount > 0; retryCount-- {
		if err = delivery.Ack(false); err == nil {
			break
		}
	}
	if err != nil {
		log.Printf("amqp_backend: failed to acknowledge result message %+v: %+v", delivery.MessageId, err)
		return err
	}
	return nil
}
