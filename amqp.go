// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

const DefaultRetryDelay = 5

// deliveryAck acknowledges delivery message with retries on error
func deliveryAck(delivery amqp.Delivery) {
	var err error
	for retryCount := 3; retryCount > 0; retryCount-- {
		if err = delivery.Ack(false); err == nil {
			break
		}
	}
	if err != nil {
		log.Warnf("amqp_backend: failed to acknowledge result message %+v: %+v", delivery.MessageId, err)
	}
}

type ReconnectFunc func() error

type AMQPSession struct {
	url                       string
	ConsumerDeliveryChannel   <-chan amqp.Delivery
	conn                      *amqp.Connection
	RWLocker                  sync.RWMutex
	ConnectionCloseNotifyChan chan *amqp.Error
	ChannelCloseNotifyChan    chan *amqp.Error
	*amqp.Channel

	hooks []ReconnectFunc
}

func NewAMQPSession(url string) (*AMQPSession, error) {
	ac, err := amqp.Dial(url)
	if err != nil {
		return nil, errors.Wrap(err, "connect to amqp server failed.")
	}

	channel, err := ac.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "get amqp channel failed.")
	}

	session := &AMQPSession{
		url:                       url,
		conn:                      ac,
		ConnectionCloseNotifyChan: make(chan *amqp.Error),
		ChannelCloseNotifyChan:    make(chan *amqp.Error),
		Channel:                   channel,
	}

	ac.NotifyClose(session.ConnectionCloseNotifyChan)
	channel.NotifyClose(session.ChannelCloseNotifyChan)

	// Launch connection watcher/reconnect
	go session.watchNotifyClose()

	return session, nil
}

func (p *AMQPSession) SetupReconnectHooks(hook ReconnectFunc) {
	p.RWLocker.Lock()
	defer p.RWLocker.Unlock()

	p.hooks = append(p.hooks, hook)
}

func (p *AMQPSession) reConnect() error {
	var ac *amqp.Connection
	var err error

	ac, err = amqp.Dial(p.url)
	if err != nil {
		return errors.Wrap(err, "failed on reconnect to AMQP server")
	}

	p.conn = ac
	return nil
}

func (p *AMQPSession) newServerChannel() (*amqp.Channel, error) {
	if p.conn == nil {
		return nil, errors.New("r.Conn is nil - did this get instantiated correctly? bug?")
	}

	ch, err := p.conn.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "unable to instantiate channel")
	}
	return ch, err
}

func (p *AMQPSession) watchNotifyClose() {
	for {
		select {
		case closeErr := <-p.ConnectionCloseNotifyChan:
			func() {
				log.Warnf("received message on notify close connection: '%+v' (reconnecting)", closeErr)

				// Acquire mutex to pause all consumers/producers while we reconnect AND prevent
				// access to the channel map
				p.RWLocker.Lock()
				defer p.RWLocker.Unlock()

				var attempts int

				for {
					attempts++
					if err := p.reConnect(); err != nil {
						log.Warnf("unable to complete reconnect: %s; retrying in %d", err, DefaultRetryDelay)
						time.Sleep(time.Duration(DefaultRetryDelay) * time.Second)
						continue
					}
					log.Infof("successfully reconnected after %d attempts", attempts)
					break
				}

				// Create and set a new notify close channel (since old one gets shutdown)
				p.ConnectionCloseNotifyChan = make(chan *amqp.Error, 0)
				p.conn.NotifyClose(p.ConnectionCloseNotifyChan)

				// Update channel
				serverChannel, err := p.newServerChannel()
				if err != nil {
					log.Errorf("unable to set new channel: %s", err)
					panic(fmt.Sprintf("unable to set new channel: %s", err))
				}

				p.Channel = serverChannel
				p.ChannelCloseNotifyChan = make(chan *amqp.Error, 0)
				p.Channel.NotifyClose(p.ChannelCloseNotifyChan)

				// run reconnect hooks
				for _, hook := range p.hooks {
					if err = hook(); err != nil {
						log.WithError(err).Error("reconnect hook execute failed.")
					}
				}

				log.Info("watchNotifyClose has completed successfully")
			}()
		case closeErr := <-p.ChannelCloseNotifyChan:
			func() {
				if p.conn.IsClosed() {
					log.Info("Connection is closing. ignore channel close notify.")
					return
				}
				log.Warnf("received message on notify close channel: '%+v' (reconnecting)", closeErr)

				// Acquire mutex to pause all consumers/producers while we reconnect AND prevent
				// access to the channel map
				p.RWLocker.Lock()
				defer p.RWLocker.Unlock()

				// Update channel
				serverChannel, err := p.newServerChannel()
				if err != nil {
					log.Errorf("unable to set new channel: %s", err)
					panic(fmt.Sprintf("unable to set new channel: %s", err))
				}

				p.Channel = serverChannel
				p.ChannelCloseNotifyChan = make(chan *amqp.Error, 0)
				p.Channel.NotifyClose(p.ChannelCloseNotifyChan)

				// run reconnect hooks
				for _, hook := range p.hooks {
					if err = hook(); err != nil {
						log.WithError(err).Error("reconnect hook execute failed.")
					}
				}

				log.Info("watchNotifyClose has completed successfully")
			}()
		}

	}
}

func (p *AMQPSession) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if p.Channel == nil {
		ch, err := p.newServerChannel()
		if err != nil {
			return errors.Wrap(err, "unable to create server channel")
		}

		p.RWLocker.Lock()
		p.Channel = ch
		p.RWLocker.Unlock()
	}

	p.RWLocker.RLock()
	defer p.RWLocker.RUnlock()
	return p.Channel.Publish(exchange, key, mandatory, immediate, msg)
}

func (p *AMQPSession) delivery() <-chan amqp.Delivery {
	// Acquire lock (in case we are reconnecting and channels are being swapped)
	p.RWLocker.RLock()
	defer p.RWLocker.RUnlock()

	return p.ConsumerDeliveryChannel
}
