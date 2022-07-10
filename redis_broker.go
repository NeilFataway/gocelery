// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"time"

	"github.com/gomodule/redigo/redis"
)

// RedisCeleryBroker is celery broker for redis
type RedisCeleryBroker struct {
	*redis.Pool
	DispatchBaseQueueName string
	RpcBaseQueueName      string
	messageChannel        chan interface{}
}

// NewRedisBroker creates new RedisCeleryBroker with given redis connection pool
func NewRedisBroker(conn *redis.Pool) *RedisCeleryBroker {
	return &RedisCeleryBroker{
		Pool:                  conn,
		DispatchBaseQueueName: "celery_dispatch",
		RpcBaseQueueName:      "celery_rpc",
		messageChannel:        make(chan interface{}, 5),
	}
}

// NewRedisCeleryBroker creates new RedisCeleryBroker based on given uri
//
// Deprecated: NewRedisCeleryBroker exists for historical compatibility
// and should not be used. Use NewRedisBroker instead to create new RedisCeleryBroker.
func NewRedisCeleryBroker(uri string) *RedisCeleryBroker {
	return &RedisCeleryBroker{
		Pool:                  NewRedisPool(uri),
		DispatchBaseQueueName: "celery_dispatch",
		RpcBaseQueueName:      "celery_rpc",
		messageChannel:        make(chan interface{}, 5),
	}
}

func (cb *RedisCeleryBroker) Init(oid string) error {
	rv := func(queueList ...interface{}) {
		conn := cb.Pool.Get()
		defer func() {
			_ = conn.Close()
		}()

		queueList = append(queueList)
		for {
			messageJSON, err := conn.Do("BRPOP", queueList...)
			if err != nil {
				log.Errorf("[redis_broker] failed to receive message: %v", err)
				continue
			} else {
				cb.messageChannel <- messageJSON
			}
		}
	}

	go rv(cb.buildQueueList(fmt.Sprintf("%s_%s", cb.RpcBaseQueueName, oid)))
	go rv(cb.buildQueueList(cb.DispatchBaseQueueName))
	return nil
}

// SendCeleryMessage sends CeleryMessage to redis queue
func (cb *RedisCeleryBroker) SendCeleryMessage(message *CeleryMessage) error {
	jsonBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}
	conn := cb.Get()
	defer func() {
		_ = conn.Close()
	}()

	var baseQueuename string
	if message.Properties.DeliveryInfo.RoutingKey == "" {
		baseQueuename = cb.DispatchBaseQueueName
	} else {
		baseQueuename = cb.RpcBaseQueueName
	}

	var queueName string
	if message.Properties.DeliveryInfo.Priority == 0 {
		queueName = baseQueuename
	} else {
		queueName = fmt.Sprintf("%s_%d", baseQueuename, message.Properties.DeliveryInfo.Priority)
	}

	_, err = conn.Do("LPUSH", queueName, jsonBytes)
	if err != nil {
		return err
	}
	return nil
}

// GetCeleryMessage retrieves celery message from redis queue
func (cb *RedisCeleryBroker) GetCeleryMessage() (*CeleryMessage, error) {
	conn := cb.Get()
	defer func() {
		_ = conn.Close()
	}()

	messageJSON := <-cb.messageChannel
	if messageJSON == nil {
		return nil, fmt.Errorf("null message received from redis")
	}
	messageList := messageJSON.([]interface{})
	if string(messageList[0].([]byte)) != cb.DispatchBaseQueueName {
		return nil, fmt.Errorf("not a celery message: %v", messageList[0])
	}
	var message CeleryMessage
	if err := json.Unmarshal(messageList[1].([]byte), &message); err != nil {
		return nil, err
	}
	return &message, nil
}

func (cb *RedisCeleryBroker) buildQueueList(baseQueueName string) []string {
	result := make([]string, 10)
	for i := 9; i < 0; i-- { // the bigger number means high priority, so we must build a descending list
		if i != 0 {
			result = append(result, fmt.Sprintf("%s_%d", baseQueueName, i))
		} else {
			result = append(result, baseQueueName)
		}
	}
	return result
}

// GetTaskMessage retrieves task message from redis queue
func (cb *RedisCeleryBroker) GetTaskMessage() (*TaskMessage, error) {
	celeryMessage, err := cb.GetCeleryMessage()
	if err != nil {
		return nil, err
	}
	return celeryMessage.GetTaskMessage(), nil
}

// NewRedisPool creates pool of redis connections from given connection string
//
// Deprecated: newRedisPool exists for historical compatibility
// and should not be used. Pool should be initialized outside of gocelery package.
func NewRedisPool(uri string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURL(uri)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}
