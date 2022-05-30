// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"fmt"
	"time"
)

// CeleryClient provides API for sending celery tasks
type CeleryClient struct {
	broker   CeleryBroker
	backend  CeleryBackend
	oid      string
	priority uint8
}

// CeleryBroker is interface for celery broker database
type CeleryBroker interface {
	Init(string) error
	SendCeleryMessage(*CeleryMessage) error
	GetCeleryMessage() (*CeleryMessage, error) // must be non-blocking
}

// CeleryBackend is interface for celery backend database
type CeleryBackend interface {
	Init(string) error
	GetResult(string) (*ResultMessage, error) // must be non-blocking
	SetResult(taskID string, result *ResultMessage) error
}

// NewCeleryClient creates new celery client
func NewCeleryClient(broker CeleryBroker, backend CeleryBackend) *CeleryClient {
	return &CeleryClient{
		broker,
		backend,
		getNodeId(),
		0,
	}
}

// Delay gets asynchronous result
func (cc *CeleryClient) Init() error {
	return cc.backend.Init(cc.oid)
}

// Delay gets asynchronous result
func (cc *CeleryClient) Delay(task string, args ...interface{}) (*AsyncResult, error) {
	celeryTask := getTaskMessage(task)
	celeryTask.Args = args
	return cc.delay(celeryTask)
}

// Call route the task to a specified worker, and gets asynchronous result
func (cc *CeleryClient) Call(task, routingKey string, args ...interface{}) (*AsyncResult, error) {
	celeryTask := getTaskMessage(task)
	celeryTask.Args = args
	return cc.call(celeryTask, routingKey)
}

// DelayKwargs gets asynchronous results with argument map
func (cc *CeleryClient) DelayKwargs(task string, args map[string]interface{}) (*AsyncResult, error) {
	celeryTask := getTaskMessage(task)
	celeryTask.Kwargs = args
	return cc.delay(celeryTask)
}

// CallKwargs route the task to a specified worker, and gets asynchronous result with argument map
func (cc *CeleryClient) CallKwargs(task, routingKey string, args map[string]interface{}) (*AsyncResult, error) {
	celeryTask := getTaskMessage(task)
	celeryTask.Kwargs = args
	return cc.call(celeryTask, routingKey)
}

func (cc *CeleryClient) delay(task *TaskMessage) (*AsyncResult, error) {
	return cc.call(task, "")
}

func (cc *CeleryClient) call(task *TaskMessage, routingKey string) (*AsyncResult, error) {
	celeryMessage, err := cc.getCeleryMessage(task, routingKey)
	defer releaseCeleryMessage(celeryMessage)
	err = cc.broker.SendCeleryMessage(celeryMessage)
	if err != nil {
		return nil, err
	}
	return &AsyncResult{
		TaskID:  task.ID,
		backend: cc.backend,
	}, nil
}

func (cc *CeleryClient) getCeleryMessage(task *TaskMessage, routingKey string) (*CeleryMessage, error) {
	defer releaseTaskMessage(task)
	encodedMessage, err := task.Encode()
	if err != nil {
		return nil, err
	}
	celeryMessage := getCeleryMessage(encodedMessage)
	celeryMessage.Properties.CorrelationID = task.ID
	celeryMessage.Properties.ReplyTo = cc.oid
	celeryMessage.Properties.DeliveryInfo.RoutingKey = routingKey
	celeryMessage.Properties.DeliveryInfo.Priority = cc.priority

	return celeryMessage, nil
}

// CeleryTask is an interface that represents actual task
// Passing CeleryTask interface instead of function pointer
// avoids reflection and may have performance gain.
// ResultMessage must be obtained using GetResultMessage()
type CeleryTask interface {

	// ParseKwargs - define a method to parse kwargs
	ParseKwargs(map[string]interface{}) error

	// RunTask - define a method for execution
	RunTask() (interface{}, error)
}

// AsyncResult represents pending result
type AsyncResult struct {
	TaskID  string
	backend CeleryBackend
	result  *ResultMessage
}

// Get gets actual result from backend
// It blocks for period of time set by timeout and returns error if unavailable
func (ar *AsyncResult) Get(timeout time.Duration) (interface{}, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	timeoutChan := time.After(timeout)
	for {
		select {
		case <-timeoutChan:
			err := fmt.Errorf("%v timeout getting result for %s", timeout, ar.TaskID)
			return nil, err
		case <-ticker.C:
			val, err := ar.AsyncGet()
			if err != nil {
				continue
			}
			return val, nil
		}
	}
}

// AsyncGet gets actual result from backend and returns nil if not available
func (ar *AsyncResult) AsyncGet() (interface{}, error) {
	if ar.result != nil {
		return ar.result.Result, nil
	}
	val, err := ar.backend.GetResult(ar.TaskID)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, err
	}
	if val.Status != "SUCCESS" {
		return nil, fmt.Errorf("error response status %v", val)
	}
	ar.result = val
	return val.Result, nil
}

// Ready checks if actual result is ready
func (ar *AsyncResult) Ready() (bool, error) {
	if ar.result != nil {
		return true, nil
	}
	val, err := ar.backend.GetResult(ar.TaskID)
	if err != nil {
		return false, err
	}
	ar.result = val
	return val != nil, nil
}
