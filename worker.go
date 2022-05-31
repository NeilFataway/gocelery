// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"
)

// CeleryWorker represents distributed task worker
type CeleryWorker struct {
	broker          CeleryBroker
	backend         CeleryBackend
	workerSpec      CeleryWorkerSpec
	cancel          context.CancelFunc
	workWG          sync.WaitGroup
	rateLimitPeriod time.Duration
}

type CeleryWorkerSpec struct {
	numWorkers      int
	oid             string
	registeredTasks map[string]interface{}
	taskLock        sync.RWMutex
}

func (w *CeleryWorker) SetOID(oid string) {
	w.workerSpec.taskLock.Lock()
	defer w.workerSpec.taskLock.Unlock()
	w.workerSpec.oid = oid
}

// NewCeleryWorker returns new celery worker
func NewCeleryWorker(broker CeleryBroker, backend CeleryBackend, numWorkers int) *CeleryWorker {
	return &CeleryWorker{
		broker:  broker,
		backend: backend,
		workerSpec: CeleryWorkerSpec{
			numWorkers:      numWorkers,
			registeredTasks: map[string]interface{}{},
			oid:             getNodeId(),
		},
		rateLimitPeriod: 100 * time.Millisecond,
	}
}

func (w *CeleryWorker) Init() error {
	return w.broker.Init(w.workerSpec.oid)
}

// StartWorkerWithContext starts celery worker(s) with given parent context
func (w *CeleryWorker) StartWorkerWithContext(ctx context.Context) {
	var wctx context.Context
	wctx, w.cancel = context.WithCancel(ctx)
	w.workWG.Add(w.workerSpec.numWorkers)
	for i := 0; i < w.workerSpec.numWorkers; i++ {
		go func(workerID int) {
			defer w.workWG.Done()
			ticker := time.NewTicker(w.rateLimitPeriod)
			for {
				select {
				case <-wctx.Done():
					return
				case <-ticker.C:
					w.RunOnce()
				}
			}
		}(i)
	}
}

func (w *CeleryWorker) RunOnce() {
	// process task request
	celeryMessage, err := w.broker.GetCeleryMessage()
	defer releaseCeleryMessage(celeryMessage)
	if err != nil || celeryMessage == nil {
		return
	}

	taskMessage := celeryMessage.GetTaskMessage()
	if taskMessage == nil {
		return
	}

	// run task
	resultMsg, err := w.RunTask(taskMessage)
	if err != nil {
		log.Printf("failed to run task message %s: %+v", taskMessage.ID, err)
		return
	}
	defer releaseResultMessage(resultMsg)

	// push result to backend
	if w.backend == nil {
		// No result will be returned if no backend configured.
		return
	}

	err = w.backend.SetResult(celeryMessage.Properties.CorrelationID,
		resultMsg)
	if err != nil {
		log.Printf("failed to push result: %+v", err)
		return
	}
}

// StartWorker starts celery workers
func (w *CeleryWorker) StartWorker() {
	w.StartWorkerWithContext(context.Background())
}

// StopWorker stops celery workers
func (w *CeleryWorker) StopWorker() {
	w.cancel()
	w.workWG.Wait()
}

// StopWait waits for celery workers to terminate
func (w *CeleryWorker) StopWait() {
	w.workWG.Wait()
}

// GetNumWorkers returns number of currently running workers
func (w *CeleryWorker) GetNumWorkers() int {
	return w.workerSpec.numWorkers
}

// Register registers tasks (functions)
func (w *CeleryWorker) Register(name string, task interface{}) {
	w.workerSpec.taskLock.Lock()
	defer w.workerSpec.taskLock.Unlock()
	w.workerSpec.registeredTasks[name] = task
}

// GetTask retrieves registered task
func (w *CeleryWorker) GetTask(name string) interface{} {
	w.workerSpec.taskLock.RLock()
	task, ok := w.workerSpec.registeredTasks[name]
	if !ok {
		w.workerSpec.taskLock.RUnlock()
		return nil
	}
	w.workerSpec.taskLock.RUnlock()
	return task
}

// RunTask runs celery task
func (w *CeleryWorker) RunTask(message *TaskMessage) (*ResultMessage, error) {

	// ignore if the message is expired
	if message.Expires != nil && message.Expires.UTC().Before(time.Now().UTC()) {
		return nil, fmt.Errorf("task %s is expired on %s", message.ID, message.Expires)
	}

	// check for malformed task message - args cannot be nil
	if message.Args == nil {
		return nil, fmt.Errorf("task %s is malformed - args cannot be nil", message.ID)
	}

	// get task
	task := w.GetTask(message.Task)
	if task == nil {
		return nil, fmt.Errorf("task %s is not registered", message.Task)
	}

	// convert to task interface
	taskInterface, ok := task.(CeleryTask)
	if ok {
		if err := taskInterface.ParseKwargs(message.Kwargs); err != nil {
			return nil, err
		}
		val, err := taskInterface.RunTask()
		if err != nil {
			return nil, err
		}
		return getResultMessage(val), err
	}

	// use reflection to execute function ptr
	taskFunc := reflect.ValueOf(task)
	return runTaskFunc(&taskFunc, message)
}

func runTaskFunc(taskFunc *reflect.Value, message *TaskMessage) (*ResultMessage, error) {

	// check number of arguments
	numArgs := taskFunc.Type().NumIn()
	messageNumArgs := len(message.Args)
	if numArgs != messageNumArgs {
		return nil, fmt.Errorf("number of task arguments %d does not match number of message arguments %d", numArgs, messageNumArgs)
	}

	// construct arguments
	in := make([]reflect.Value, messageNumArgs)
	for i, arg := range message.Args {
		origType := taskFunc.Type().In(i).Kind()
		msgType := reflect.TypeOf(arg).Kind()
		// special case - convert float64 to int if applicable
		// this is due to json limitation where all numbers are converted to float64
		if origType == reflect.Int && msgType == reflect.Float64 {
			arg = int(arg.(float64))
		}
		if origType == reflect.Float32 && msgType == reflect.Float64 {
			arg = float32(arg.(float64))
		}

		in[i] = reflect.ValueOf(arg)
	}

	// call method
	res := taskFunc.Call(in)
	if len(res) == 0 {
		return nil, nil
	}

	return getReflectionResultMessage(&res[0]), nil
}
