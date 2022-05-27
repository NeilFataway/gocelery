package gocelery

import uuid "github.com/satori/go.uuid"

var nodeID string

func init() {
	nodeID = generateUuid()
}

func generateUuid() string {
	id, _ := uuid.NewV1()
	return id.String()
}

func getNodeId() string {
	return nodeID
}
