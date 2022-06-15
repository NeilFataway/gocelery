package gocelery

import (
	"fmt"
	uuid "github.com/satori/go.uuid"
	"os"
)

func generateUuid() string {
	id, _ := uuid.NewV1()
	return id.String()
}

func getNodeId() string {
	if hostName, err := os.Hostname(); err != nil {
		os.Getegid()
		return fmt.Sprintf("%s_%d", hostName, os.Getegid())
	}

	return generateUuid()
}
