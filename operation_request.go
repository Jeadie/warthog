package main

import (
	"errors"
	"strings"
)

type OperationType int

const (
	UNSUPPORTED OperationType = iota
	GET
	SET
	DONE
)

func (ot OperationType) String() string {
	// Defines the operation sent in a request payload.
	return []string{"UNSUPPORTED", "GET", "SET", "DONE"}[ot]
}

func parse(rawOperationType string) (OperationType, error) {
	switch rawOperationType {
	case "GET":
		return OperationType(1), nil
	case "SET":
		return OperationType(2), nil
	case "DONE":
		return OperationType(3), nil
	}
	return OperationType(0), errors.New("Unknown operation type provided")
}

type OperationRequest struct {
	operationType OperationType
	key           string
	value         string
}

func constructOperationRequest(request string) (OperationRequest, error) {
	requestParts := strings.SplitN(request, " ", 3)
	operationType, err := parse(requestParts[0])
	if err != nil {
		return OperationRequest{}, err
	}
	if requestParts[1] == "" {
		return OperationRequest{}, errors.New("no key provided")
	}
	return OperationRequest{
		operationType: operationType,
		key:           requestParts[1],
		value:         requestParts[2]}, nil
}
