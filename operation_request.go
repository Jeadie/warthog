package main

import (
	"errors"
	"fmt"
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
	requestParts := strings.Split(strings.TrimSuffix(request, "\n"), " ")
	operationType, err := parse(requestParts[0])
	if err != nil {
		return OperationRequest{}, err
	}

	if requestParts[1] == "" {
		return OperationRequest{}, errors.New("no key provided")
	}

	value := ""
	if SET.String() == operationType.String() {
		value = requestParts[2]
	}

	fmt.Println("Constructed a valid operation request")
	return OperationRequest{
		operationType: operationType,
		key:           requestParts[1],
		value:         value}, nil
}
