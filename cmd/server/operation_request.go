package server

import (
	"errors"
	"strings"
)

// OperationType defines the naming convention for database operations
type OperationType int

const (
	UNSUPPORTED OperationType = iota
	GET
	SET
	DONE
)

// String method for the OperationType
func (ot OperationType) String() string {
	// Defines the operation sent in a request payload.
	return []string{"UNSUPPORTED", "GET", "SET", "DONE"}[ot]
}

// Parse a raw operation string to a corresponding OperationType. Returns an error if the
// rawOperationType is an invalid operation type.
func Parse(rawOperationType string) (OperationType, error) {
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

// OperationRequest defines the components of a request to the server.
type OperationRequest struct {
	operationType OperationType
	key           string
	value         string
}

// ConstructOperationRequest builds an OperationRequest struct from an unparsed client request. The
// request is expected as a space delimetered strings with the first word being an OperationType.
// If OperationType is GET or SET, then a second parameter is the key. If it is a SET, then the
// third parameter is the value to associate with the key. Returns an error if an unsupported
// OperationRequest is provided as the first parameter, or if the number and of parameters does not
// reflect that required (as described above).
func ConstructOperationRequest(request string) (OperationRequest, error) {
	requestParts := strings.Split(strings.TrimSuffix(request, "\n"), " ")
	operationType, err := Parse(requestParts[0])
	if err != nil {
		return OperationRequest{}, err
	}

	if requestParts[1] == "" {
		return OperationRequest{}, errors.New("no key provided")
	}

	var value string
	if SET.String() == operationType.String() {
		value = requestParts[2]
	}
	return OperationRequest{
		operationType: operationType,
		key:           requestParts[1],
		value:         value}, nil
}
