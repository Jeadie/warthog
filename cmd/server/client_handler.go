package server

import (
	"bufio"
	"errors"
	"fmt"
	"warthog/pkg/db"
)

// TODO: Create manager for the backend database to Run parallel calls.

// ClientHandler encapsulates fields needed for the server to communicate with a single client.
type ClientHandler struct {
	delimiter byte     // delimiter demarcating a single request from the client.
	channel   chan int // Channel back to server accepting routine. Currently not used.
	table     db.Database
	reader    *bufio.Reader // stream to read client communications from
	writer    *bufio.Writer // stream to respond to client.
}

// Run requests from a single client until they have sent a DONE OperationType.
func (c *ClientHandler) Run() {
	continueRunning := true
	for continueRunning {
		message, _ := c.reader.ReadString(c.delimiter)
		o, err := ConstructOperationRequest(message)
		if err != nil {
			break
		}

		response := ""
		switch o.operationType {
		case GET:
			response, err = c.handleGet(o)
		case SET:
			err = c.handleSet(o)
		case DONE:
			continueRunning = false
		case UNSUPPORTED:
			fmt.Println("unsupported operation requested by client")
		}
		if err != nil {
			fmt.Printf("operation %s failed. Error: %s\n", o.operationType.String(), err)
		}
		if len(response) > 0 {
			c.handleResponse(response)
		}
	}
	c.closeClientHandler()
}

// closeClientHandler wraps all logic in handling the end of communication with the client.
func (c *ClientHandler) closeClientHandler() {
	fmt.Println("Finished communication with client...")
}

// handleGet performs a GET operation to the provided database. Returns an error if propagated from
// the database and the value from the database.
func (c *ClientHandler) handleGet(request OperationRequest) (string, error) {
	value, err := c.table.Get(request.key)
	return value, err
}

// handleSet performs a SET operation to the provided database. Returns an error if propagated
// from the database.
func (c *ClientHandler) handleSet(request OperationRequest) error {
	fmt.Println(request.operationType.String(), request.key, request.value)
	if ok := c.table.Set(request.key, request.value); !ok {
		return errors.New("failed to SET request into table")
	}
	return nil
}

// handleResponse sends a response to the client.
func (c *ClientHandler) handleResponse(response string) {
	if _, err := c.writer.WriteString(response); err != nil {
		fmt.Printf("error occurred sending response to client, %s\n", err)
	}
	if c.writer.Flush() != nil {
		fmt.Println("Could not flush output to client")
	}
}
