package main

import (
	"bufio"
	"errors"
	"fmt"
)

type clientHandler struct {
	delimeter byte
	channel   chan int
	table     SimpleDB
	reader    *bufio.Reader
	writer    *bufio.Writer
}

func (c *clientHandler) handle() {
	continueRunning := true
	for continueRunning {
		message, _ := c.reader.ReadString(c.delimeter)
		o, err := constructOperationRequest(message)
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

func (c *clientHandler) closeClientHandler() {
	c.channel <- 1
}

func (c *clientHandler) handleGet(request OperationRequest) (string, error) {
	fmt.Println(request.operationType.String(), request.key, request.value)
	value, err := c.table.get(request.key)
	return value, err
}

func (c *clientHandler) handleSet(request OperationRequest) error {
	fmt.Println(request.operationType.String(), request.key, request.value)
	if c.table.set(request.key, request.value) {
		return errors.New("failed to SET request into table")
	}
	return nil
}

func (c *clientHandler) handleResponse(response string) {
	if _, err := c.writer.WriteString(response); err != nil {
		fmt.Printf("error occurred sending response to client, %s\n", err)
	}
	if c.writer.Flush() != nil {
		fmt.Println("Could not flush output to client")
	}
}
