package main

import (
	"bufio"
	"fmt"
)

type clientHandler struct {
	delimeter byte
	channel chan int
	table   SimpleDB
}

func (c *clientHandler) handle(reader *bufio.Reader, writer *bufio.Writer) {
	continueRunning:= true
	for continueRunning {
		message, err := reader.ReadString(c.delimeter)
		if err != nil {
			continueRunning = false
			break
		}
		o, err := constructOperationRequest(message)
		if err != nil {
			continueRunning = false
			break
		}

		var response string
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
			fmt.Printf("operation %s failed\n", o.operationType.String())
			continueRunning = false
		}
		if _, err := writer.WriteString(response); len(response) > 0 && err != nil {
			fmt.Println("error occurred sending response to client")
			continueRunning = false
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
	return c.table.set(request.key, request.value)
}
