package main

import (
	"bufio"
	"fmt"
)
import "net"

func main() {
	delimeter := byte('\n')
	table := ConstructLogStreamTable(100)

	ln, _ := net.Listen("tcp", ":8000")
	fmt.Println("listening to unix port 8000")
	for {
		conn, _ := ln.Accept()
		fmt.Println("Received a connection")
		c := make(chan int)
		handler := ClientHandler{
			delimiter: delimeter,
			channel:   c,
			table:     table,
			reader:    bufio.NewReader(conn),
			writer:    bufio.NewWriter(conn)}
		go handler.Handle()
	}
}
