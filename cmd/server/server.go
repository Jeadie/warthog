package server

import (
	"bufio"
	"fmt"
	"warthog/pkg/db"
)
import "net"

func Run(config *Config) {

	table := db.ConstructLogStreamTable(100)

	ln, _ := net.Listen(config.Network, fmt.Sprintf(":%d", config.Port))
	for {
		conn, _ := ln.Accept()
		fmt.Println("Received a connection")
		c := make(chan int)
		handler := ClientHandler{
			delimiter: config.Delimiter,
			channel:   c,
			table:     table,
			reader:    bufio.NewReader(conn),
			writer:    bufio.NewWriter(conn)}
		go handler.Run()
	}
}
