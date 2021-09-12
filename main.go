package main

import (
	"bufio"
	"fmt"
)
import "net"

func main() {
	delimeter := byte('\n')
	table := ConstructSimpleDB(100)
	fmt.Println("Constructed in-memory simply key-value table")
	ln, _ := net.Listen("tcp", ":8000")
	fmt.Println("listening to unix port 8000")
	for {
		conn, _ := ln.Accept()
		fmt.Println("Received a connection")
		c := make(chan int)
		handler := clientHandler{
			delimeter: delimeter,
			channel:   c,
			table:     table,
			reader:    bufio.NewReader(conn),
			writer:    bufio.NewWriter(conn)}
		go handler.handle()
	}
}
