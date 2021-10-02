package main

import (
	"warthog/cmd/server"
)

func main() {
	server.Run(&server.Config{
		Delimiter: '\n',
		Port: 8000,
		Network: "tcp",
	})
}

