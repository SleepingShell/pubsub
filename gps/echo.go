package gps

import "fmt"

//EchoClient is grpc client that echos all received messages
type EchoClient struct {
	GClient
}

func (e *EchoClient) EchoTopics(topics ...string) {
	e.Subscribe(func(t string, i []byte) {
		fmt.Printf("%s: %s\n", t, string(i))
	}, topics...)
}
