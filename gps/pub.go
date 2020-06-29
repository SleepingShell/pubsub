package gps

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	pb "github.com/sleepingshell/pubsub/proto"
	"google.golang.org/grpc"
)

//GPublisher is a GRPC pubslisher
type GPublisher struct {
	//TODO: Add labels to a topic
	subscribers map[string][]*subscriber
	subMU       sync.Mutex
	grpcS       *grpc.Server
}

type topicValue struct {
	topic string
	value []byte
}

type subscriber struct {
	ctx    context.Context
	cancel context.CancelFunc
	inbox  chan topicValue
}

//Publish will publish the given item to the topic
func (s *GPublisher) Publish(topic string, item interface{}) error {
	s.subMU.Lock()
	subs, ok := s.subscribers[topic]
	s.subMU.Unlock()
	if !ok {
		return errors.New("No one subscribed")
	}

	for _, sub := range subs {
		var buf bytes.Buffer
		gob.NewEncoder(&buf).Encode(item)
		sub.inbox <- topicValue{topic: topic, value: buf.Bytes()}
	}
	return nil
}

//AddSubscriber will add a subscriber to the given topics
func (s *GPublisher) AddSubscriber(sub *subscriber, topics ...string) {
	if len(topics) < 1 {
		return
	}

	s.subMU.Lock()
	for _, topic := range topics {
		if topic == "" {
			continue
		}
		s.subscribers[topic] = append(s.subscribers[topic], sub)
	}
	s.subMU.Unlock()
}

//NumSubscribed returns the number of subscribers to a given topic
func (s *GPublisher) NumSubscribed(topic string) int {
	return len(s.subscribers[topic])
}

//Run will start the GRPC listener
func (s *GPublisher) Run(port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}

	pb.RegisterRouterServer(s.grpcS, s)
	go s.grpcS.Serve(lis)
	return nil
}

//Subscribe implements the GRPC subscribe method and runs as a subscriber
func (s *GPublisher) Subscribe(req *pb.SubRequest, server pb.Router_SubscribeServer) error {
	ctx, cancel := context.WithCancel(server.Context())
	inbox := make(chan topicValue, 100)

	subscriber := subscriber{
		ctx:    ctx,
		cancel: cancel,
		inbox:  inbox,
	}

	topics := req.GetTopics()
	s.AddSubscriber(&subscriber, topics...)

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-inbox:
			server.Send(&pb.Reply{
				Topic: msg.topic,
				Value: msg.value,
			})
		}
	}
}

//Teardown will clear out the state and stop the grpc service
func (s *GPublisher) Teardown() {
	s.Reset()
	s.grpcS.Stop()
}

//Reset will stop all subscribers and then clear out the subscribers state
func (s *GPublisher) Reset() {
	for _, subs := range s.subscribers {
		for _, sub := range subs {
			sub.cancel()
		}
	}

	time.Sleep(1 * time.Second)
	s.subMU.Lock()
	defer s.subMU.Unlock()
	s.subscribers = make(map[string][]*subscriber)
}

//NewServer returns a new GPublisher
func NewServer() *GPublisher {
	var opts []grpc.ServerOption
	return &GPublisher{
		subscribers: make(map[string][]*subscriber),
		grpcS:       grpc.NewServer(opts...),
	}
}
