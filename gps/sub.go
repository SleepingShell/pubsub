package gps

import (
	"context"

	//"github.com/sleepingshell/pubsub"
	pb "github.com/sleepingshell/pubsub/proto"
	"google.golang.org/grpc"
)

//ReceivedHandler will be called everytime a message is received
type ReceivedHandler func(topic string, value []byte)

//GClient is a subscriber that connects to the publisher via GRPC
type GClient struct {
	c   pb.RouterClient
	cfs []context.CancelFunc
}

//Connect connects to a GRPC publisher
func (g *GClient) Connect(host string) error {
	conn, err := grpc.Dial(host, grpc.WithInsecure())
	if err != nil {
		return err
	}

	g.c = pb.NewRouterClient(conn)
	return nil
}

//Close will send the closing signal to all contexts
func (g *GClient) Close() {
	for _, cf := range g.cfs {
		cf()
	}
}

//Subscribe will subscribe to a publisher over GRPC
func (g *GClient) Subscribe(handler ReceivedHandler, topics ...string) {
	ctx, cancel := context.WithCancel(context.Background())
	g.cfs = append(g.cfs, cancel)

	go subscribe(ctx, handler, g.c, topics...)
}

//Call the GRPC subscriber endpoint and call handler when a msg is received
func subscribe(ctx context.Context, handler ReceivedHandler, c pb.RouterClient, topics ...string) error {
	stream, err := c.Subscribe(context.Background(), &pb.SubRequest{
		Topics: topics,
	})
	if err != nil {
		return err
	}

	receive := make(chan pb.Reply, 1)
	fail := make(chan error, 1)

	go func() {
		for {
			r, err := stream.Recv()
			if err != nil {
				fail <- err
				return
			}
			receive <- *r
		}
	}()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case err := <-fail:
			return err
		case <-ctx.Done():
			return stream.CloseSend()
		case reply := <-receive:
			topic := reply.GetTopic()
			value := reply.GetValue()
			handler(topic, value)
		}
	}
}
