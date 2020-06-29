package pubsub

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	gps "github.com/sleepingshell/pubsub/gps"
)

func TestEcho(t *testing.T) {
	srv := gps.NewServer()
	err := srv.Run(5555)
	if err != nil {
		t.Error(err)
	}

	var client gps.EchoClient
	err = client.Connect("127.0.0.1:5555")
	if err != nil {
		t.Error(err)
	}

	go client.EchoTopics("testTopic")
	time.Sleep(1 * time.Second)

	err = srv.Publish("testTopic", "yo")
	if err != nil {
		//t.Error(err)
		t.Log(err)
	}

	srv.Teardown()
}

func TestMultipleClients(t *testing.T) {
	srv := gps.NewServer()
	err := srv.Run(5555)
	if err != nil {
		t.Error(err)
	}

	NUM_CLIENTS := 20
	NUM_MESSAGES := 100

	topics := []string{"topicA", "topicB", "topicC"}
	topicCount := make(map[string]int)
	var countMu sync.Mutex

	add := func(topic string, val []byte) {
		countMu.Lock()
		defer countMu.Unlock()
		topicCount[topic]++
	}

	var clients []gps.GClient
	for i := 0; i < NUM_CLIENTS; i++ {
		numTopics := rand.Intn(3)
		var client gps.GClient
		err = client.Connect("127.0.0.1:5555")
		if err != nil {
			t.Error(err)
		}

		clients = append(clients, client)
		go client.Subscribe(add, topics[0:numTopics]...)
	}

	time.Sleep(500 * time.Millisecond)

	n := 0
	for i := 0; i < NUM_MESSAGES; i++ {
		err = srv.Publish(topics[rand.Intn(3)], "Test"+strconv.Itoa(i))
		if err != nil {
			n++
		}
	}

	time.Sleep(2 * time.Second)
	for _, client := range clients {
		client.Close()
	}

	for _, topic := range topics {
		t.Log(topic+":", srv.NumSubscribed(topic))
	}
	t.Log(topicCount)
	t.Log("Num times received no one subscribed", n)

	srv.Teardown()
}

func BenchmarkSingleTopic(b *testing.B) {
	srv := gps.NewServer()
	err := srv.Run(5555)
	if err != nil {
		b.Error(err)
	}

	NUM_CLIENTS := 20
	var wg sync.WaitGroup

	add := func(topic string, val []byte) {
		wg.Done()
	}

	var clients []gps.GClient
	for i := 0; i < NUM_CLIENTS; i++ {
		var client gps.GClient
		err = client.Connect("127.0.0.1:5555")
		if err != nil {
			b.Error(err)
		}

		clients = append(clients, client)
		go client.Subscribe(add, "topic")
	}

	time.Sleep(1 * time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg = sync.WaitGroup{}
		wg.Add(NUM_CLIENTS)

		err = srv.Publish("topic", "t")
		if err != nil {
			b.Error(err)
		}
		wg.Wait()
	}

	b.StopTimer()

	for _, client := range clients {
		client.Close()
	}
	srv.Teardown()
}
