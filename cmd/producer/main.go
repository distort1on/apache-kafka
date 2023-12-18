package main

import (
	"context"
	"flag"
	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
	"os/signal"
)

type Connection struct {
	bitstampConn *websocket.Conn
	kafkaConn    *kafka.Conn
	readSignal   chan struct{}
}

func (c *Connection) startReadMessages() {
	defer close(c.readSignal)

	//var data struct {
	//	Data json.RawMessage `json:"data"`
	//}

	for {
		select {
		case <-c.readSignal:
			return
		default:
			_, message, err := c.bitstampConn.ReadMessage()
			if err != nil {
				log.Println("read:", err)
			}
			log.Printf("recv: %s\n\n", string(message))

			err = c.sendDataToKafka(message)
			if err != nil {
				log.Println(err)
			}
		}
	}
}

func (c *Connection) stopReadMessages() {
	close(c.readSignal)
}

func (c *Connection) closeConnection() {
	err := c.bitstampConn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	if err != nil {
		log.Println("write close:", err)
		return
	}

	err = c.bitstampConn.Close()
	if err != nil {
		return
	}
}

func (c *Connection) sendDataToKafka(data []byte) error {
	_, err := c.kafkaConn.WriteMessages(kafka.Message{
		Value: data,
	})
	if err != nil {
		return err
	}

	return nil
}

func NewConnection(url string) (*Connection, error) {
	bC, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return nil, err
	}

	kC, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9093", "bitstamp-data", 0)
	if err != nil {
		return nil, err
	}

	return &Connection{bitstampConn: bC, kafkaConn: kC}, nil
}

func main() {
	flag.Parse()
	log.SetFlags(0)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	conn, err := NewConnection("wss://ws.bitstamp.net")
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer func() {
		log.Println("stopped")
		conn.closeConnection()
	}()

	// subscribe
	data := map[string]interface{}{
		"event": "bts:subscribe",
		"data": map[string]interface{}{
			"channel": "live_trades_btcusd",
		},
	}

	err = conn.bitstampConn.WriteJSON(data)
	if err != nil {
		log.Fatal(err)
	} else {
		_, message, err := conn.bitstampConn.ReadMessage()
		if err != nil {
			log.Println("read:", err)
		}
		log.Printf("recv: %s", string(message))
	}

	go conn.startReadMessages()

	select {
	case <-interrupt:
	}
}
