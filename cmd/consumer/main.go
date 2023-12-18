package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"time"
)

const MaxCollectionSize = 10

func clearTerminal() {
	osName := runtime.GOOS

	var cmd *exec.Cmd

	if osName == "windows" {
		cmd = exec.Command("cmd", "/c", "cls")
	} else {
		cmd = exec.Command("clear")
	}

	cmd.Stdout = os.Stdout

	err := cmd.Run()
	if err != nil {
		log.Println(err)
	}
}

type Tx struct {
	TxData  []byte
	TxPrice int
}

type TxLimitedCollection struct {
	TxCollection []Tx
	MaxSize      int
}

func (t *TxLimitedCollection) Append(tx Tx) bool {
	if len(t.TxCollection) < t.MaxSize {
		t.TxCollection = append(t.TxCollection, tx)

		if len(t.TxCollection) == MaxCollectionSize {
			t.SortDes()
			return true
		}

		return false
	}

	if tx.TxPrice > t.TxCollection[MaxCollectionSize-1].TxPrice {
		t.TxCollection[MaxCollectionSize-1] = tx
	} else {
		return false
	}

	t.SortDes()

	return true
}

func (t *TxLimitedCollection) PrintCollection() {
	clearTerminal()

	for i := 0; i < MaxCollectionSize; i++ {
		log.Println(fmt.Sprintf("TX NUMBER %d: \n%s\n\n", i, string(t.TxCollection[i].TxData)))
	}
	log.Println("-----------------------------------------------------")

	time.Sleep(time.Second * 10)
}

func (t *TxLimitedCollection) SortDes() {
	sort.Slice(t.TxCollection, func(i, j int) bool {
		return t.TxCollection[i].TxPrice > t.TxCollection[j].TxPrice
	})
}

type PriceData struct {
	Data struct {
		Price int `json:"price"`
	} `json:"data"`
}

func main() {
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9093", "bitstamp-data", 0)
	if err != nil {
		log.Fatal(err)
	}
	txList := &TxLimitedCollection{MaxSize: MaxCollectionSize}

	log.Println("Getting transactions, please wait: ")

	for {
		message, err := conn.ReadMessage(400)
		if err != nil {
			time.Sleep(time.Second * 5)
		} else {
			var priceData PriceData
			if err := json.Unmarshal(message.Value, &priceData); err != nil {
				log.Println(err)
				continue
			}

			ok := txList.Append(Tx{
				TxData:  message.Value,
				TxPrice: priceData.Data.Price,
			})

			if ok {
				txList.PrintCollection()
			}

		}
	}
}
