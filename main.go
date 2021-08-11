package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/sirupsen/logrus"
)

func main() {
	r, ok := os.LookupEnv("RPC")
	if !ok {
		r = "http://127.0.0.1:8545"
	}
	logrus.Infoln("Streaming started")

	client, err := NewSafeClient(r)
	if err != nil {
		logrus.Infoln("cannot connect to geth :%s err=%s", r, err)
		return
	}

	if err != nil {
		logrus.Error("Stream:", err.Error())
	}
	headers := make(chan *types.Header)
	sub, err := client.SubscribeNewHead(context.Background(), headers)
	if err != nil {
		logrus.Error("Stream:", err.Error())
	}
	for {
		select {
		case err := <-sub.Err():
			logrus.Error("Stream:", err.Error())
			continue
		case header := <-headers:
			block, err := client.BlockByHash(context.Background(), header.Hash())
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(block.Hash().Hex())        // 0xbc10defa8dda384c96a17640d84de5578804945d347072e091b4e5f390ddea7f
			fmt.Println(block.Number().Uint64())   // 3477413
			fmt.Println(block.Time())              // 1529525947
			fmt.Println(block.Nonce())             // 130524141876765836
			fmt.Println(len(block.Transactions())) // 7
		}
	}
}
