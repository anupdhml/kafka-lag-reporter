package main

import (
	"fmt"
	"os"

	"github.com/Shopify/sarama"
)

func failf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func main() {
	//fmt.Println("Hello, world")

	//bootstrapBrokers := "kafkac1n1.dev.bo1.csnzoo.com:9092,kafkac1n2.dev.bo1.csnzoo.com:9092,kafkac1n3.dev.bo1.csnzoo.com:9092"
	bootstrapBrokers := []string{
		"kafkac1n1.dev.bo1.csnzoo.com:9092",
		"kafkac1n2.dev.bo1.csnzoo.com:9092",
		"kafkac1n3.dev.bo1.csnzoo.com:9092",
	}

	config := sarama.NewConfig()
	//config.Version = 'placeholder'
	//config.ClientID = 'wf_kafka_lag_exporter'

	client, err := sarama.NewClient(bootstrapBrokers, config)
	if err != nil {
		failf("failed to create client err=%v", err)
	}

	brokers := client.Brokers()
	fmt.Fprintf(os.Stderr, "found %v brokers\n", len(brokers))
}
