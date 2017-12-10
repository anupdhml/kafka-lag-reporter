package main

import (
	"fmt"
	"io"
	"os"

	"github.com/Shopify/sarama"
)

func failf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func logClose(name string, c io.Closer) {
	if err := c.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to close %#v err=%v", name, err)
	}
}

func main() {
	//groups := []string{
	//"moawsl_weblog_elk",
	//"applog_elk",
	//"syslog_elk",
	//}

	//fmt.Println(groups)
	//os.Exit(0)

	//bootstrapBrokers := "kafkac1n1.dev.bo1.csnzoo.com:9092,kafkac1n2.dev.bo1.csnzoo.com:9092,kafkac1n3.dev.bo1.csnzoo.com:9092"
	bootstrapBrokers := []string{
		//"kafkac1n1.dev.bo1.csnzoo.com:9092",
		"kafkac1n2.dev.bo1.csnzoo.com:9092",
		"kafkac1n3.dev.bo1.csnzoo.com:9092",
	}

	config := sarama.NewConfig()
	//config.Version = 'placeholder'
	//config.ClientID = 'wf_kafka_lag_exporter'
	// set to false and try?
	//fmt.Println(config.Metadata.Full)

	client, err := sarama.NewClient(bootstrapBrokers, config)
	defer client.Close()

	if err != nil {
		failf("failed to create client err=%v", err)
	}

	brokers := client.Brokers()
	fmt.Fprintf(os.Stderr, "found %v brokers\n", len(brokers))

	//for _, group := range groups {
	//fmt.Println(group)
	//}

	group := "moawsl_weblog_elk"

	offsetManager, err := sarama.NewOffsetManagerFromClient(group, client)
	if err != nil {
		failf("failed to create offsetManager err=%v", err)
	}
	defer logClose("offset manager", offsetManager)

	topic := "moawsl_dev"
	partition := int32(0)

	pom, err := offsetManager.ManagePartition(topic, partition)
	if err != nil {
		failf("failed to manage partition group=%s topic=%s partition=%d err=%v", group, topic, partition, err)
	}
	defer logClose("partition offset manager", pom)

	groupOffset, _ := pom.NextOffset()
	fmt.Println(groupOffset)

	producerOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		failf("failed to get offset for topic=%s partition=%d err=%v", topic, partition, err)
	}
	fmt.Println(producerOffset)

	lag := producerOffset - groupOffset
	fmt.Println(lag)
}
