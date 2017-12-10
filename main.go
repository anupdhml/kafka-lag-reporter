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

func fetchPartitions(client *sarama.Client, topic string) []int32 {
	partitions, err := (*client).Partitions(topic)
	if err != nil {
		failf("failed to read partitions for topic=%s err=%v", topic, err)
	}
	return partitions
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

	// required for broker centric requests
	// we are on 0.10.1.1. this is the closes sarama provides
	config.Version = sarama.V0_10_1_0
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

	coordinator, err := client.Coordinator(group)
	if err != nil {
		failf("failed to get coordinating broker for group=%s err=%v", group, err)
	}

	//r, err := (*coordinator).GetConsumerMetadata(&sarama.ConsumerMetadataRequest{
	//group,
	//})
	//fmt.Println(r, err)

	//r, err := (*coordinator).ListGroups(&sarama.ListGroupsRequest{})
	//fmt.Println(r, err)

	response, err := (*coordinator).DescribeGroups(&sarama.DescribeGroupsRequest{
		[]string{group},
		// other groups may have different coordinator
		//groups,
	})
	if err != nil {
		failf("failed to get group info for group=%s err=%v", group, err)
	}

	for _, groupInfo := range response.Groups {
		//err := (*groupInfo).Err
		state := (*groupInfo).State

		fmt.Println(state)

		for _, memberInfo := range (*groupInfo).Members {
			//fmt.Println(k)
			fmt.Println(memberInfo.ClientId, memberInfo.ClientHost)
			//fmt.Println(memberInfo.MemberMetadata, memberInfo.MemberAssignment)
		}
	}

	//os.Exit(0)

	groupTopics := []string{"moawsl_dev"}

	topicPartitions := map[string][]int32{}
	for _, topic := range groupTopics {
		partitions := fetchPartitions(&client, topic)
		fmt.Fprintf(os.Stderr, "found partitions=%v for topic=%v\n", partitions, topic)
		topicPartitions[topic] = partitions
	}
	//fmt.Println(topicPartitions)

	offsetManager, err := sarama.NewOffsetManagerFromClient(group, client)
	if err != nil {
		failf("failed to create offsetManager err=%v", err)
	}
	defer logClose("offset manager", offsetManager)

	for topic, partitions := range topicPartitions {
		for _, partition := range partitions {
			//fmt.Println(topic, partition)

			pom, err := offsetManager.ManagePartition(topic, partition)
			if err != nil {
				failf("failed to manage partition group=%s topic=%s partition=%d err=%v", group, topic, partition, err)
			}
			defer logClose("partition offset manager", pom)

			groupOffset, _ := pom.NextOffset()
			//fmt.Println(groupOffset)

			producerOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				failf("failed to get offset for topic=%s partition=%d err=%v", topic, partition, err)
			}
			//fmt.Println(producerOffset)

			lag := producerOffset - groupOffset
			//fmt.Println(lag)

			fmt.Println(group, topic, partition, groupOffset, producerOffset, lag)
		}
	}

}
