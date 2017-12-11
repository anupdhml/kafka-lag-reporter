package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Shopify/sarama"
)

type partitionOwner struct {
	Id     string
	HostIp string
	// TODO implement reverse DNS lookup for this field
	//HostName string
}

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
	// TODO separate all this stuff out into separate functions

	groupsArg := flag.String("groups", "", "Consumer groups we wish to report on")
	bootstrapServersArg := flag.String("bootstrap-servers", "localhost:9092", "Kafka broker(s) to bootstrap the connection")
	//fmt.Println(*groupsArg, *bootstrapServersArg)

	flag.Parse()

	if *groupsArg == "" {
		fmt.Fprintf(os.Stderr, "No consumer groups specified. Exiting...\n")
		os.Exit(1)
	}

	groups := strings.Split(*groupsArg, ",")
	bootstrapServers := strings.Split(*bootstrapServersArg, ",")
	//fmt.Println(groups, bootstrapServers)

	// -----------------------------------------------------------------------

	config := sarama.NewConfig()

	// required for broker centric requests
	// we are on 0.10.1.1. this is the closes sarama provides
	config.Version = sarama.V0_10_1_0

	//config.ClientID = 'wf_kafka_lag_exporter'

	// TODO try with false here. Default is true
	//config.Metadata.Full = false

	client, err := sarama.NewClient(bootstrapServers, config)
	if err != nil {
		failf("failed to create client err=%v", err)
	}
	defer client.Close()

	//brokers := client.Brokers()
	//fmt.Fprintf(os.Stderr, "found %v brokers\n", len(brokers))

	// -----------------------------------------------------------------------

	// TODO implement for multiple groups
	group := groups[0]
	//for _, group := range groups {
	//fmt.Println(group)
	//}

	// implement for all groups in the cluster maybe
	//response, err := (*coordinator).ListGroups(&sarama.ListGroupsRequest{})
	//fmt.Println(response, err)

	// -----------------------------------------------------------------------

	// mapping partitions for each topic to their owners. more types here maybe...
	topicOwners := make(map[string]map[int32]partitionOwner)

	coordinator, err := client.Coordinator(group)
	if err != nil {
		failf("failed to get coordinating broker for group=%s err=%v", group, err)
	}

	response, err := (*coordinator).DescribeGroups(&sarama.DescribeGroupsRequest{
		[]string{group},
		// other groups may have different coordinator but can try to batch it
		//groups,
	})
	if err != nil {
		failf("failed to get group info for group=%s err=%v", group, err)
	}

	for _, groupInfo := range response.Groups {
		err := (*groupInfo).Err
		state := (*groupInfo).State
		members := (*groupInfo).Members

		if err != sarama.ErrNoError {
			failf("group %s has errors err=%v", group, err)
		}

		fmt.Fprintf(os.Stderr, "group=%s is in state=%s\n", group, state)

		for _, memberInfo := range members {
			//fmt.Println("\n")
			//fmt.Println(memberInfo.ClientId, memberInfo.ClientHost)

			// see if we can use this too
			//memberMetadata, _ := memberInfo.GetMemberMetadata()
			//fmt.Println(*memberMetadata)

			// TODO catch the error here
			memberAssignment, _ := memberInfo.GetMemberAssignment()

			// TODO don't dereference here for later
			memberTopics := (*memberAssignment).Topics
			for topic, partitions := range memberTopics {
				//fmt.Println(topic, partitions)

				for _, partition := range partitions {
					//fmt.Println(partition)

					_, ok := topicOwners[topic]
					if !ok {
						topicOwners[topic] = make(map[int32]partitionOwner)
					}

					topicOwners[topic][partition] = partitionOwner{
						memberInfo.ClientId,
						memberInfo.ClientHost,
					}

					//fmt.Println(topicOwners)
				}
			}
		}
	}
	//fmt.Println(topicOwners)

	// -----------------------------------------------------------------------

	topicPartitions := map[string][]int32{}
	for topic := range topicOwners {
		partitions := fetchPartitions(&client, topic)
		// TODO convert to debug log
		fmt.Fprintf(os.Stderr, "found partitions=%v for topic=%v\n", partitions, topic)
		topicPartitions[topic] = partitions
	}
	//fmt.Println(topicPartitions)

	// -----------------------------------------------------------------------

	offsetManager, err := sarama.NewOffsetManagerFromClient(group, client)
	if err != nil {
		failf("failed to create offsetManager err=%v", err)
	}
	defer logClose("offset manager", offsetManager)

	for topic, partitions := range topicPartitions {
		fmt.Println("")

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

			partitionOwner, ok := topicOwners[topic][partition]
			if !ok {
				failf("failed to get owner for topic=%s partition=%d", topic, partition)
			}

			fmt.Println(group, topic, partition, groupOffset, producerOffset, lag, partitionOwner.Id, partitionOwner.HostIp)
		}
	}

	// -----------------------------------------------------------------------

	// TODO implement reporting to other sources
}
