package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func consume(ctx context.Context, seeds []string, topic string, offset int64, value string) {
	client, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.ConsumerGroup(topic), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit())
	check(err)
	defer client.Close()
	fetches := client.PollRecords(ctx, 1)
	check(fetches.Err())
	records := fetches.Records()
	if len(records) != 1 || records[0].Offset != offset || string(records[0].Value) != value {
		panic(fmt.Sprintf("unexpected records: %+v", records))
	}
	check(client.CommitRecords(ctx, records[0]))
}

func main() {
	seeds := strings.Split(os.Args[1], ",")
	topic, shared := os.Args[2], os.Args[3]
	ctx, cancel := context.WithTimeout(context.Background(), 85*time.Second)
	defer cancel()
	client, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.RequiredAcks(kgo.AllISRAcks()))
	check(err)
	defer client.Close()
	request := kmsg.NewPtrCreateTopicsRequest()
	request.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: 1, ReplicationFactor: 3}}
	result, err := request.RequestWith(ctx, client)
	check(err)
	check(kerr.ErrorForCode(result.Topics[0].ErrorCode))
	ready := time.Now().Add(12 * time.Second)
	for {
		all, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, client)
		check(err)
		found := false
		for _, item := range all.Topics {
			if item.Topic != nil && *item.Topic == topic {
				found = len(item.Partitions) == 1 && len(item.Partitions[0].Replicas) == 3 &&
					item.Partitions[0].Leader > 0 && item.Partitions[0].ErrorCode == 0
			}
		}
		if found {
			break
		}
		if time.Now().After(ready) {
			panic("topic list or partition leader missing")
		}
		time.Sleep(100 * time.Millisecond)
	}
	write := func(value string, offset int64) {
		results := client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(value)})
		check(results.FirstErr())
		if results[0].Record.Offset != offset {
			panic(fmt.Sprintf("produce offset: %d, expected %d", results[0].Record.Offset, offset))
		}
	}
	write("first", 0)
	consume(ctx, seeds, topic, 0, "first")
	check(os.WriteFile(filepath.Join(shared, topic+".ready"), []byte("ready\n"), 0644))
	for {
		if _, err := os.Stat(filepath.Join(shared, topic+".go")); err == nil {
			break
		}
		check(ctx.Err())
		time.Sleep(20 * time.Millisecond)
	}
	write("second", 1)
	consume(ctx, seeds, topic, 1, "second")
	fmt.Println("franz-go create/list/describe, idempotent produce across failover, fetch and group offset verified")
}
