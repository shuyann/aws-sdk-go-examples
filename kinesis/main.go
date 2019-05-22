package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

func main() {
	// Create session using the default region and credentials
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	// Create Kinesis Client
	kc := kinesis.New(sess)
	streamName := aws.String("test-stream")
	createStream(kc, streamName)
	// check: createStream -> describeStream -> putRecord -> putRecords -> getRecords -> deleteStream
}

func createStream(kc *kinesis.Kinesis, streamName *string) {
	out, err := kc.CreateStream(&kinesis.CreateStreamInput{
		ShardCount: aws.Int64(1),
		StreamName: streamName,
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v\n", out)

	if err := kc.WaitUntilStreamExists(&kinesis.DescribeStreamInput{StreamName: streamName}); err != nil {
		panic(err)
	}
}
func describeStream(kc *kinesis.Kinesis, streamName *string) {
	streams, err := kc.DescribeStream(&kinesis.DescribeStreamInput{StreamName: streamName})
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v\n", streams)
}
func putRecord(kc *kinesis.Kinesis, streamName *string) {
	putOutput, err := kc.PutRecord(&kinesis.PutRecordInput{
		Data:         []byte("hoge"),
		StreamName:   streamName,
		PartitionKey: aws.String("key1"),
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v\n", putOutput)
}
func putRecords(kc *kinesis.Kinesis, streamName *string) {
	// put 10 records using PutRecords API
	entries := make([]*kinesis.PutRecordsRequestEntry, 10)
	for i := 0; i < len(entries); i++ {
		entries[i] = &kinesis.PutRecordsRequestEntry{
			Data:         []byte(fmt.Sprintf("hoge%d", i)),
			PartitionKey: aws.String("key2"),
		}
	}
	fmt.Printf("%v\n", entries)
	putsOutput, err := kc.PutRecords(&kinesis.PutRecordsInput{
		Records:    entries,
		StreamName: streamName,
	})
	if err != nil {
		panic(err)
	}
	// putsOutput has Records, and its shard id and sequece enumber.
	fmt.Printf("%v\n", putsOutput)
}
func getRecords(kc *kinesis.Kinesis, streamName *string) {
	// retrieve iterator
	iteratorOutput, err := kc.GetShardIterator(&kinesis.GetShardIteratorInput{
		// Shard Id is provided when making put record(s) request.
		ShardId:           aws.String("shardId-000000000000"), // putObject shardId
		ShardIteratorType: aws.String("TRIM_HORIZON"),
		// ShardIteratorType: aws.String("AT_SEQUENCE_NUMBER"),
		// ShardIteratorType: aws.String("LATEST"),
		StreamName: streamName,
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v\n", iteratorOutput)
	// get records use shard iterator for making request
	records, err := kc.GetRecords(&kinesis.GetRecordsInput{
		ShardIterator: iteratorOutput.ShardIterator,
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v\n", records)
	for _, record := range records.Records {
		fmt.Println(string(record.Data))
	}

}
func deleteStream(kc *kinesis.Kinesis, streamName *string) {
	// OK, finally delete your stream
	deleteOutput, err := kc.DeleteStream(&kinesis.DeleteStreamInput{
		StreamName: streamName,
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v\n", deleteOutput)
}
