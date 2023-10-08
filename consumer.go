package main

import (
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
)

func consumeMessage(sqsQueue *sqs.SQS, wt *sync.WaitGroup) {
	for {
		msgResult, _ := sqsQueue.ReceiveMessage(&sqs.ReceiveMessageInput{
			AttributeNames: []*string{
				aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
			},
			MessageAttributeNames: []*string{
				aws.String(sqs.QueueAttributeNameAll),
			},
			QueueUrl:            aws.String("https://sqs.us-east-1.amazonaws.com/507403822990/scale-producer"),
			MaxNumberOfMessages: aws.Int64(10),
		})

		if len(msgResult.Messages) == 0 {
			continue
		}

		var messagesToDelete []*sqs.DeleteMessageBatchRequestEntry
		for index := 0; index < len(msgResult.Messages); index += 1 {
			id := uuid.NewString()
			messagesToDelete = append(messagesToDelete, &sqs.DeleteMessageBatchRequestEntry{
				Id:            &id,
				ReceiptHandle: msgResult.Messages[index].ReceiptHandle,
			})
		}

		if len(messagesToDelete) > 0 {
			sqsQueue.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
				QueueUrl: aws.String("https://sqs.us-east-1.amazonaws.com/507403822990/scale-producer"),
				Entries:  messagesToDelete,
			})
		}

	}
	wt.Done()
}

func main() {

	sess, _ := session.NewSessionWithOptions(session.Options{
		Profile: "tiago",
		Config: aws.Config{
			Region: aws.String("us-east-1"),
		},
	})

	sqsQueue := sqs.New(sess)

	var wt sync.WaitGroup
	for index := 0; index < 300; index += 1 {
		wt.Add(1)
		go consumeMessage(sqsQueue, &wt)
	}

	wt.Wait()
	fmt.Println("Hi consumer")
}
