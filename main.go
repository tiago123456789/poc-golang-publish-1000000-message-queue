package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
)

type EmailMessage struct {
	To      string `json:"to"`
	Content string `json:"content"`
}

func processInBatch(sqsQueue *sqs.SQS, wt *sync.WaitGroup, total int) {

	messages := []*sqs.SendMessageBatchRequestEntry{}
	for index := 0; index < total; index++ {
		emailToJson, _ := json.Marshal(EmailMessage{
			To:      "tiago@gmail.com",
			Content: "Hi my friend",
		})
		emailToString := string(emailToJson)
		id := uuid.NewString()

		messages = append(messages, &sqs.SendMessageBatchRequestEntry{
			Id:          &id,
			MessageBody: &emailToString,
		})

		if len(messages) == 10 {
			sqsQueue.SendMessageBatch(&sqs.SendMessageBatchInput{
				Entries:  messages,
				QueueUrl: aws.String("https://sqs.us-east-1.amazonaws.com/507403822990/scale-producer"),
			})

			messages = []*sqs.SendMessageBatchRequestEntry{}
		}

	}
	wt.Done()
	// <-guard
}

func main() {
	start := time.Now()
	sess, _ := session.NewSessionWithOptions(session.Options{
		Profile: "tiago",
		Config: aws.Config{
			Region: aws.String("us-east-1"),
		},
	})

	sqsQueue := sqs.New(sess)

	// maxGoroutines := 80
	// guard := make(chan struct{}, maxGoroutines)
	var wt sync.WaitGroup
	for index := 0; index < 100000; index += 500 {
		wt.Add(1)
		// guard <- struct{}{}
		go processInBatch(sqsQueue, &wt, 500)
	}

	wt.Wait()
	fmt.Println(time.Since(start))
}
