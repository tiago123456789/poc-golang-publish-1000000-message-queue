package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
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

type HTTPClientSettings struct {
	Connect          time.Duration
	ConnKeepAlive    time.Duration
	ExpectContinue   time.Duration
	IdleConn         time.Duration
	MaxAllIdleConns  int
	MaxHostIdleConns int
	ResponseHeader   time.Duration
	TLSHandshake     time.Duration
}

func NewHTTPClientWithSettings(httpSettings HTTPClientSettings) (*http.Client, error) {
	tr := &http.Transport{
		ResponseHeaderTimeout: httpSettings.ResponseHeader,
		Proxy:                 http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			KeepAlive: httpSettings.ConnKeepAlive,
			DualStack: true,
			Timeout:   httpSettings.Connect,
		}).DialContext,
		MaxIdleConns:          httpSettings.MaxAllIdleConns,
		IdleConnTimeout:       httpSettings.IdleConn,
		TLSHandshakeTimeout:   httpSettings.TLSHandshake,
		MaxIdleConnsPerHost:   httpSettings.MaxHostIdleConns,
		ExpectContinueTimeout: httpSettings.ExpectContinue,
	}

	return &http.Client{
		Transport: tr,
	}, nil
}

func main() {
	start := time.Now()
	httpClient, err := NewHTTPClientWithSettings(HTTPClientSettings{
		Connect:          5 * time.Second,
		ExpectContinue:   1 * time.Second,
		IdleConn:         90 * time.Second,
		ConnKeepAlive:    30 * time.Second,
		MaxAllIdleConns:  100,
		MaxHostIdleConns: 10,
		ResponseHeader:   5 * time.Second,
		TLSHandshake:     5 * time.Second,
	})
	if err != nil {
		fmt.Println("Got an error creating custom HTTP client:")
		fmt.Println(err)
		return
	}

	// sess := session.Must(session.NewSession(&aws.Config{
	// 	HTTPClient: httpClient,
	// }))

	sess, _ := session.NewSessionWithOptions(session.Options{
		Profile: "tiago",
		Config: aws.Config{
			HTTPClient: httpClient,
			Region:     aws.String("us-east-1"),
		},
	})

	sqsQueue := sqs.New(sess)

	// maxGoroutines := 80
	// guard := make(chan struct{}, maxGoroutines)
	var wt sync.WaitGroup
	for index := 0; index < 500000; index += 500 {
		wt.Add(1)
		// guard <- struct{}{}
		go processInBatch(sqsQueue, &wt, 500)
	}

	wt.Wait()
	fmt.Println(time.Since(start))
}
