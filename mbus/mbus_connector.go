package mbus

import (
	"fmt"
	"github.com/jimlloyd/mbus/receiver"
	"github.com/jimlloyd/mbus/sender"
	"log"
	"time"
)

const (
	MbusHostUri = "192.168.1.51:5000"
)
func MakeReceiver() *receiver.Receiver {
	aReceiver, err := receiver.NewReceiver(MbusHostUri)
	if err != nil {
		log.Printf("Failed to create receiver: %s", err)
		panic("Failed to create receiver")
	}
	return aReceiver
}

func RunReceiver(aReceiver *receiver.Receiver, sem chan<- int, messages []string, numSenders int) {
	receivedMessages := make(map[string]int)
	for _, msg := range messages {
		receivedMessages[msg] = 0
	}

	incoming := aReceiver.MessagesChannel()
	for i:=0; i < len(messages)*numSenders;i++ {
		packet := <- incoming;
		msg := string(packet.Data)
		fmt.Println("Received message:", msg)
		count, ok := receivedMessages[msg]
		if ! ok {
			fmt.Println("Unexpected message received")
		}
		receivedMessages[msg] = count + 1
	}

	for msg, count := range receivedMessages {
		if count != numSenders {
			fmt.Printf("Wrong number of messages received for message: %s, Expected:%d, received:%d", msg, numSenders, count)
		}
	}

	fmt.Println("Done receiving")
	sem <- 1
}

func MakeSender() *sender.Sender {
	aSender, err := sender.NewSender(MbusHostUri)
	if err != nil {
		log.Printf("Failed to create sender: %s", err)
		panic("Failed to create sender")
	}
	return aSender
}

func RunSender(aSender *sender.Sender, sem chan <- int, messages []string) {
	for _, expected := range messages {
		fmt.Println("Sending message:", expected)
		time.Sleep(50 * time.Millisecond)
		_, err := aSender.Send([]byte(expected))
		if err != nil {
			fmt.Println("Error sending message:", err)
		}
		fmt.Println("Sent message:", expected)
	}
	fmt.Println("Done sending")
	sem <- 1
}



