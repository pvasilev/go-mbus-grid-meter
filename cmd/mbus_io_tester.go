package main

import (
	"github.com/jimlloyd/mbus/receiver"
	"github.com/jimlloyd/mbus/sender"
	"plamenv.com/mbusgridmeter/v1/mbus"
)

func main() {
	messages := []string{"aaa","bbb","ccc"}

	const numReceivers = 1
	const numSenders = 1

	var senders []*sender.Sender
	var receivers []*receiver.Receiver

	for i := 0; i< numReceivers;i++ {
		receivers = append(receivers, mbus.MakeReceiver())
	}
	for i := 0;i< numSenders;i++ {
		senders = append(senders, mbus.MakeSender())
	}
	receiverSem := make(chan int)
	senderSem := make(chan int)

	for _, aReceiver := range receivers {
		go mbus.RunReceiver(aReceiver, receiverSem, messages, numSenders)
	}
	for _, aSender := range senders {
		go mbus.RunSender(aSender, senderSem, messages)
	}
	for i:=0; i< numReceivers;i++ {
		<- receiverSem
	}
	for i:=0; i< numSenders;i++ {
		<- senderSem
	}
	for _, aReceiver := range receivers {
		_ = aReceiver.Close()
	}
	for _, aSender := range senders {
		_ = aSender.Close()
	}
}
