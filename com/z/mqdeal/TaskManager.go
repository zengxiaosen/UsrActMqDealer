package main

import (
	"UsrActMqDealer/com/z/common"
	"UsrActMqDealer/com/z/storageclient/redisclient"
	"golang.org/x/net/context"
	"sync"
)

/**
queue: 超时队列，相当于一个scheduler
1、consumer->queue
2、queue->computer
*/
func main() {
	//init
	common.InitUsrAct5MinQueueMsgChan()
	common.InitUsrRpt5MinQueueMsgChan()
	InitExecutor()
	redisclient.Init()
	//work
	G_executor.getTumblingWrapper()
	consumeActor := &ConsumeActor{}

	//同步等待所有异步协程执行完
	var wg sync.WaitGroup
	wg.Add(2)

	ctx, cancel := context.WithCancel(context.Background())
	consumeActor.ctx = ctx
	consumeActor.ctxCancel = cancel
	defer cancel()

	go func() {
		defer wg.Add(-1)
		consumeActor.consumerUsrClickShow5MinDealer()
	}()
	go func() {
		defer wg.Add(-1)
		consumeActor.consumerUsrRpt5MinDealer()
	}()
	wg.Wait()
}
