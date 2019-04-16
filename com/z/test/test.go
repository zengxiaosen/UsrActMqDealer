package main

import (
	queue "UsrActMqDealer/com/z/utils/timeoutQueueUtils"
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"reflect"
	"sync"
	"time"
)

func main() {
	//timeTest()
	//timeoutQueueTest()
	//testInterface()
	//testQueue()
	//testReflect()
	//testMap()
	testDeadlineContext()
}

func testDeadlineContext() {
	//d := time.Now().Add(1 * time.Second)
	//ctx, cancel := context.WithDeadline(context.Background(), d)
	//defer cancel()
	//select {
	//case <- time.After(2 * time.Second):
	//	fmt.Println("oversleep")
	//case <- ctx.Done():
	//	fmt.Println(ctx.Err())
	//}
	myCtx := &MyContext{}
	ctx, cancel := context.WithCancel(context.Background())
	myCtx.ctx = ctx
	myCtx.cancelFunc = cancel
	//等待一组协程结束
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		myCtx.value1 = "value1"
		defer wg.Add(-1)
		go func() {
			myCtx.value2 = "value2"
			defer wg.Add(-1)
			go func() {
				defer wg.Add(-1)
				for {
					select {
					case <-myCtx.ctx.Done():
						fmt.Println(ctx.Err())
						wg.Add(-1)
					default:
						fmt.Println("value1: ", myCtx.value1)
						fmt.Println("value2: ", myCtx.value2)
						myCtx.cancelFunc()
					}

				}
			}()
		}()
	}()
	wg.Wait()

}

type MyContext struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	value1     string
	value2     string
}

func testMap() {
	record := make(map[string]TestMapRecord)
	mapRecord := TestMapRecord{"record1", "record2"}
	record["test"] = mapRecord
	fmt.Println(record)
	mapRecord2 := record["test"]
	mapRecord2.record1 = "record2"
	mapRecord2.record2 = "record1"
	//record["test"] = mapRecord2
	fmt.Println(record)
}

type TestMapRecord struct {
	record1 string
	record2 string
}

func testReflect() {
	s1 := AA{1}
	fmt.Println(reflect.TypeOf(s1).Name())
}

func testQueue() {
	s1 := &AA{1}
	//s2 := s1
	s2 := &AA{}
	deepCopy(s2, s1)
	fmt.Println(s1.a)
	fmt.Println(s2.a)
	s2.a = 2
	fmt.Println(s1.a)
	fmt.Println(s2.a)
}

//深拷贝
func deepCopy(dst, src interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}

type AA struct {
	a int
}

func testInterface() {
	s := &S{}
	s.methodA()
	s.methodB()
}

type S struct {
}

func (s *S) methodA() {
	fmt.Println("method a")
}

func (s *S) methodB() {
	fmt.Println("method b")
}

type A interface {
	methodA()
}

type B interface {
	A
	methodB()
}

func timeoutQueueTest() {
	fmt.Println("========普通队列=========")
	q := queue.NewEmpty()
	q.Push(5)
	q.Push(4)
	q.Print()
	fmt.Println("pop: ", q.Pop())
	q.Print()
	fmt.Println("len: ", q.Length())
	//并发安全压入
	q.SafePush(6)
	q.Print()
	//并发安全出队列
	fmt.Println("safe pop: ", q.SafePop())
	q.Print()
	fmt.Println("========超时队列=========")
	tq := queue.TimeQueueWithTimeStep(10*time.Second, 50, 1*time.Nanosecond)
	tq.StartTimeSpying()
	tq.SafeTPush(5)
	tq.SafeTPush(6)

	fmt.Println("init:")
	tq.Print()

	time.Sleep(5 * time.Second)
	fmt.Println("after 5s: ")
	tq.Print()
	time.Sleep(9 * time.Second)
	fmt.Println("after 14s")
	tq.Print()

}

func timeTest() {
	processingTime := time.Now().UnixNano() / 1e6
	fmt.Println(processingTime)
	time.Sleep(time.Second * 2)
	processingTime = time.Now().UnixNano() / 1e6
	fmt.Println(processingTime)
}
