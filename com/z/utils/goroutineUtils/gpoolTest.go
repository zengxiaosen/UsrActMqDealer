package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type counter struct {
	sync.Mutex
	counter int
}

func incrCounter(c *counter) {
	c.Lock()
	c.counter ++
	c.Unlock()
}

func TestBaseOption(t *testing.T) {
	opt := Options{
		MaxWorker: 10,
		MinWorker: 100,
	}
	_, err := NewGPool(&opt)
	if err == nil {
		t.Fatal(err)
	}
}

func TestBaseRun() {
	opt := Options{
		MaxWorker:      10,
		MinWorker:      3,
		JobBuffer:      3,
		IdleTimeout:    10 * time.Second,
		DispatchPeriod: 100 * time.Millisecond,
	}

	pool, err := NewGPool(&opt)
	if err != nil {
		fmt.Println(err)
	}

	num := 100
	joinRun(num, pool, 0)
}
func joinRun(num int, pool *gpool, blockTime time.Duration) {
	incr := new(counter)
	res := make(chan bool, num)
	for index := 0; index < num; index++ {
		pool.ProcessSync(
			func() {
				incrCounter(incr)
				res <- true
				if blockTime > 0 {
					time.Sleep(blockTime)
				}
			},
		)
	}

	lc := 0
	timer := time.NewTimer(15 * time.Second)
	for {
		select {
		case <-res:
			lc ++
			if lc == num {
				return
			}
		case <-timer.C:
			if lc != num {
				fmt.Println("counter error")
			}
			return
		}
	}
}

func main() {
	gp, err := NewGPool(&Options{
		MaxWorker:   5, 				// 最大的协程数
		MinWorker:   2, 				// 最小的协程数
		JobBuffer:   1, 				// 缓冲队列的大小
		IdleTimeout: 120 * time.Second, // 协程的空闲超时退出时间
	})

	if err != nil {
		panic(err.Error())
	}

	for index := 0; index < 1000; index++ {
		gp.ProcessAsync(func() {
			fmt.Println(time.Now())
			time.Sleep(5 * time.Second)
		})
	}

	k := make(chan bool, 0)
	<- k
}
