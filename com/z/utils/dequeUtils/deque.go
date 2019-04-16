package dequeUtils

import (
	"container/list"
	"sync"
	"time"
)

// safe for concurrent usage
type Deque struct {
	sync.RWMutex
	container *list.List
	capacity  int
	buf       chan interface{}
}

//NewDeque creates a Deque
func NewDeque(capacity int, withBlock bool) *Deque {
	//capacity default -1
	return newCappedDeque(capacity, withBlock)
}

func newCappedDeque(capacity int, withBlock bool) *Deque {
	return &Deque{
		container: list.New(),
		capacity:  capacity,
		buf:       make(chan interface{}, 0), //block
	}
}

//Append inserts element at the back of the Deque in a O(1) time complexity
func (s *Deque) Append(item interface{}) bool {
	s.Lock()
	defer s.Unlock()

	if s.capacity < 0 || s.container.Len() < s.capacity {
		s.container.PushBack(item)
		return true
	}
	return false
}

//prepend inserts element at the Deques front in a O(1) time complexity
func (s *Deque) Prepend(item interface{}) bool {
	s.Lock()
	defer s.Unlock()
	if s.capacity < 0 || s.container.Len() < s.capacity {
		s.container.PushFront(item)
		return true
	}
	return false
}

//pop removes the last element if the deque in a O(1) time complexity
func (s *Deque) Pop() interface{} {
	s.Lock()
	defer s.Unlock()

	var item interface{} = nil
	var lastContainerItem *list.Element = nil

	lastContainerItem = s.container.Back()
	if lastContainerItem != nil {
		item = s.container.Remove(lastContainerItem)
	}
	return item
}

//popBlock
func (s *Deque) PopBlock() interface{} {
	s.Lock()
	defer s.Unlock()

	for {
		res := s.Pop()
		if res == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return res
	}
}

func (s *Deque) PopBlockTimeout(timeout time.Duration) interface{} {
	s.Lock()
	defer s.Unlock()

	timer := time.After(timeout)
	for {
		select {
		case <-timer:
			return nil
		default:
			res := s.Pop()
			if res == nil {
				continue
			}
			return res
		}
	}
}

//Shift removes the first element of the deque in a O(1) time complexity
func (s *Deque) Shift() interface{} {
	s.Lock()
	defer s.Unlock()

	var item interface{} = nil
	var firstContainerItem *list.Element = nil

	firstContainerItem = s.container.Front()
	if firstContainerItem != nil {
		item = s.container.Remove(firstContainerItem)
	}
	return item
}

//First returns the first value stored in the deque in a O(1) time complexity
func (s *Deque) First() interface{} {
	s.RLock()
	defer s.RUnlock()

	item := s.container.Front()
	if item != nil {
		return item.Value
	} else {
		return nil
	}
}

//Lst returns the last value stored in the deque in a O(1) time complexity
func (s *Deque) Last() interface{} {
	s.RLock()
	defer s.RUnlock()

	item := s.container.Back()
	if item != nil {
		return item.Value
	} else {
		return nil
	}
}

// Size returns the actual deque size
func (s *Deque) Size() int {
	s.RLock()
	defer s.RUnlock()

	return s.container.Len()
}

// Capacity returns the capacity of the deque, or -1 if unlimited
func (s *Deque) Capacity() int {
	s.RLock()
	defer s.RUnlock()
	return s.capacity
}

// Empty checks if the deque is empty
func (s *Deque) Empty() bool {
	s.RLock()
	defer s.RUnlock()

	return s.container.Len() == 0
}

// Full checks if the deque is full
func (s *Deque) Full() bool {
	s.RLock()
	defer s.RUnlock()

	return s.capacity >= 0 && s.container.Len() >= s.capacity
}
