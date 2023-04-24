package cyclicbarrier

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marusama/cyclicbarrier"
)

func checkBarrier(t *testing.T, b CyclicBarrier,
	expectedParties, expectedNumberWaiting int, expectedIsBroken bool) {

	parties, numberWaiting := b.GetParties(), b.GetNumberWaiting()
	isBroken := b.IsBroken()

	if expectedParties >= 0 && parties != expectedParties {
		t.Error("barrier must have parties = ", expectedParties, ", but has ", parties)
	}
	if expectedNumberWaiting >= 0 && numberWaiting != expectedNumberWaiting {
		t.Error("barrier must have numberWaiting = ", expectedNumberWaiting, ", but has ", numberWaiting)
	}
	if isBroken != expectedIsBroken {
		t.Error("barrier must have isBroken = ", expectedIsBroken, ", but has ", isBroken)
	}
}

func TestNew(t *testing.T) {
	tests := []func(){
		func() {
			b := New(10)
			checkBarrier(t, b, 10, 0, false)
			if b.(*cyclicBarrier).barrierAction != nil {
				t.Error("barrier have unexpected barrierAction")
			}
		},
		func() {
			defer func() {
				if recover() == nil {
					t.Error("Panic expected")
				}
			}()
			_ = New(0)
		},
		func() {
			defer func() {
				if recover() == nil {
					t.Error("Panic expected")
				}
			}()
			_ = New(-1)
		},
	}
	for _, test := range tests {
		test()
	}
}

func TestNewWithAction(t *testing.T) {
	tests := []func(){
		func() {
			b := NewWithAction(10, func() error { return nil })
			checkBarrier(t, b, 10, 0, false)
			if b.(*cyclicBarrier).barrierAction == nil {
				t.Error("barrier doesn't have expected barrierAction")
			}
		},
		func() {
			b := NewWithAction(10, nil)
			checkBarrier(t, b, 10, 0, false)
			if b.(*cyclicBarrier).barrierAction != nil {
				t.Error("barrier have unexpected barrierAction")
			}
		},
		func() {
			defer func() {
				if recover() == nil {
					t.Error("Panic expected")
				}
			}()
			_ = NewWithAction(0, func() error { return nil })
		},
		func() {
			defer func() {
				if recover() == nil {
					t.Error("Panic expected")
				}
			}()
			_ = NewWithAction(-1, func() error { return nil })
		},
	}
	for _, test := range tests {
		test()
	}
}

func TestAwaitOnce(t *testing.T) {
	n := 100 // goroutines count
	b := New(n)
	ctx := context.Background()

	wg := sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			err := b.Await(ctx)
			if err != nil {
				panic(err)
			}
			wg.Done()
		}()
	}

	wg.Wait()
	checkBarrier(t, b, n, 0, false)
}

func TestAwaitMany(t *testing.T) {
	n := 100  // goroutines count
	m := 1000 // inner cycle count
	b := New(n)
	ctx := context.Background()

	wg := sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(num int) {
			for j := 0; j < m; j++ {
				err := b.Await(ctx)
				if err != nil {
					panic(err)
				}
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	checkBarrier(t, b, n, 0, false)
}

func TestAwaitOnceCtxDone(t *testing.T) {
	n := 100        // goroutines count
	b := New(n + 1) // parties are more than goroutines count so all goroutines will wait
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	var deadlineCount, brokenBarrierCount int32

	wg := sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(num int) {
			err := b.Await(ctx)
			if err == context.DeadlineExceeded {
				atomic.AddInt32(&deadlineCount, 1)
			} else if err == ErrBrokenBarrier {
				atomic.AddInt32(&brokenBarrierCount, 1)
			} else {
				panic("must be context.DeadlineExceeded or ErrBrokenBarrier error")
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	checkBarrier(t, b, n+1, -1, true)
	if deadlineCount == 0 {
		t.Error("must be more than 0 context.DeadlineExceeded errors, but found", deadlineCount)
	}
	if deadlineCount+brokenBarrierCount != int32(n) {
		t.Error("must be exactly", n, "context.DeadlineExceeded and ErrBrokenBarrier errors, but found", deadlineCount+brokenBarrierCount)
	}
}

func TestAwaitManyCtxDone(t *testing.T) {
	n := 100 // goroutines count
	b := New(n)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	wg := sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			for {
				err := b.Await(ctx)
				if err != nil {
					if err != context.DeadlineExceeded && err != ErrBrokenBarrier {
						panic("must be context.DeadlineExceeded or ErrBrokenBarrier error")
					}
					break
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()
	checkBarrier(t, b, n, -1, true)
}

func TestAwaitAction(t *testing.T) {
	n := 100  // goroutines count
	m := 1000 // inner cycle count
	ctx := context.Background()

	cnt := 0
	b := NewWithAction(n, func() error {
		cnt++
		return nil
	})

	wg := sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < m; j++ {
				err := b.Await(ctx)
				if err != nil {
					panic(err)
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()
	checkBarrier(t, b, n, 0, false)
	if cnt != m {
		t.Error("cnt must be equal to = ", m, ", but it's ", cnt)
	}
}

func TestReset(t *testing.T) {
	n := 100        // goroutines count
	b := New(n + 1) // parties are more than goroutines count so all goroutines will wait
	ctx := context.Background()

	go func() {
		time.Sleep(30 * time.Millisecond)
		b.Reset()
	}()

	wg := sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			err := b.Await(ctx)
			if err != ErrBrokenBarrier {
				panic(err)
			}
			wg.Done()
		}()
	}

	wg.Wait()
	checkBarrier(t, b, n+1, 0, false)
}

func TestAwaitErrorInActionThenReset(t *testing.T) {
	n := 100 // goroutines count
	ctx := context.Background()

	errExpected := errors.New("test error")

	isActionCalled := false
	var expectedErrCount, errBrokenBarrierCount int32

	b := NewWithAction(n, func() error {
		isActionCalled = true
		return errExpected
	})

	wg := sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			err := b.Await(ctx)
			if err == errExpected {
				atomic.AddInt32(&expectedErrCount, 1)
			} else if err == ErrBrokenBarrier {
				atomic.AddInt32(&errBrokenBarrierCount, 1)
			} else {
				panic(err)
			}
			wg.Done()
		}()
	}

	wg.Wait()
	checkBarrier(t, b, n, n, true) // check that barrier is broken
	if !isActionCalled {
		t.Error("barrier action must be called")
	}
	if !b.IsBroken() {
		t.Error("barrier must be broken via action error")
	}
	if expectedErrCount != 1 {
		t.Error("expectedErrCount must be equal to", 1, ", but it equals to", expectedErrCount)
	}
	if errBrokenBarrierCount != int32(n-1) {
		t.Error("expectedErrCount must be equal to", n-1, ", but it equals to", errBrokenBarrierCount)
	}

	// call await on broken barrier must return ErrBrokenBarrier
	if b.Await(ctx) != ErrBrokenBarrier {
		t.Error("call await on broken barrier must return ErrBrokenBarrier")
	}

	// do reset broken barrier
	b.Reset()
	if b.IsBroken() {
		t.Error("barrier must not be broken after reset")
	}
	checkBarrier(t, b, n, 0, false)
}

func TestAwaitTooMuchGoroutines(t *testing.T) {

	n := 100  // goroutines count
	m := 1000 // inner cycle count
	b := New(1)
	ctx := context.Background()

	var panicCount int32

	wg := sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(num int) {
			defer func() {
				if recover() != nil {
					atomic.AddInt32(&panicCount, 1)
				}
				wg.Done()
			}()
			for j := 0; j < m; j++ {
				err := b.Await(ctx)
				if err != nil {
					panic(err)
				}
			}
		}(i)
	}

	wg.Wait()
	checkBarrier(t, b, 1, 0, false)

	if panicCount == 0 {
		t.Error("barrier must panic when await is called from too much goroutines")
	}
}

func TestExmple1(t *testing.T) {
	barrier := New(3)
	for i := 0; i < 3; i++ {
		go func(id int) {
			log.Printf("start: %v", id)
			barrier.Await(context.Background()) // 这里会一直等待，直到3个groutine 都走到这里
			log.Printf("finish: %v", id)
		}(i)
	}

	select {}
}

func TestExmple2(t *testing.T) {
	cnt := 0
	b := cyclicbarrier.NewWithAction(10, func() error {
		cnt++ // 定义一个回调函数，让cnt自增，barrier放行一次，该回调执行一次
		return nil
	})

	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ { // create 10 goroutines (the same count as barrier parties)
		wg.Add(1)
		go func() {
			for j := 0; j < 5; j++ {

				// do some hard work 5 times
				time.Sleep(100 * time.Millisecond)

				err := b.Await(context.TODO()) // ..and wait for other parties on the barrier. 即所有的parties都走到这里，才放行，下一轮
				// Last arrived goroutine will do the barrier action
				// and then pass all other goroutines to the next round
				if err != nil {
					panic(err)
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()
	fmt.Println(cnt) // cnt = 5, it means that the barrier was passed 5 times
}
