package tools

import (
	"log"
	"sync/atomic"
	"time"
)

var (
	seq = new(int64)
)

func MethodExecTime(method string) func() {
	id := atomic.AddInt64(seq, 1)
	start := time.Now()
	deferFun := func() {
		end := time.Now()
		log.Printf("%d ns %d ms  has cost by task %s %d  ",
			end.Sub(start).Nanoseconds(),
			end.Sub(start).Nanoseconds()/1000000,
			method, id)
	}

	return deferFun
}
