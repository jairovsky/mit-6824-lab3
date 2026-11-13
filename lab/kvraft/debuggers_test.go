package kvraft

import (
	"log"
	"runtime"
	"time"
)

func init() {
	debugNumGoroutines := false
	if debugNumGoroutines {
		go func() {
			for {
				time.Sleep(1000 * time.Millisecond)
				log.Printf("num goroutine %d", runtime.NumGoroutine())
			}
		}()
	}
}
