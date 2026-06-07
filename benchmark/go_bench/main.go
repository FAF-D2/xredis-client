package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const numTasks = 100000

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	defer rdb.Close()

	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		fmt.Printf("Connection failed: %v\n", err)
		return
	}
	fmt.Println("---Redis Response---")
	fmt.Printf("connect success, ping: %s, num_tasks: %d\n", pong, numTasks)

	var wg sync.WaitGroup
	var cur int64 = 0

	var globalStart time.Time
	var globalEnd time.Time

	// bigKey := ""
	// for i := 0; i < 1024; i++ {
	// 	bigKey += "\t"
	// }
	// rdb.Set(ctx, "bulkstring", bigKey, 0)

	smallKey := ""
	for i := 0; i < 256; i++ {
		smallKey += "\t"
	}
	rdb.Set(ctx, "bulkstring", smallKey, 0)

	globalStart = time.Now()

	for i := 0; i < numTasks; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			_, err := rdb.Get(ctx, "bulkstring").Result()
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			}

			doneTasks := atomic.AddInt64(&cur, 1)
			if doneTasks >= numTasks {
				globalEnd = time.Now()
				all := globalEnd.Sub(globalStart).Nanoseconds()
				fmt.Printf("Avg Duration: %d ns\n", all/numTasks)
				fmt.Printf("Total Duration: %d ns\n", all)
			}
		}()
	}

	wg.Wait()
}