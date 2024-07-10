package main

import (
	"flag"
	"log"
	"runtime"
	"runtime/pprof"
	"os"
	"time"
	"sync/atomic"
	"fmt"

	"golang.org/x/sync/errgroup"
)

var(
	connect = flag.String("connect", "", "connect to a server")
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	threads = flag.Int("threads", -1, "number of system threads to use to use")

	// client only flags
	benchmarkWorkers = flag.Int("benchmarkworkers", 10, "number of goroutines to use to benchmark")


	// server only flags
	acceptWorkers = flag.Int("acceptworkers", 10, "number of goroutines to use to accept connections")
	shards = flag.Int("shards", 1, "number of store shards to use")
	backend = flag.String("backend", "channel", "backend to use (channel, concurrent, lock)")
)

var (
	globalStart = time.Now()
	globalIters atomic.Uint64
)

func barrier(store *TCPStoreClient, key string, worldSize int) error {
	finalKey := key + "/final"

	rank, err := store.Add(key, 1)
	if err != nil {
		return err
	}
	if int(rank) == worldSize {
		if err := store.Set(finalKey, []byte("done")); err != nil {
			return err
		}
	} else {
		if err := store.Wait([]string{finalKey}); err != nil {
			return err
		}
	}
	return nil
}

func benchmarkBarrier(rank int) error {
	pid := os.Getpid()

	for i := 0; i < 2; i ++ {
		start := time.Now()

		store, err := Dial(*connect)
		if err != nil {
			return err
		}
		defer store.Close()

		key := fmt.Sprintf("very long barrier blah blah blah blah blah blah blah blah blah blah barrier/%d/%d", pid, i)
		if err := barrier(store, key, *benchmarkWorkers); err != nil {
			return err
		}

		if err := store.Close(); err != nil {
			return err
		}

		dur := time.Since(start)
		if rank == 0 {
			log.Printf("[%d] barrier took %s", rank, dur)
		}
	}

	return nil
}

func benchmark(i int) error {
	log.Printf("connecting to %q", *connect)
	c, err := Dial(*connect)
	if err != nil {
		return err
	}
	log.Printf("connected to %q", *connect)

	for {
		iters := 0
		start := time.Now()
		for time.Since(start) < 10 * time.Second {
			const key = "key"
			if err := c.Set(key, []byte("value")); err != nil {
				return err
			}
			if _, err := c.Get(key); err != nil {
				return err
			}
			iters += 1
			globalIters.Add(1)
		}
		const opCount = 3

		dur := float64(time.Since(start)) / float64(time.Second)
		qps := float64(iters) / dur * opCount
		globalDur := float64(time.Since(globalStart)) / float64(time.Second)
		globalQPS := float64(globalIters.Load()) / globalDur * opCount

		log.Printf("[%d] qps %.2f, global qps %.2f", i, qps, globalQPS)
	}

	return nil
}

func run() error {
	if *connect != "" {
		var eg errgroup.Group

		for i := 0; i < *benchmarkWorkers; i++ {
			i := i
			eg.Go(func() error {
				if err := benchmarkBarrier(i); err != nil {
					log.Printf("conn errored: %s", err)
					return err
				}
				return nil
			})
		}

		return eg.Wait()
	} else {
		store := &TCPStoreServer{}

		for i := 0; i < *shards; i++ {
			s, err := newStore(*backend)
			if err != nil {
				return err
			}
			store.stores = append(store.stores, s)
		}
		addr := ":19503"
		log.Printf("listening on %q", addr)

		return store.Listen(addr)
	}
}

func main() {
	log.SetFlags(log.Flags() | log.Lshortfile)
	flag.Parse()
	if *threads > 0 {
		runtime.GOMAXPROCS(*threads)
	}

	log.Printf("cpu profile %q %v", *cpuprofile)

    if *cpuprofile != "" {
		log.Printf("cpuprofile")
        f, err := os.Create(*cpuprofile)
        if err != nil {
            log.Fatal(err)
        }
        pprof.StartCPUProfile(f)
        defer pprof.StopCPUProfile()

		go func() {
			log.Printf("stopping in 30 seconds")
			<-time.NewTimer(time.Second * 30).C
			log.Printf("Stopping CPU profile")
            pprof.StopCPUProfile()
		}()
    }


	if err := run(); err != nil {
		log.Fatal(err)
	}
}
