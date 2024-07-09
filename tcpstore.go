package main

import (
	"log"
	"net"
	"io"
	"encoding/binary"
	"fmt"
	"bufio"
	"flag"
	"os"
	"time"
	"runtime/pprof"
	"runtime"

	"golang.org/x/sync/errgroup"
	"github.com/cespare/xxhash"
)

var(
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	threads = flag.Int("threads", 10, "number of system threads to use to use")
	acceptWorkers = flag.Int("acceptworkers", 10, "number of goroutines to use to accept connections")
	shards = flag.Int("shards", 1, "number of store shards to use")
	backend = flag.String("backend", "channel", "backend to use (channel, concurrent, lock)")
)

type Store interface {
	Set(key string, value []byte) error
	Get(key string) ([]byte, error)
	Add(key string, value int64) (int64, error)
	Wait(keys []string) error
}


type QueryType byte
const (
	VALIDATE QueryType = iota
	SET
	COMPARE_SET
	GET
	ADD
	CHECK
	WAIT
	GETNUMKEYS
	DELETE_KEY
	APPEND
	MULTI_GET
	MULTI_SET
	CANCEL_WAIT
	PING
)

type CheckResponseType byte
const (
	READY CheckResponseType = iota
	NOT_READY
)

type WaitResponseType byte
const (
	STOP_WAITING WaitResponseType = iota
	WAIT_CANCELED
)

const validationMagicNumber = 0x3C85F7CE

type TCPStore struct{
	stores []Store
}

func newStore(backend string) (Store, error) {
	switch backend {
	case "channel":
		return NewChannelStore(), nil
	case "concurrent":
		return NewConcurrentStore(), nil
	case "lock":
		return NewLockStore(), nil
	default:
		return nil, fmt.Errorf("unknown backend %s", backend)
	}
}


func run() error {
	store := &TCPStore{}

	for i := 0; i < *shards; i++ {
		s, err := newStore(*backend)
		if err != nil {
			return err
		}
		store.stores = append(store.stores, s)
	}

	return store.Listen(":19503")
}

func (s *TCPStore) Listen(bind string) error {
	l, err := net.Listen("tcp", bind)
	if err != nil {
		return err
	}
	defer l.Close()

	log.Printf("Listening on %s", bind)

	var eg errgroup.Group

	for i:=0;i<*acceptWorkers;i++ {
		eg.Go(func() error {
			for {
				// Wait for a connection.
				conn, err := l.Accept()
				if err != nil {
					return err
				}

				go s.workerHandler(conn.(*net.TCPConn))
			}
		})
	}

	return eg.Wait()
}

func (s *TCPStore) getShardStore(key string) Store {
	if len(s.stores) == 1 {
		return s.stores[0]
	}
	id := xxhash.Sum64String(key)
	return s.stores[id % uint64(len(s.stores))]
}

func readByte(r io.Reader) (byte, error) {
	var data [1]byte
	_, err := r.Read(data[:])
	if err != nil {
		return 0, err
	}
	return data[0], nil
}

func writeByte(w io.Writer, data byte) error {
	var buf [1]byte
	buf[0] = data
	_, err := w.Write(buf[:])
	return err
}

func readUint32(r io.Reader) (uint32, error) {
	var data [4]byte
	_, err := r.Read(data[:])
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(data[:]), nil
}

func writeUint32(w io.Writer, data uint32) error {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], data)
	_, err := w.Write(buf[:])
	return err
}

func readUint64(r io.Reader) (uint64, error) {
	var data [8]byte
	_, err := r.Read(data[:])
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(data[:]), nil
}

func writeUint64(w io.Writer, data uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], data)
	_, err := w.Write(buf[:])
	return err
}

func readInt64(r io.Reader) (int64, error) {
	var data [8]byte
	_, err := r.Read(data[:])
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(data[:])), nil
}

func writeInt64(w io.Writer, data int64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(data))
	_, err := w.Write(buf[:])
	return err
}

func int64ToBytes(i int64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(i))
	return buf[:]
}

func bytesToInt64(b []byte) int64 {
	return int64(binary.LittleEndian.Uint64(b))
}

func readString(r io.Reader) (string, error) {
	length, err := readUint64(r)
	if err != nil {
		return "", err
	}

	buf := make([]byte, length)
	_, err = r.Read(buf)
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

func writeString(w io.Writer, data string) error {
	length := uint64(len(data))
	if err := writeUint64(w, length); err != nil {
		return err
	}

	_, err := w.Write([]byte(data))
	return err
}

func readStrings(r io.Reader) ([]string, error) {
	length, err := readUint64(r)
	if err != nil {
		return nil, err
	}

	keys := make([]string, length)
	for i := range keys {
		key, err := readString(r)
		if err != nil {
			return nil, err
		}
		keys[i] = key
	}

	return keys, nil
}

func (s *TCPStore) workerHandler(c *net.TCPConn) {
	defer c.Close()

	c.SetNoDelay(true)

	reader := bufio.NewReader(c)
	writer := bufio.NewWriter(c)
	rw := bufio.NewReadWriter(reader, writer)

	for {
		if err := s.processCommand(rw); err != nil{
			if err != io.EOF {
				log.Printf("Error processing command: %v", err)
			}
			break
		}
		if err := writer.Flush(); err != nil {
			if err != io.EOF {
				log.Printf("Error flushing writer: %v", err)
			}
			break
		}
	}
}

func (s *TCPStore) processCommand(c *bufio.ReadWriter) error {
	cmd, err := readByte(c)
	if err != nil {
		return err
	}

	switch QueryType(cmd) {
	case VALIDATE:
		data, err := readUint32(c)
		if err != nil {
			return err
		}
		if data != validationMagicNumber {
			return fmt.Errorf("invalid magic number %x", data)
		}

		return nil
	case PING:
		data, err := readUint32(c)
		if err != nil {
			return err
		}
		if err := writeUint32(c, data); err != nil {
			return err
		}
		return nil

	case SET:
		key, err := readString(c)
		if err != nil {
			return err
		}
		value, err := readString(c)
		if err != nil {
			return err
		}

        return s.getShardStore(key).Set(key, []byte(value))

	case WAIT:
		keys, err := readStrings(c)
		if err != nil {
			return err
		}

		if len(s.stores) == 0 {
			if err := s.stores[0].Wait(keys); err != nil {
				return err
			}
		} else {
			for _, key := range keys {
				if err := s.getShardStore(key).Wait([]string{key}); err != nil {
					return err
				}
			}
		}

		return writeByte(c, byte(STOP_WAITING))

	case GET:
		key, err := readString(c)
		if err != nil {
			return err
		}

		value, err := s.getShardStore(key).Get(key)
		if err != nil {
			return err
		}

		if err := writeString(c, string(value)); err != nil {
			return err
		}

		return nil

	case ADD:
		key, err := readString(c)
		if err != nil {
			return err
		}
		incr, err := readInt64(c)
		if err != nil {
			return err
		}

		v, err := s.getShardStore(key).Add(key, incr)
		if err != nil {
			return err
		}

		if err := writeInt64(c, v); err != nil {
			return err
		}

		return nil

	default:
		return fmt.Errorf("unknown command %d", cmd)
	}
}

func main() {
	log.SetFlags(log.Flags() | log.Lshortfile)
	flag.Parse()
	runtime.GOMAXPROCS(*threads)

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
