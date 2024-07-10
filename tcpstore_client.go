package main

import (
	"os"
	"net"
	"bufio"
	"fmt"
	"math/rand"
)

type TCPStoreClient struct{
	conn net.Conn
	rw *bufio.ReadWriter
}

func Dial(addr string) (*TCPStoreClient, error) {
	if addr[0] == ':' {
		// assign random ip in the loopback range to avoid socket limits
		addr = fmt.Sprintf("127.0.0.%d%s", rand.Int() % 255 + 1, addr)
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c := conn.(*net.TCPConn)
	c.SetNoDelay(true)

	reader := bufio.NewReader(c)
	writer := bufio.NewWriter(c)
	rw := bufio.NewReadWriter(reader, writer)

	client := &TCPStoreClient{conn: c, rw: rw}
	if err := client.Validate(); err != nil {
		return nil, err
	}
	if err := client.Ping(); err != nil {
		return nil, err
	}
	return client, nil
}

func (c *TCPStoreClient) Close() error {
	return c.conn.Close()
}

func (c *TCPStoreClient) Validate() error {
	if err := writeByte(c.rw, byte(VALIDATE)); err != nil {
		return err
	}
	if err := writeUint32(c.rw, validationMagicNumber); err != nil {
		return err
	}
	return c.rw.Flush()
}

func (c *TCPStoreClient) Ping() error {
	if err := writeByte(c.rw, byte(PING)); err != nil {
		return err
	}
	pid := uint32(os.Getpid())
	if err := writeUint32(c.rw, pid); err != nil {
		return err
	}
	if err := c.rw.Flush(); err != nil {
		return err
	}

	data, err := readUint32(c.rw)
	if err != nil {
		return err
	}
	if data != pid {
		return fmt.Errorf("ping response %d != %d", data, pid)
	}
	return nil
}

func (c *TCPStoreClient) Wait(keys []string) error {
	if err := writeByte(c.rw, byte(WAIT)); err != nil {
		return err
	}
	if err := writeStrings(c.rw, keys); err != nil {
		return err
	}
	if err := c.rw.Flush(); err != nil {
		return err
	}

	data, err := readByte(c.rw)
	if err != nil {
		return err
	}
	if WaitResponseType(data) != STOP_WAITING {
		return fmt.Errorf("wait response %d != %d", data, STOP_WAITING)
	}
	return nil
}

func (c *TCPStoreClient) Get(key string) ([]byte, error) {
	if err := c.Wait([]string{key}); err != nil {
		return nil, err
	}

	if err := writeByte(c.rw, byte(GET)); err != nil {
		return nil, err
	}
	if err := writeString(c.rw, key); err != nil {
		return nil, err
	}
	if err := c.rw.Flush(); err != nil {
		return nil, err
	}

	data, err := readString(c.rw)
	if err != nil {
		return nil, err
	}
	return []byte(data), nil
}

func (c *TCPStoreClient) Set(key string, value []byte) error {
	if err := writeByte(c.rw, byte(SET)); err != nil {
		return err
	}
	if err := writeString(c.rw, key); err != nil {
		return err
	}
	if err := writeString(c.rw, string(value)); err != nil {
		return err
	}
	if err := c.rw.Flush(); err != nil {
		return err
	}
	return nil
}

func (c *TCPStoreClient) Add(key string, incr int64) (int64, error) {
	if err := writeByte(c.rw, byte(ADD)); err != nil {
		return 0, err
	}
	if err := writeString(c.rw, key); err != nil {
		return 0, err
	}
	if err := writeInt64(c.rw, incr); err != nil {
		return 0, err
	}
	if err := c.rw.Flush(); err != nil {
		return 0, err
	}

	data, err := readInt64(c.rw)
	if err != nil {
		return 0, err
	}
	return data, nil
}
