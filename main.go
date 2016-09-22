package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/tidwall/finn"
	"github.com/tidwall/raft-redcon"
	"github.com/tidwall/redcon"
	"github.com/tidwall/redlog"
)

func main() {
	var port int
	var backend string
	var durability string
	var consistency string
	var loglevel string
	var join string
	var dir string

	flag.IntVar(&port, "p", 7481, "Bind port")
	flag.StringVar(&backend, "backend", "fastlog", "Raft log backend [fastlog,bolt,inmem]")
	flag.StringVar(&durability, "durability", "medium", "Log durability [low,medium,high]")
	flag.StringVar(&consistency, "consistency", "medium", "Raft consistency [low,medium,high]")
	flag.StringVar(&loglevel, "loglevel", "notice", "Log level [quiet,warning,notice,verbose,debug]")
	flag.StringVar(&dir, "dir", "data", "Data directory")
	flag.StringVar(&join, "join", "", "Join a cluster by providing an address")
	flag.Parse()

	var opts finn.Options

	switch strings.ToLower(backend) {
	default:
		log.Fatalf("invalid backend '%v'", backend)
	case "fastlog":
		opts.Backend = finn.FastLog
	case "bolt":
		opts.Backend = finn.Bolt
	case "inmem":
		opts.Backend = finn.InMem
	}
	switch strings.ToLower(durability) {
	default:
		log.Fatalf("invalid durability '%v'", durability)
	case "low":
		opts.Durability = finn.Low
	case "medium":
		opts.Durability = finn.Medium
	case "high":
		opts.Durability = finn.High
	}
	switch strings.ToLower(consistency) {
	default:
		log.Fatalf("invalid consistency '%v'", consistency)
	case "low":
		opts.Consistency = finn.Low
	case "medium":
		opts.Consistency = finn.Medium
	case "high":
		opts.Consistency = finn.High
	}
	opts.LogOutput = os.Stdout
	switch strings.ToLower(loglevel) {
	default:
		log.Fatalf("invalid loglevel '%v'", loglevel)
	case "quiet":
		opts.LogOutput = ioutil.Discard
		loglevel = "warning" // reassign for redis
	case "warning":
		opts.LogLevel = finn.Warning
	case "notice":
		opts.LogLevel = finn.Notice
	case "verbose":
		opts.LogLevel = finn.Verbose
	case "debug":
		opts.LogLevel = finn.Debug
	}

	// Store appendonly.aof and unix.sock in a temp directory
	tmpdir, err := ioutil.TempDir("", "redis")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	// make redis executable path
	wd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	redisServerPath := "redis-server"
	fi, err := os.Stat(path.Join(wd, redisServerPath))
	if err == nil && !fi.IsDir() {
		redisServerPath = path.Join(wd, redisServerPath)
	}

	// Create the Redis command line
	cmd := exec.Command(redisServerPath,
		"--port", "0",
		"--bind", "127.0.0.1",
		"--unixsocketperm", "755",
		"--unixsocket", path.Join(tmpdir, "unix.sock"),
		"--appendonly", "yes",
		"--syslog-enabled", "yes",
		"--save", "",
		"--loglevel", loglevel,
	)
	cmd.Dir = tmpdir
	cmd.Stdout = redlog.RedisLogColorizer(opts.LogOutput)
	cmd.Stderr = os.Stderr

	// Start the Redis processes
	err = cmd.Start()
	if err != nil {
		log.Fatal(err)
	}
	defer cmd.Process.Kill()

	// Watch for forced termination such as Ctrl-C.
	ch := make(chan os.Signal)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		// Send a SIGTERM to Redis will force the process to end and
		// tear down Redis and Raft.
		cmd.Process.Signal(syscall.SIGTERM)
	}()

	// Start up Finn
	rr, err := NewRedRaft(cmd, tmpdir, &opts)
	if err != nil {
		log.Fatal(err)
	}
	n, err := finn.Open(dir, fmt.Sprintf(":%d", port), join, rr, &opts)
	if err != nil {
		log.Fatal(err)
	}
	defer n.Close()

	// Wait for the Redis process to complete.
	cmd.Wait()
}

type RedRaft struct {
	tmpdir string
	mu     sync.RWMutex
	rconn  net.Conn
	rd     *bufio.Reader
	cmd    *exec.Cmd
}
type connCtx struct {
	rconn net.Conn
	rd    *bufio.Reader
}

func NewRedRaft(
	cmd *exec.Cmd, tmpdir string, opts *finn.Options,
) (*RedRaft, error) {
	rr := &RedRaft{
		tmpdir: tmpdir,
		cmd:    cmd,
	}
	unixSock := path.Join(tmpdir, "unix.sock")
	start := time.Now()
	for {
		// Interprocess connection to Redis.
		rconn, err := net.Dial("unix", unixSock)
		if err != nil {
			if time.Now().Sub(start) > time.Second*5 {
				// 5 second timeout waiting to connect.
				return nil, err
			}
			time.Sleep(time.Millisecond * 100)
			continue
		}
		rr.rconn = rconn
		break
	}
	rr.rd = bufio.NewReader(rr.rconn)
	opts.ConnAccept = func(conn redcon.Conn) bool {
		rconn, err := net.Dial("unix", unixSock)
		if err != nil {
			return false
		}
		conn.SetContext(&connCtx{rconn, bufio.NewReader(rconn)})
		return true
	}
	opts.ConnClosed = func(conn redcon.Conn, err error) {
		conn.Context().(*connCtx).rconn.Close()
	}
	return rr, nil
}

const (
	unknown = 0
	read    = 1
	write   = 2
)

func commandKind(cmd redcon.Command) int {
	switch strings.ToLower(string(cmd.Args[0])) {
	default:
		return unknown
	case // Read commands
		"get", "strlen", "exists", "getbit", "getrange", "substr", "mget",
		"llen", "lindex", "lrange", "sismember", "scard", "srandmember",
		"sinter", "sunion", "sdiff", "smembers", "sscan", "zrange",
		"zrangebyscore", "zrevrangebyscore", "zrangebylex", "zrevrangebylex",
		"zcount", "zlexcount", "zrevrange", "zcard", "zscore", "zrank",
		"zrevrank", "zscan", "hget", "hmget", "hlen", "hstrlen", "hkeys",
		"hvals", "hgetall", "hexists", "hscan", "randomkey", "keys", "scan",
		"dbsize", "echo", "type", "info", "ttl", "pttl", "dump", "object",
		"memory", "time", "bitpos", "bitcount", "geohash", "geopos", "geodist",
		"pfcount":
		return read
	case // Write commands
		"set", "setnx", "setex", "psetex", "append", "del", "setbit",
		"bitfield", "setrange", "incr", "decr", "rpush", "lpush", "rpushx",
		"lpushx", "linsert", "rpop", "lpop", "brpop", "brpoplpush", "blpop",
		"lset", "ltrim", "lrem", "rpoplpush", "sadd", "srem", "smove", "spop",
		"sinterstore", "sunionstore", "sdiffstore", "zadd", "zincrby", "zrem",
		"zremrangebyscore", "zremrangebyrank", "zremrangebylex", "zunionstore",
		"zinterstore", "hset", "hsetnx", "hmset", "hincrby", "hincrbyfloat",
		"hdel", "incrby", "decrby", "incrbyfloat", "getset", "mset", "msetnx",
		"select", "move", "rename", "renamenx", "expire", "expireat", "pexpire",
		"pexpireat", "auth", "flushdb", "flushall", "sort", "persist", "config",
		"restore", "migrate", "bitop", "geoadd", "georadius",
		"georadiusbymember", "pfadd", "pfmerge":
		return write
	}
}

func (rr *RedRaft) Command(m finn.Applier, conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	switch commandKind(cmd) {
	default:
		return nil, finn.ErrUnknownCommand

	case write:
		return m.Apply(conn, cmd,
			func() (interface{}, error) {
				rr.mu.Lock()
				defer rr.mu.Unlock()
				resp, err := rr.do(conn, cmd.Raw)
				if err != nil {
					return nil, err
				}
				return resp, nil
			},
			func(v interface{}) (interface{}, error) {
				conn.WriteRaw(v.([]byte))
				return nil, nil
			},
		)

	case read:
		return m.Apply(conn, cmd, nil,
			func(v interface{}) (interface{}, error) {
				rr.mu.RLock()
				defer rr.mu.RUnlock()
				resp, err := rr.do(conn, cmd.Raw)
				if err != nil {
					return nil, err
				}
				conn.WriteRaw(resp)
				return nil, nil
			},
		)
	}
	return nil, nil
}

// do will proxy the input command to the Redis process. A failed
// read or write indicates a broken pipe and will cause a panic.
func (rr *RedRaft) do(conn redcon.Conn, cmd []byte) (resp []byte, err error) {
	var rconn net.Conn
	var rd *bufio.Reader
	if conn == nil {
		rconn = rr.rconn
		rd = rr.rd
	} else {
		ctx := conn.Context().(*connCtx)
		rconn = ctx.rconn
		rd = ctx.rd
	}
	if _, err = rconn.Write(cmd); err == nil {
		var kind byte
		resp, kind, err = raftredcon.ReadRawResponse(rd)
		if err == nil {
			if kind == '-' {
				err = errors.New(strings.TrimSpace(string(resp[1:])))
				return
			}
			return
		}
	}
	fmt.Fprintf(os.Stderr, "panic: %v\n", err)
	rr.cmd.Process.Signal(syscall.SIGTERM)
	return
}

// Restore restores a snapshot
func (rr *RedRaft) Restore(rd io.Reader) error {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	conn, err := net.Dial("unix", path.Join(rr.tmpdir, "unix.sock"))
	if err != nil {
		return err
	}
	defer conn.Close()
	var crd = bufio.NewReader(conn)
	var buf []byte
	var count int
	// issue a FLUSHALL to delete all keys from Redis process.
	buf = append(buf, "*1\r\n$8\r\nFLUSHALL\r\n"...)
	count++
	flush := func() error {
		if _, err := conn.Write(buf); err != nil {
			return err
		}
		for i := 0; i < count; i++ {
			_, _, err := raftredcon.ReadRawResponse(crd)
			if err != nil {
				return err
			}
		}
		buf = buf[:0]
		count = 0
		return nil
	}
	var brd = bufio.NewReader(rd)
	for {
		raw, kind, err := raftredcon.ReadRawResponse(brd)
		if err != nil {
			if err == io.EOF {
				if err := flush(); err != nil {
					return err
				}
				break
			}
			return err
		}
		if kind != '*' {
			return errors.New("invalid command")
		}
		buf = append(buf, raw...)
		count++
		if len(buf) >= 16*1024*1024 {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return nil
	//	return finn.ErrDisabled
}

// Snapshot creates a snapshot
func (rr *RedRaft) Snapshot(wr io.Writer) error {
	// Open the file and read the file info behind a lock.
	// We'll use the file size this to limit the amount of writing.
	var end int64
	var f *os.File
	var err error
	func() {
		rr.mu.Lock()
		defer rr.mu.Unlock()
		f, err = os.Open(path.Join(rr.tmpdir, "appendonly.aof"))
		if err == nil {
			var fi os.FileInfo
			fi, err = f.Stat()
			if err != nil {
				f.Close()
				return
			}
			end = fi.Size()
		}
	}()
	if err != nil {
		return err
	}
	defer f.Close()
	// copy the file to the writer
	_, err = io.CopyN(wr, f, end)
	return err
}
