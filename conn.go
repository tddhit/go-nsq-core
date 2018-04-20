package nsq

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tddhit/tools/log"
)

type IdentifyResponse struct {
	MaxRdyCount int64 `json:"max_rdy_count"`
}

type msgResponse struct {
	msg     *Message
	cmd     *Command
	success bool
	backoff bool
}

type Conn struct {
	messagesInFlight int64 //投递但未确认的消息数量
	maxRdyCount      int64
	rdyCount         int64 //剩余还未收到的消息数量
	lastRdyCount     int64 //最后一次发送的RDY数量
	lastRdyTimestamp int64
	lastMsgTimestamp int64
	mtx              sync.Mutex
	config           *Config
	conn             *net.TCPConn
	addr             string
	delegate         ConnDelegate
	r                io.Reader
	w                io.Writer
	cmdChan          chan *Command
	msgResponseChan  chan *msgResponse
	exitChan         chan struct{}
	drainReady       chan struct{}
	closeFlag        int32
	stopper          sync.Once
	wg               sync.WaitGroup
	readLoopRunning  int32
}

func NewConn(addr string, config *Config, delegate ConnDelegate) *Conn {
	if !config.initialized {
		panic("Config must be created with NewConfig()")
	}
	return &Conn{
		addr:             addr,
		config:           config,
		delegate:         delegate,
		maxRdyCount:      2500,
		lastMsgTimestamp: time.Now().UnixNano(),
		cmdChan:          make(chan *Command),
		msgResponseChan:  make(chan *msgResponse),
		exitChan:         make(chan struct{}),
		drainReady:       make(chan struct{}),
	}
}

func (c *Conn) Connect() (*IdentifyResponse, error) {
	dialer := &net.Dialer{
		LocalAddr: c.config.LocalAddr,
		Timeout:   c.config.DialTimeout,
	}
	conn, err := dialer.Dial("tcp", c.addr)
	if err != nil {
		return nil, err
	}
	c.conn = conn.(*net.TCPConn)
	c.r = conn
	c.w = conn
	_, err = c.Write(MagicV2)
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("[%s] failed to write magic - %s", c.addr, err)
	}
	resp, err := c.identify()
	if err != nil {
		return nil, err
	}
	c.wg.Add(2)
	atomic.StoreInt32(&c.readLoopRunning, 1)
	go c.readLoop()
	go c.writeLoop()
	return resp, nil
}

func (c *Conn) Close() error {
	atomic.StoreInt32(&c.closeFlag, 1)
	if c.conn != nil && atomic.LoadInt64(&c.messagesInFlight) == 0 {
		return c.conn.CloseRead()
	}
	return nil
}

func (c *Conn) IsClosing() bool {
	return atomic.LoadInt32(&c.closeFlag) == 1
}

func (c *Conn) RDY() int64 {
	return atomic.LoadInt64(&c.rdyCount)
}

func (c *Conn) LastRDY() int64 {
	return atomic.LoadInt64(&c.lastRdyCount)
}

func (c *Conn) SetRDY(rdy int64) {
	atomic.StoreInt64(&c.rdyCount, rdy)
	atomic.StoreInt64(&c.lastRdyCount, rdy)
	if rdy > 0 {
		atomic.StoreInt64(&c.lastRdyTimestamp, time.Now().UnixNano())
	}
}

func (c *Conn) MaxRDY() int64 {
	return c.maxRdyCount
}

func (c *Conn) LastRdyTime() time.Time {
	return time.Unix(0, atomic.LoadInt64(&c.lastRdyTimestamp))
}

func (c *Conn) LastMessageTime() time.Time {
	return time.Unix(0, atomic.LoadInt64(&c.lastMsgTimestamp))
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Conn) String() string {
	return c.addr
}

func (c *Conn) Read(p []byte) (int, error) {
	c.conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
	return c.r.Read(p)
}

func (c *Conn) Write(p []byte) (int, error) {
	c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	return c.w.Write(p)
}

func (c *Conn) WriteCommand(cmd *Command) error {
	c.mtx.Lock()
	_, err := cmd.WriteTo(c)
	if err != nil {
		goto exit
	}
	err = c.Flush()
exit:
	c.mtx.Unlock()
	if err != nil {
		log.Errorf("IO error - %s", err)
		c.delegate.OnIOError(c, err)
	}
	return err
}

type flusher interface {
	Flush() error
}

func (c *Conn) Flush() error {
	if f, ok := c.w.(flusher); ok {
		return f.Flush()
	}
	return nil
}

func (c *Conn) identify() (*IdentifyResponse, error) {
	ci := make(map[string]interface{})
	ci["client_id"] = c.config.ClientID
	ci["hostname"] = c.config.Hostname
	ci["user_agent"] = c.config.UserAgent
	ci["short_id"] = c.config.ClientID // deprecated
	ci["long_id"] = c.config.Hostname  // deprecated
	if c.config.HeartbeatInterval == -1 {
		ci["heartbeat_interval"] = -1
	} else {
		ci["heartbeat_interval"] = int64(c.config.HeartbeatInterval / time.Millisecond)
	}
	ci["sample_rate"] = c.config.SampleRate
	ci["output_buffer_size"] = c.config.OutputBufferSize
	if c.config.OutputBufferTimeout == -1 {
		ci["output_buffer_timeout"] = -1
	} else {
		ci["output_buffer_timeout"] = int64(c.config.OutputBufferTimeout / time.Millisecond)
	}
	ci["msg_timeout"] = int64(c.config.MsgTimeout / time.Millisecond)
	cmd, err := Identify(ci)
	if err != nil {
		return nil, ErrIdentify{err.Error()}
	}

	err = c.WriteCommand(cmd)
	if err != nil {
		return nil, ErrIdentify{err.Error()}
	}

	frameType, data, err := ReadUnpackedResponse(c)
	if err != nil {
		return nil, ErrIdentify{err.Error()}
	}

	if frameType == FrameTypeError {
		return nil, ErrIdentify{string(data)}
	}

	if data[0] != '{' {
		return nil, nil
	}

	resp := &IdentifyResponse{}
	err = json.Unmarshal(data, resp)
	if err != nil {
		return nil, ErrIdentify{err.Error()}
	}

	log.Debugf("IDENTIFY response: %+v", resp)

	c.maxRdyCount = resp.MaxRdyCount

	c.r = bufio.NewReader(c.r)
	if _, ok := c.w.(flusher); !ok {
		c.w = bufio.NewWriter(c.w)
	}

	return resp, nil
}

func (c *Conn) readLoop() {
	delegate := &connMessageDelegate{c}
	for {
		if atomic.LoadInt32(&c.closeFlag) == 1 {
			goto exit
		}
		frameType, data, err := ReadUnpackedResponse(c)
		if err != nil {
			if err == io.EOF && atomic.LoadInt32(&c.closeFlag) == 1 {
				goto exit
			}
			if !strings.Contains(err.Error(), "use of closed network connection") {
				log.Errorf("IO error - %s", err)
				c.delegate.OnIOError(c, err)
			}
			goto exit
		}
		if frameType == FrameTypeResponse && bytes.Equal(data, []byte("_heartbeat_")) {
			log.Debug("heartbeat received")
			c.delegate.OnHeartbeat(c)
			if err := c.WriteCommand(Nop()); err != nil {
				log.Errorf("IO error - %s", err)
				c.delegate.OnIOError(c, err)
				goto exit
			}
			continue
		}
		switch frameType {
		case FrameTypeResponse:
			c.delegate.OnResponse(c, data)
		case FrameTypeMessage:
			msg, err := DecodeMessage(data)
			if err != nil {
				log.Errorf("IO error - %s", err)
				c.delegate.OnIOError(c, err)
				goto exit
			}
			msg.Delegate = delegate
			msg.NSQDAddress = c.String()
			atomic.AddInt64(&c.rdyCount, -1)
			atomic.AddInt64(&c.messagesInFlight, 1)
			atomic.StoreInt64(&c.lastMsgTimestamp, time.Now().UnixNano())
			c.delegate.OnMessage(c, msg)
		case FrameTypeError:
			log.Errorf("protocol error - %s", data)
			c.delegate.OnError(c, data)
		default:
			log.Errorf("IO error - %s", err)
			c.delegate.OnIOError(c, fmt.Errorf("unknown frame type %d", frameType))
		}
	}
exit:
	atomic.StoreInt32(&c.readLoopRunning, 0)
	messagesInFlight := atomic.LoadInt64(&c.messagesInFlight)
	if messagesInFlight == 0 {
		c.close()
	} else {
		log.Warnf("delaying close, %d outstanding messages", messagesInFlight)
	}
	c.wg.Done()
	log.Info("readLoop exiting")
}

func (c *Conn) writeLoop() {
	for {
		select {
		case <-c.exitChan:
			log.Info("breaking out of writeLoop")
			close(c.drainReady)
			goto exit
		case cmd := <-c.cmdChan:
			if err := c.WriteCommand(cmd); err != nil {
				log.Errorf("error sending command %s - %s", cmd, err)
				c.close()
				continue
			}
		case resp := <-c.msgResponseChan:
			msgsInFlight := atomic.AddInt64(&c.messagesInFlight, -1)
			if resp.success {
				log.Debugf("FIN %s", resp.msg.ID)
				c.delegate.OnMessageFinished(c, resp.msg)
				c.delegate.OnResume(c)
			} else {
				log.Debugf("REQ %s", resp.msg.ID)
				c.delegate.OnMessageRequeued(c, resp.msg)
				if resp.backoff {
					c.delegate.OnBackoff(c)
				} else {
					c.delegate.OnContinue(c)
				}
			}
			if err := c.WriteCommand(resp.cmd); err != nil {
				log.Errorf("error sending command %s - %s", resp.cmd, err)
				c.close()
				continue
			}
			if msgsInFlight == 0 && atomic.LoadInt32(&c.closeFlag) == 1 {
				c.close()
				continue
			}
		}
	}

exit:
	c.wg.Done()
	log.Info("writeLoop exiting")
}

func (c *Conn) close() {
	c.stopper.Do(func() {
		log.Info("beginning close")
		close(c.exitChan)
		c.conn.CloseRead()
		c.wg.Add(1)
		go c.cleanup()
		go c.waitForCleanup()
	})
}

func (c *Conn) cleanup() {
	<-c.drainReady
	ticker := time.NewTicker(100 * time.Millisecond)
	lastWarning := time.Now()
	for {
		var msgsInFlight int64
		select {
		case <-c.msgResponseChan:
			msgsInFlight = atomic.AddInt64(&c.messagesInFlight, -1)
		case <-ticker.C:
			msgsInFlight = atomic.LoadInt64(&c.messagesInFlight)
		}
		if msgsInFlight > 0 {
			if time.Now().Sub(lastWarning) > time.Second {
				log.Warnf("draining... waiting for %d messages in flight", msgsInFlight)
				lastWarning = time.Now()
			}
			continue
		}
		if atomic.LoadInt32(&c.readLoopRunning) == 1 {
			if time.Now().Sub(lastWarning) > time.Second {
				log.Warnf("draining... readLoop still running")
				lastWarning = time.Now()
			}
			continue
		}
		goto exit
	}

exit:
	ticker.Stop()
	c.wg.Done()
	log.Info("finished draining, cleanup exiting")
}

func (c *Conn) waitForCleanup() {
	c.wg.Wait()
	c.conn.CloseWrite()
	log.Info("clean close complete")
	c.delegate.OnClose(c)
}

func (c *Conn) onMessageFinish(m *Message) {
	c.msgResponseChan <- &msgResponse{msg: m, cmd: Finish(m.ID), success: true}
}

func (c *Conn) onMessageRequeue(m *Message, delay time.Duration, backoff bool) {
	if delay == -1 {
		delay = c.config.DefaultRequeueDelay * time.Duration(m.Attempts)
		if delay > c.config.MaxRequeueDelay {
			delay = c.config.MaxRequeueDelay
		}
	}
	c.msgResponseChan <- &msgResponse{msg: m, cmd: Requeue(m.ID, delay), success: false, backoff: backoff}
}

func (c *Conn) onMessageTouch(m *Message) {
	select {
	case c.cmdChan <- Touch(m.ID):
	case <-c.exitChan:
	}
}
