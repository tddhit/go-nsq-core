package nsq

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tddhit/tools/log"
)

type Handler interface {
	HandleMessage(message *Message) error
}

type HandlerFunc func(message *Message) error

func (h HandlerFunc) HandleMessage(m *Message) error {
	return h(m)
}

var instCount int64

type backoffSignal int

const (
	backoffFlag backoffSignal = iota
	continueFlag
	resumeFlag
)

type Consumer struct {
	id                   int64
	mtx                  sync.RWMutex
	topic                string
	channel              string
	messagesReceived     uint64
	messagesFinished     uint64
	messagesRequeued     uint64
	totalRdyCount        int64
	backoffDuration      int64
	backoffCounter       int32
	maxInFlight          int32
	config               Config
	rngMtx               sync.Mutex
	rng                  *rand.Rand
	needRDYRedistributed int32
	backoffMtx           sync.RWMutex
	incomingMessages     chan *Message
	rdyRetryMtx          sync.RWMutex
	rdyRetryTimers       map[string]*time.Timer
	pendingConnections   map[string]*Conn
	connections          map[string]*Conn
	nsqdTCPAddrs         []string
	wg                   sync.WaitGroup
	runningHandlers      int32
	stopFlag             int32
	connectedFlag        int32
	stopHandler          sync.Once
	exitHandler          sync.Once
	StopChan             chan struct{}
	exitChan             chan struct{}
}

func NewConsumer(topic string, channel string, config *Config) (*Consumer, error) {
	config.assertInitialized()
	if !IsValidTopicName(topic) {
		return nil, errors.New("invalid topic name")
	}
	if !IsValidChannelName(channel) {
		return nil, errors.New("invalid channel name")
	}
	r := &Consumer{
		id:                 atomic.AddInt64(&instCount, 1),
		topic:              topic,
		channel:            channel,
		config:             *config,
		maxInFlight:        int32(config.MaxInFlight),
		incomingMessages:   make(chan *Message),
		rdyRetryTimers:     make(map[string]*time.Timer),
		pendingConnections: make(map[string]*Conn),
		connections:        make(map[string]*Conn),
		rng:                rand.New(rand.NewSource(time.Now().UnixNano())),
		StopChan:           make(chan struct{}),
		exitChan:           make(chan struct{}),
	}
	r.wg.Add(1)
	go r.rdyLoop()
	return r, nil
}

func (r *Consumer) conns() []*Conn {
	r.mtx.RLock()
	conns := make([]*Conn, 0, len(r.connections))
	for _, c := range r.connections {
		conns = append(conns, c)
	}
	r.mtx.RUnlock()
	return conns
}

func (r *Consumer) perConnMaxInFlight() int64 {
	b := float64(r.getMaxInFlight())
	s := b / float64(len(r.conns()))
	return int64(math.Min(math.Max(1, s), b))
}

func (r *Consumer) getMaxInFlight() int32 {
	return atomic.LoadInt32(&r.maxInFlight)
}

func (r *Consumer) ConnectToNSQDs(addresses []string) error {
	for _, addr := range addresses {
		if err := r.ConnectToNSQD(addr); err != nil {
			return err
		}
	}
	return nil
}

func (r *Consumer) ConnectToNSQD(addr string) error {
	if atomic.LoadInt32(&r.stopFlag) == 1 {
		return errors.New("consumer stopped")
	}
	if atomic.LoadInt32(&r.runningHandlers) == 0 {
		return errors.New("no handlers")
	}
	atomic.StoreInt32(&r.connectedFlag, 1)

	r.mtx.Lock()
	_, pendingOk := r.pendingConnections[addr]
	_, ok := r.connections[addr]
	if ok || pendingOk {
		r.mtx.Unlock()
		return ErrAlreadyConnected
	}
	conn := NewConn(addr, &r.config, &consumerConnDelegate{r})
	r.pendingConnections[addr] = conn
	if idx := indexOf(addr, r.nsqdTCPAddrs); idx == -1 {
		r.nsqdTCPAddrs = append(r.nsqdTCPAddrs, addr)
	}
	r.mtx.Unlock()
	log.Infof("(%s) connecting to nsqd", addr)

	cleanupConnection := func() {
		r.mtx.Lock()
		delete(r.pendingConnections, addr)
		r.mtx.Unlock()
		conn.Close()
	}
	resp, err := conn.Connect()
	if err != nil {
		cleanupConnection()
		return err
	}
	if resp != nil {
		if resp.MaxRdyCount < int64(r.getMaxInFlight()) {
			log.Warnf("(%s) max RDY count %d < consumer max in flight %d, truncation possible",
				conn.String(), resp.MaxRdyCount, r.getMaxInFlight())
		}
	}
	cmd := Subscribe(r.topic, r.channel)
	if err = conn.WriteCommand(cmd); err != nil {
		cleanupConnection()
		return fmt.Errorf("[%s] failed to subscribe to %s:%s - %s",
			conn, r.topic, r.channel, err.Error())
	}

	r.mtx.Lock()
	delete(r.pendingConnections, addr)
	r.connections[addr] = conn
	r.mtx.Unlock()

	for _, c := range r.conns() {
		r.maybeUpdateRDY(c)
	}
	return nil
}

func indexOf(n string, h []string) int {
	for i, a := range h {
		if n == a {
			return i
		}
	}
	return -1
}

func (r *Consumer) DisconnectFromNSQD(addr string) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	idx := indexOf(addr, r.nsqdTCPAddrs)
	if idx == -1 {
		return ErrNotConnected
	}
	r.nsqdTCPAddrs = append(r.nsqdTCPAddrs[:idx], r.nsqdTCPAddrs[idx+1:]...)
	pendingConn, pendingOk := r.pendingConnections[addr]
	conn, ok := r.connections[addr]
	if ok {
		conn.Close()
	} else if pendingOk {
		pendingConn.Close()
	}
	return nil
}

func (r *Consumer) onConnMessage(c *Conn, msg *Message) {
	atomic.AddInt64(&r.totalRdyCount, -1)
	atomic.AddUint64(&r.messagesReceived, 1)
	r.incomingMessages <- msg
	r.maybeUpdateRDY(c)
}

func (r *Consumer) onConnMessageFinished(c *Conn, msg *Message) {
	atomic.AddUint64(&r.messagesFinished, 1)
}

func (r *Consumer) onConnMessageRequeued(c *Conn, msg *Message) {
	atomic.AddUint64(&r.messagesRequeued, 1)
}

func (r *Consumer) onConnBackoff(c *Conn) {
	r.startStopContinueBackoff(c, backoffFlag)
}

func (r *Consumer) onConnContinue(c *Conn) {
	r.startStopContinueBackoff(c, continueFlag)
}

func (r *Consumer) onConnResume(c *Conn) {
	r.startStopContinueBackoff(c, resumeFlag)
}

func (r *Consumer) onConnResponse(c *Conn, data []byte) {
	switch {
	case bytes.Equal(data, []byte("CLOSE_WAIT")):
		log.Infof("(%s) received CLOSE_WAIT from nsqd", c.String())
		c.Close()
	}
}

func (r *Consumer) onConnError(c *Conn, data []byte) {}

func (r *Consumer) onConnHeartbeat(c *Conn) {}

func (r *Consumer) onConnIOError(c *Conn, err error) {
	c.Close()
}

func (r *Consumer) onConnClose(c *Conn) {
	var hasRDYRetryTimer bool
	rdyCount := c.RDY()
	atomic.AddInt64(&r.totalRdyCount, -rdyCount)
	r.rdyRetryMtx.Lock()
	if timer, ok := r.rdyRetryTimers[c.String()]; ok {
		timer.Stop()
		delete(r.rdyRetryTimers, c.String())
		hasRDYRetryTimer = true
	}
	r.rdyRetryMtx.Unlock()
	r.mtx.Lock()
	delete(r.connections, c.String())
	left := len(r.connections)
	r.mtx.Unlock()
	log.Warnf("there are %d connections left alive", left)
	if (hasRDYRetryTimer || rdyCount > 0) &&
		(int32(left) == r.getMaxInFlight() || r.inBackoff()) {
		atomic.StoreInt32(&r.needRDYRedistributed, 1)
	}

	if atomic.LoadInt32(&r.stopFlag) == 1 {
		if left == 0 {
			r.stopHandlers()
		}
		return
	}

	r.mtx.RLock()
	reconnect := indexOf(c.String(), r.nsqdTCPAddrs) >= 0
	r.mtx.RUnlock()
	if reconnect {
		go func(addr string) {
			for {
				if atomic.LoadInt32(&r.stopFlag) == 1 {
					break
				}
				r.mtx.RLock()
				reconnect := indexOf(addr, r.nsqdTCPAddrs) >= 0
				r.mtx.RUnlock()
				if !reconnect {
					log.Warnf("(%s) skipped reconnect after removal...", addr)
					return
				}
				err := r.ConnectToNSQD(addr)
				if err != nil && err != ErrAlreadyConnected {
					log.Errorf("(%s) error connecting to nsqd - %s", addr, err)
					continue
				}
				break
			}
		}(c.String())
	}
}

func (r *Consumer) startStopContinueBackoff(conn *Conn, signal backoffSignal) {
	r.backoffMtx.Lock()
	if r.inBackoffTimeout() {
		r.backoffMtx.Unlock()
		return
	}
	defer r.backoffMtx.Unlock()

	backoffUpdated := false
	backoffCounter := atomic.LoadInt32(&r.backoffCounter)
	switch signal {
	case resumeFlag:
		if backoffCounter > 0 {
			backoffCounter--
			backoffUpdated = true
		}
	case backoffFlag:
		nextBackoff := r.config.BackoffStrategy.Calculate(int(backoffCounter) + 1)
		if nextBackoff <= r.config.MaxBackoffDuration {
			backoffCounter++
			backoffUpdated = true
		}
	}
	atomic.StoreInt32(&r.backoffCounter, backoffCounter)

	if r.backoffCounter == 0 && backoffUpdated {
		count := r.perConnMaxInFlight()
		log.Warnf("exiting backoff, returning all to RDY %d", count)
		for _, c := range r.conns() {
			r.updateRDY(c, count)
		}
	} else if r.backoffCounter > 0 {
		backoffDuration := r.config.BackoffStrategy.Calculate(int(backoffCounter))
		if backoffDuration > r.config.MaxBackoffDuration {
			backoffDuration = r.config.MaxBackoffDuration
		}
		log.Warnf("backing off for %s (backoff level %d), setting all to RDY 0",
			backoffDuration, backoffCounter)
		for _, c := range r.conns() {
			r.updateRDY(c, 0)
		}
		r.backoff(backoffDuration)
	}
}

func (r *Consumer) backoff(d time.Duration) {
	atomic.StoreInt64(&r.backoffDuration, d.Nanoseconds())
	time.AfterFunc(d, r.resume)
}

func (r *Consumer) resume() {
	if atomic.LoadInt32(&r.stopFlag) == 1 {
		atomic.StoreInt64(&r.backoffDuration, 0)
		return
	}
	conns := r.conns()
	if len(conns) == 0 {
		log.Warn("no connection available to resume")
		log.Warnf("backing off for %s", time.Second)
		r.backoff(time.Second)
		return
	}
	r.rngMtx.Lock()
	idx := r.rng.Intn(len(conns))
	r.rngMtx.Unlock()
	choice := conns[idx]

	log.Warnf("(%s) backoff timeout expired, sending RDY 1",
		choice.String())

	if err := r.updateRDY(choice, 1); err != nil {
		log.Warnf("(%s) error resuming RDY 1 - %s", choice.String(), err)
		log.Warnf("backing off for %s", time.Second)
		r.backoff(time.Second)
		return
	}
	atomic.StoreInt64(&r.backoffDuration, 0)
}

func (r *Consumer) inBackoff() bool {
	return atomic.LoadInt32(&r.backoffCounter) > 0
}

func (r *Consumer) inBackoffTimeout() bool {
	return atomic.LoadInt64(&r.backoffDuration) > 0
}

func (r *Consumer) maybeUpdateRDY(conn *Conn) {
	inBackoff := r.inBackoff()
	inBackoffTimeout := r.inBackoffTimeout()
	if inBackoff || inBackoffTimeout {
		log.Debugf("(%s) skip sending RDY inBackoff:%v || inBackoffTimeout:%v",
			conn, inBackoff, inBackoffTimeout)
		return
	}
	remain := conn.RDY()
	lastRdyCount := conn.LastRDY()
	count := r.perConnMaxInFlight()

	if remain <= 1 || remain < (lastRdyCount/4) || (count > 0 && count < remain) {
		log.Debugf("(%s) sending RDY %d (%d remain from last RDY %d)",
			conn, count, remain, lastRdyCount)
		r.updateRDY(conn, count)
	} else {
		log.Debugf("(%s) skip sending RDY %d (%d remain out of last RDY %d)",
			conn, count, remain, lastRdyCount)
	}
}

func (r *Consumer) rdyLoop() {
	redistributeTicker := time.NewTicker(r.config.RDYRedistributeInterval)
	for {
		select {
		case <-redistributeTicker.C:
			r.redistributeRDY()
		case <-r.exitChan:
			goto exit
		}
	}
exit:
	redistributeTicker.Stop()
	log.Info("rdyLoop exiting")
	r.wg.Done()
}

func (r *Consumer) updateRDY(c *Conn, count int64) error {
	if c.IsClosing() {
		return ErrClosing
	}
	if count > c.MaxRDY() {
		count = c.MaxRDY()
	}
	r.rdyRetryMtx.Lock()
	if timer, ok := r.rdyRetryTimers[c.String()]; ok {
		timer.Stop()
		delete(r.rdyRetryTimers, c.String())
	}
	r.rdyRetryMtx.Unlock()

	rdyCount := c.RDY()
	maxPossibleRdy := int64(r.getMaxInFlight()) - atomic.LoadInt64(&r.totalRdyCount) + rdyCount
	if maxPossibleRdy > 0 && maxPossibleRdy < count {
		count = maxPossibleRdy
	}
	if maxPossibleRdy <= 0 && count > 0 {
		if rdyCount == 0 {
			r.rdyRetryMtx.Lock()
			r.rdyRetryTimers[c.String()] = time.AfterFunc(5*time.Second,
				func() {
					r.updateRDY(c, count)
				})
			r.rdyRetryMtx.Unlock()
		}
		return ErrOverMaxInFlight
	}
	return r.sendRDY(c, count)
}

func (r *Consumer) sendRDY(c *Conn, count int64) error {
	if count == 0 && c.LastRDY() == 0 {
		return nil
	}
	atomic.AddInt64(&r.totalRdyCount, -c.RDY()+count)
	c.SetRDY(count)
	if err := c.WriteCommand(Ready(int(count))); err != nil {
		log.Errorf("(%s) error sending RDY %d - %s", c.String(), count, err)
		return err
	}
	return nil
}

func (r *Consumer) redistributeRDY() {
	if r.inBackoffTimeout() {
		return
	}
	conns := r.conns()
	if len(conns) == 0 {
		return
	}
	maxInFlight := r.getMaxInFlight()
	if len(conns) > int(maxInFlight) {
		log.Debugf("redistributing RDY state (%d conns > %d max_in_flight)",
			len(conns), maxInFlight)
		atomic.StoreInt32(&r.needRDYRedistributed, 1)
	}
	if r.inBackoff() && len(conns) > 1 {
		log.Debugf("redistributing RDY state (in backoff and %d conns > 1)", len(conns))
		atomic.StoreInt32(&r.needRDYRedistributed, 1)
	}
	if !atomic.CompareAndSwapInt32(&r.needRDYRedistributed, 1, 0) {
		return
	}
	possibleConns := make([]*Conn, 0, len(conns))
	for _, c := range conns {
		lastMsgDuration := time.Now().Sub(c.LastMessageTime())
		lastRdyDuration := time.Now().Sub(c.LastRdyTime())
		rdyCount := c.RDY()
		log.Debugf("(%s) rdy: %d (last message received %s)",
			c.String(), rdyCount, lastMsgDuration)
		if rdyCount > 0 {
			if lastMsgDuration > r.config.LowRdyIdleTimeout {
				log.Debugf("(%s) idle connection, giving up RDY", c.String())
				r.updateRDY(c, 0)
			} else if lastRdyDuration > r.config.LowRdyTimeout {
				log.Debugf("(%s) RDY timeout, giving up RDY", c.String())
				r.updateRDY(c, 0)
			}
		}
		possibleConns = append(possibleConns, c)
	}
	availableMaxInFlight := int64(maxInFlight) - atomic.LoadInt64(&r.totalRdyCount)
	if r.inBackoff() {
		availableMaxInFlight = 1 - atomic.LoadInt64(&r.totalRdyCount)
	}
	for len(possibleConns) > 0 && availableMaxInFlight > 0 {
		availableMaxInFlight--
		r.rngMtx.Lock()
		i := r.rng.Int() % len(possibleConns)
		r.rngMtx.Unlock()
		c := possibleConns[i]
		possibleConns = append(possibleConns[:i], possibleConns[i+1:]...)
		log.Debugf("(%s) redistributing RDY", c.String())
		r.updateRDY(c, 1)
	}
}

func (r *Consumer) Stop() {
	if !atomic.CompareAndSwapInt32(&r.stopFlag, 0, 1) {
		return
	}
	log.Info("stopping...")
	if len(r.conns()) == 0 {
		r.stopHandlers()
	} else {
		for _, c := range r.conns() {
			if err := c.WriteCommand(StartClose()); err != nil {
				log.Errorf("(%s) error sending CLS - %s", c.String(), err)
			}
		}
		time.AfterFunc(time.Second*30, func() {
			r.exit()
		})
	}
}

func (r *Consumer) stopHandlers() {
	r.stopHandler.Do(func() {
		log.Info("stopping handlers")
		close(r.incomingMessages)
	})
}

func (r *Consumer) AddHandler(handler Handler) {
	r.AddConcurrentHandlers(handler, 1)
}

func (r *Consumer) AddConcurrentHandlers(handler Handler, concurrency int) {
	if atomic.LoadInt32(&r.connectedFlag) == 1 {
		panic("already connected")
	}
	atomic.AddInt32(&r.runningHandlers, int32(concurrency))
	for i := 0; i < concurrency; i++ {
		go r.handlerLoop(handler)
	}
}

func (r *Consumer) handlerLoop(handler Handler) {
	log.Debug("starting Handler")
	for {
		message, ok := <-r.incomingMessages
		if !ok {
			goto exit
		}
		if r.shouldFailMessage(message, handler) {
			message.Finish()
			continue
		}
		if err := handler.HandleMessage(message); err != nil {
			log.Errorf("Handler returned error (%s) for msg %s", err, message.ID)
			if !message.IsAutoResponseDisabled() {
				message.Requeue(-1)
			}
			continue
		}
		if !message.IsAutoResponseDisabled() {
			message.Finish()
		}
	}
exit:
	log.Debug("stopping Handler")
	if atomic.AddInt32(&r.runningHandlers, -1) == 0 {
		r.exit()
	}
}

func (r *Consumer) shouldFailMessage(message *Message, handler interface{}) bool {
	if r.config.MaxAttempts > 0 && message.Attempts > r.config.MaxAttempts {
		log.Warnf("msg %s attempted %d times, giving up",
			message.ID, message.Attempts)
		return true
	}
	return false
}

func (r *Consumer) exit() {
	r.exitHandler.Do(func() {
		close(r.exitChan)
		r.wg.Wait()
		close(r.StopChan)
	})
}
