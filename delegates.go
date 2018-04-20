package nsq

import "time"

type MessageDelegate interface {
	OnFinish(*Message)
	OnRequeue(m *Message, delay time.Duration, backoff bool)
	OnTouch(*Message)
}

type connMessageDelegate struct {
	c *Conn
}

func (d *connMessageDelegate) OnFinish(m *Message) { d.c.onMessageFinish(m) }
func (d *connMessageDelegate) OnRequeue(m *Message, t time.Duration, b bool) {
	d.c.onMessageRequeue(m, t, b)
}
func (d *connMessageDelegate) OnTouch(m *Message) { d.c.onMessageTouch(m) }

type ConnDelegate interface {
	OnResponse(*Conn, []byte)
	OnError(*Conn, []byte)
	OnMessage(*Conn, *Message)
	OnMessageFinished(*Conn, *Message)
	OnMessageRequeued(*Conn, *Message)
	OnBackoff(*Conn)
	OnContinue(*Conn)
	OnResume(*Conn)
	OnIOError(*Conn, error)
	OnHeartbeat(*Conn)
	OnClose(*Conn)
}

type consumerConnDelegate struct {
	r *Consumer
}

func (d *consumerConnDelegate) OnResponse(c *Conn, data []byte)       { d.r.onConnResponse(c, data) }
func (d *consumerConnDelegate) OnError(c *Conn, data []byte)          { d.r.onConnError(c, data) }
func (d *consumerConnDelegate) OnMessage(c *Conn, m *Message)         { d.r.onConnMessage(c, m) }
func (d *consumerConnDelegate) OnMessageFinished(c *Conn, m *Message) { d.r.onConnMessageFinished(c, m) }
func (d *consumerConnDelegate) OnMessageRequeued(c *Conn, m *Message) { d.r.onConnMessageRequeued(c, m) }
func (d *consumerConnDelegate) OnBackoff(c *Conn)                     { d.r.onConnBackoff(c) }
func (d *consumerConnDelegate) OnContinue(c *Conn)                    { d.r.onConnContinue(c) }
func (d *consumerConnDelegate) OnResume(c *Conn)                      { d.r.onConnResume(c) }
func (d *consumerConnDelegate) OnIOError(c *Conn, err error)          { d.r.onConnIOError(c, err) }
func (d *consumerConnDelegate) OnHeartbeat(c *Conn)                   { d.r.onConnHeartbeat(c) }
func (d *consumerConnDelegate) OnClose(c *Conn)                       { d.r.onConnClose(c) }

type producerConnDelegate struct {
	w *Producer
}

func (d *producerConnDelegate) OnResponse(c *Conn, data []byte)       { d.w.onConnResponse(c, data) }
func (d *producerConnDelegate) OnError(c *Conn, data []byte)          { d.w.onConnError(c, data) }
func (d *producerConnDelegate) OnMessage(c *Conn, m *Message)         {}
func (d *producerConnDelegate) OnMessageFinished(c *Conn, m *Message) {}
func (d *producerConnDelegate) OnMessageRequeued(c *Conn, m *Message) {}
func (d *producerConnDelegate) OnBackoff(c *Conn)                     {}
func (d *producerConnDelegate) OnContinue(c *Conn)                    {}
func (d *producerConnDelegate) OnResume(c *Conn)                      {}
func (d *producerConnDelegate) OnIOError(c *Conn, err error)          { d.w.onConnIOError(c, err) }
func (d *producerConnDelegate) OnHeartbeat(c *Conn)                   { d.w.onConnHeartbeat(c) }
func (d *producerConnDelegate) OnClose(c *Conn)                       { d.w.onConnClose(c) }
