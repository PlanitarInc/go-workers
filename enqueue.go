package workers

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

const (
	NanoSecondPrecision = 1000000000.0
)

type EnqueueData struct {
	Queue      string      `json:"queue,omitempty"`
	Class      string      `json:"class"`
	Args       interface{} `json:"args"`
	Jid        string      `json:"jid"`
	EnqueuedAt float64     `json:"enqueued_at"`
	EnqueueOptions
}

type EnqueueOptions struct {
	MaxAttempts int     `json:"max_attempts,omitempty"`
	RetryCount  int     `json:"retry_count,omitempty"`
	Retry       bool    `json:"retry,omitempty"`
	At          float64 `json:"at,omitempty"`
}

func generateJid() string {
	// Return 12 random bytes as 24 character hex
	b := make([]byte, 12)
	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", b)
}

func Enqueue(queue, class string, args interface{}) (string, error) {
	return EnqueueWithOptions(queue, class, args, EnqueueOptions{At: NowToSecondsWithNanoPrecision()})
}

func EnqueueIn(queue, class string, in float64, args interface{}) (string, error) {
	return EnqueueWithOptions(queue, class, args, EnqueueOptions{At: NowToSecondsWithNanoPrecision() + in})
}

func EnqueueAt(queue, class string, at time.Time, args interface{}) (string, error) {
	return EnqueueWithOptions(queue, class, args, EnqueueOptions{At: timeToSecondsWithNanoPrecision(at)})
}

func EnqueueWithOptions(queue, class string, args interface{}, opts EnqueueOptions) (string, error) {
	now := NowToSecondsWithNanoPrecision()
	data := EnqueueData{
		Queue:          queue,
		Class:          class,
		Args:           args,
		Jid:            generateJid(),
		EnqueuedAt:     now,
		EnqueueOptions: opts,
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		return "", err
	}

	if now < opts.At {
		err := enqueueAt(data.At, bytes)
		return data.Jid, err
	}

	conn := Config.Pool.Get()
	defer conn.Close()

	_, err = conn.Do("sadd", Config.Namespace+"queues", queue)
	if err != nil {
		return "", err
	}
	queue = Config.Namespace + "queue:" + queue
	_, err = conn.Do("rpush", queue, bytes)
	if err != nil {
		return "", err
	}

	return data.Jid, nil
}

func PrepareEnqueuMsg(queue, class string, args interface{}, opts EnqueueOptions) *Msg {
	now := NowToSecondsWithNanoPrecision()

	if opts.At <= 0 {
		opts.At = now
	}

	data := EnqueueData{
		Queue:          queue,
		Class:          class,
		Args:           args,
		Jid:            generateJid(),
		EnqueuedAt:     now,
		EnqueueOptions: opts,
	}

	bytes, _ := json.Marshal(data)
	msg, _ := NewMsg(string(bytes))
	return msg
}

func EnqueueMsg(msg *Msg) error {
	if _, err := msg.Get("queue").String(); err != nil {
		return fmt.Errorf("Bad queue value: %s", err)
	}

	now := NowToSecondsWithNanoPrecision()

	if _, err := msg.Get("jid").String(); err != nil {
		if _, ok := msg.CheckGet("jid"); ok {
			return fmt.Errorf("Bad JID value: %s", err)
		}

		msg.Set("jid", generateJid())
	}

	if _, err := msg.Get("at").Float64(); err != nil {
		if _, ok := msg.CheckGet("at"); ok {
			return fmt.Errorf("Bad at value: %s", err)
		}

		msg.Set("at", now)
	}

	msg.Set("enqueued_at", now)
	bytes, _ := msg.Encode()
	queue, _ := msg.Get("queue").String()

	if at, _ := msg.Get("at").Float64(); now < at {
		err := enqueueAt(at, bytes)
		return err
	}

	conn := Config.Pool.Get()
	defer conn.Close()

	if _, err := conn.Do("sadd", Config.Namespace+"queues", queue); err != nil {
		return err
	}

	queue = Config.Namespace + "queue:" + queue
	if _, err := conn.Do("rpush", queue, bytes); err != nil {
		return err
	}

	return nil
}

func enqueueAt(at float64, bytes []byte) error {
	conn := Config.Pool.Get()
	defer conn.Close()

	_, err := conn.Do(
		"zadd",
		Config.Namespace+SCHEDULED_JOBS_KEY, at, bytes,
	)
	if err != nil {
		return err
	}

	return nil
}

func timeToSecondsWithNanoPrecision(t time.Time) float64 {
	return float64(t.UnixNano()) / NanoSecondPrecision
}

func durationToSecondsWithNanoPrecision(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / NanoSecondPrecision
}

func NowToSecondsWithNanoPrecision() float64 {
	return timeToSecondsWithNanoPrecision(time.Now())
}
