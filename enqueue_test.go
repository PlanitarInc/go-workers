package workers

import (
	"encoding/json"
	"fmt"

	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
)

func EnqueueSpec(c gospec.Context) {
	was := Config.Namespace
	Config.Namespace = "prod:"

	c.Specify("Enqueue", func() {
		conn := Config.Pool.Get()
		defer conn.Close()

		c.Specify("makes the queue available", func() {
			Enqueue("enqueue1", "Add", []int{1, 2})

			found, _ := redis.Bool(conn.Do("sismember", "prod:queues", "enqueue1"))
			c.Expect(found, IsTrue)
		})

		c.Specify("adds a job to the queue", func() {
			nb, _ := redis.Int(conn.Do("llen", "prod:queue:enqueue2"))
			c.Expect(nb, Equals, 0)

			Enqueue("enqueue2", "Add", []int{1, 2})

			nb, _ = redis.Int(conn.Do("llen", "prod:queue:enqueue2"))
			c.Expect(nb, Equals, 1)
		})

		c.Specify("saves the arguments", func() {
			Enqueue("enqueue3", "Compare", []string{"foo", "bar"})

			bytes, _ := redis.Bytes(conn.Do("lpop", "prod:queue:enqueue3"))
			var result map[string]interface{}
			json.Unmarshal(bytes, &result)
			c.Expect(result["class"], Equals, "Compare")

			args := result["args"].([]interface{})
			c.Expect(len(args), Equals, 2)
			c.Expect(args[0], Equals, "foo")
			c.Expect(args[1], Equals, "bar")
		})

		c.Specify("has a jid", func() {
			Enqueue("enqueue4", "Compare", []string{"foo", "bar"})

			bytes, _ := redis.Bytes(conn.Do("lpop", "prod:queue:enqueue4"))
			var result map[string]interface{}
			json.Unmarshal(bytes, &result)
			c.Expect(result["class"], Equals, "Compare")

			jid := result["jid"].(string)
			c.Expect(len(jid), Equals, 24)
		})

		c.Specify("has enqueued_at that is close to now", func() {
			Enqueue("enqueue5", "Compare", []string{"foo", "bar"})

			bytes, _ := redis.Bytes(conn.Do("lpop", "prod:queue:enqueue5"))
			var result map[string]interface{}
			json.Unmarshal(bytes, &result)
			c.Expect(result["class"], Equals, "Compare")

			ea := result["enqueued_at"].(float64)
			c.Expect(ea, Not(Equals), 0)
			c.Expect(ea, IsWithin(0.1), NowToSecondsWithNanoPrecision())
		})

		c.Specify("has retry and retry_count when set", func() {
			EnqueueWithOptions("enqueue6", "Compare", []string{"foo", "bar"}, EnqueueOptions{RetryCount: 13, Retry: true, MaxAttempts: 98})

			bytes, _ := redis.Bytes(conn.Do("lpop", "prod:queue:enqueue6"))
			var result map[string]interface{}
			json.Unmarshal(bytes, &result)
			c.Expect(result["class"], Equals, "Compare")

			retry := result["retry"].(bool)
			c.Expect(retry, Equals, true)

			retryCount := int(result["retry_count"].(float64))
			c.Expect(retryCount, Equals, 13)

			maxRetries := int(result["max_attempts"].(float64))
			c.Expect(maxRetries, Equals, 98)
		})
	})

	c.Specify("EnqueueIn", func() {
		scheduleQueue := "prod:" + SCHEDULED_JOBS_KEY
		conn := Config.Pool.Get()
		defer conn.Close()

		c.Specify("has added a job in the scheduled queue", func() {
			_, err := EnqueueIn("enqueuein1", "Compare", 10, map[string]interface{}{"foo": "bar"})
			c.Expect(err, Equals, nil)

			scheduledCount, _ := redis.Int(conn.Do("zcard", scheduleQueue))
			c.Expect(scheduledCount, Equals, 1)

			conn.Do("del", scheduleQueue)
		})

		c.Specify("has the correct 'queue'", func() {
			_, err := EnqueueIn("enqueuein2", "Compare", 10, map[string]interface{}{"foo": "bar"})
			c.Expect(err, Equals, nil)

			var data EnqueueData
			elem, err := conn.Do("zrange", scheduleQueue, 0, -1)
			bytes, err := redis.Bytes(elem.([]interface{})[0], err)
			json.Unmarshal(bytes, &data)

			c.Expect(data.Queue, Equals, "enqueuein2")

			conn.Do("del", scheduleQueue)
		})
	})

	c.Specify("PrepareEnqueuMsg", func() {

		c.Specify("empty options", func() {
			val := map[string]interface{}{"foo": "bar", "baz": true}
			opts := EnqueueOptions{}
			msg := PrepareEnqueuMsg("ququ-queue", "QuquClass", val, opts)

			c.Specify("has set queue, class and args fields", func() {
				queue, err := msg.Get("queue").String()
				c.Expect(err, Equals, nil)
				c.Expect(queue, Equals, "ququ-queue")

				class, err := msg.Get("class").String()
				c.Expect(err, Equals, nil)
				c.Expect(class, Equals, "QuquClass")

				args, err := msg.Get("args").Map()
				c.Expect(err, Equals, nil)
				c.Expect(len(args), Equals, 2)
				c.Expect(args["foo"], Equals, "bar")
				c.Expect(args["baz"], Equals, true)
			})

			c.Specify("has set JID", func() {
				val, err := msg.Get("jid").String()
				c.Expect(err, Equals, nil)
				c.Expect(len(val), Equals, 24)
			})

			c.Specify("filled the timestamps", func() {
				at, err := msg.Get("at").Float64()
				c.Expect(err, Equals, nil)
				c.Expect(at, IsWithin(0.1), NowToSecondsWithNanoPrecision())

				enqueueAt, err := msg.Get("enqueued_at").Float64()
				c.Expect(err, Equals, nil)
				c.Expect(enqueueAt, IsWithin(0.1), NowToSecondsWithNanoPrecision())
			})

			c.Specify("has default options", func() {
				_, ok1 := msg.CheckGet("retry")
				c.Expect(ok1, Equals, false)

				_, ok2 := msg.CheckGet("retry_count")
				c.Expect(ok2, Equals, false)
			})
		})

		c.Specify("preserve passed options", func() {
			val := map[string]interface{}{"foo": "bar", "baz": true}
			opts := EnqueueOptions{Retry: true, RetryCount: 17, At: NowToSecondsWithNanoPrecision() + 5}
			msg := PrepareEnqueuMsg("ququ-queue", "QuquClass", val, opts)

			c.Specify("has retry fields", func() {
				retry, err := msg.Get("retry").Bool()
				c.Expect(err, Equals, nil)
				c.Expect(retry, Equals, true)

				retry_count, err := msg.Get("retry_count").Int()
				c.Expect(err, Equals, nil)
				c.Expect(retry_count, Equals, 17)
			})

			c.Specify("preserved the at timestamp", func() {
				at, err := msg.Get("at").Float64()
				c.Expect(err, Equals, nil)
				c.Expect(at, IsWithin(0.1), NowToSecondsWithNanoPrecision()+5)
			})

			c.Specify("filled enqueued_at timestamp", func() {
				enqueueAt, err := msg.Get("enqueued_at").Float64()
				c.Expect(err, Equals, nil)
				c.Expect(enqueueAt, IsWithin(0.1), NowToSecondsWithNanoPrecision())
			})
		})
	})

	c.Specify("EnqueueMsg", func() {
		conn := Config.Pool.Get()
		defer conn.Close()

		c.Specify("makes the queue available", func() {
			msg, err := NewMsg(`{"jid":"1", "queue": "enqueue1"}`)
			c.Expect(err, Equals, nil)

			err = EnqueueMsg(msg)
			c.Expect(err, Equals, nil)

			found, _ := redis.Bool(conn.Do("sismember", "prod:queues", "enqueue1"))
			c.Expect(found, IsTrue)
		})

		c.Specify("adds a job to the queue", func() {
			msg, err := NewMsg(`{"jid":"2", "queue": "enqueue2"}`)
			c.Expect(err, Equals, nil)

			nb, _ := redis.Int(conn.Do("llen", "prod:queue:enqueue2"))
			c.Expect(nb, Equals, 0)

			err = EnqueueMsg(msg)
			c.Expect(err, Equals, nil)

			nb, _ = redis.Int(conn.Do("llen", "prod:queue:enqueue2"))
			c.Expect(nb, Equals, 1)
		})

		c.Specify("saves the full message as is", func() {
			msg, err := NewMsg(fmt.Sprintf(`{"jid":"3", "queue": "enqueue3", "at": %f}`,
				NowToSecondsWithNanoPrecision()))
			c.Expect(err, Equals, nil)

			err = EnqueueMsg(msg)
			c.Expect(err, Equals, nil)

			bytes, _ := redis.Bytes(conn.Do("lpop", "prod:queue:enqueue3"))
			savedMsg, err := NewMsg(string(bytes))
			c.Expect(err, Equals, nil)

			c.Expect(msg.Get("jid").MustString(), Equals, savedMsg.Get("jid").MustString())
			c.Expect(msg.Get("queue").MustString(), Equals, savedMsg.Get("queue").MustString())
			c.Expect(msg.Get("at").MustFloat64(), Equals, savedMsg.Get("at").MustFloat64())
		})

		c.Specify("saves the message as is including unknown fields", func() {
			msg, err := NewMsg(fmt.Sprintf(`{"jid":"3", "vava": true, "queue": "enqueue3", "at": %f, "x-field": {"one": "1", "two": "_2_"}}`,
				NowToSecondsWithNanoPrecision()))
			c.Expect(err, Equals, nil)

			err = EnqueueMsg(msg)
			c.Expect(err, Equals, nil)

			bytes, _ := redis.Bytes(conn.Do("lpop", "prod:queue:enqueue3"))
			savedMsg, err := NewMsg(string(bytes))
			c.Expect(err, Equals, nil)

			c.Expect(savedMsg.Get("vava").MustBool(), Equals, true)
			xfield := savedMsg.Get("x-field").MustMap()
			c.Expect(len(xfield), Equals, 2)
			c.Expect(xfield["one"], Equals, "1")
			c.Expect(xfield["two"], Equals, "_2_")
		})

		c.Specify("sets `enqueud_at` field", func() {
			msg, err := NewMsg(fmt.Sprintf(`{"jid":"4", "queue": "enqueue4", "at": %f}`,
				NowToSecondsWithNanoPrecision()))
			c.Expect(err, Equals, nil)

			err = EnqueueMsg(msg)
			c.Expect(err, Equals, nil)

			bytes, _ := redis.Bytes(conn.Do("lpop", "prod:queue:enqueue4"))
			savedMsg, err := NewMsg(string(bytes))
			c.Expect(err, Equals, nil)

			enqueueAt, err := msg.Get("enqueued_at").Float64()
			c.Expect(err, Equals, nil)
			c.Expect(enqueueAt, IsWithin(0.1), NowToSecondsWithNanoPrecision())

			enqueueAt2, err := savedMsg.Get("enqueued_at").Float64()
			c.Expect(err, Equals, nil)
			c.Expect(enqueueAt2, Equals, enqueueAt)
		})

		c.Specify("sets `jid` field if missing", func() {
			msg, err := NewMsg(fmt.Sprintf(`{"queue": "enqueue5", "at": %f}`,
				NowToSecondsWithNanoPrecision()))
			c.Expect(err, Equals, nil)

			err = EnqueueMsg(msg)
			c.Expect(err, Equals, nil)

			bytes, _ := redis.Bytes(conn.Do("lpop", "prod:queue:enqueue5"))
			savedMsg, err := NewMsg(string(bytes))
			c.Expect(err, Equals, nil)

			jid, err := msg.Get("jid").String()
			c.Expect(err, Equals, nil)
			c.Expect(len(jid), Equals, 24)

			jid2, err := savedMsg.Get("jid").String()
			c.Expect(err, Equals, nil)
			c.Expect(jid2, Equals, jid)
		})

		c.Specify("sets `at` field if missing", func() {
			msg, err := NewMsg(fmt.Sprintf(`{"jid":"6", "queue": "enqueue6"}`))
			c.Expect(err, Equals, nil)

			err = EnqueueMsg(msg)
			c.Expect(err, Equals, nil)

			bytes, _ := redis.Bytes(conn.Do("lpop", "prod:queue:enqueue6"))
			savedMsg, err := NewMsg(string(bytes))
			c.Expect(err, Equals, nil)

			at, err := savedMsg.Get("at").Float64()
			c.Expect(err, Equals, nil)
			c.Expect(at, IsWithin(0.1), NowToSecondsWithNanoPrecision())

			at2, err := savedMsg.Get("at").Float64()
			c.Expect(err, Equals, nil)
			c.Expect(at2, Equals, at)
		})

		c.Specify("rejects a message with undefined queue", func() {
			msg, err := NewMsg(`{"jid":"7"}`)
			c.Expect(err, Equals, nil)

			err = EnqueueMsg(msg)
			c.Expect(err, Not(Equals), nil)

			nq, err := redis.Int(conn.Do("llen", "prod:queues"))
			c.Expect(err, Equals, nil)
			c.Expect(nq, Equals, 0)
		})
	})

	Config.Namespace = was
}
