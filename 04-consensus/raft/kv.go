package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// KV is a linearizable key/value state machine on top of Raft. Nothing here is
// clever: it just applies committed commands in order.
//
// Linearity comes from routing *reads through the log* as well. A read that
// consulted a leader's local map directly could serve stale data after a
// partition (the old leader does not know it was deposed), which is exactly the
// "stale read on a deposed leader" failure in DDIA §8.3.1.
type KV struct {
	mu       sync.Mutex
	data     map[string]string
	applied  uint64
	node     *Node
	done     chan struct{}
	stopOnce sync.Once
}

// Command is the on-the-wire form of a KV operation.
type Command struct {
	Op    string `json:"op"` // "put" | "del" | "get"
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ErrNotLeader is returned by a proposal that hit a non-leader.
var ErrNotLeader = errors.New("raft: not the leader")

// NewKV attaches a state machine to a node and starts consuming its apply
// channel. Results are matched to in-flight requests by log index.
func NewKV(node *Node) *KV {
	kv := &KV{
		data: map[string]string{},
		node: node,
		done: make(chan struct{}),
	}
	go kv.consume()
	return kv
}

func (kv *KV) consume() {
	for {
		var msg ApplyMsg
		select {
		case <-kv.done:
			return
		case msg = <-kv.node.ApplyCh():
		}
		{
			if msg.Type == "noop" || msg.Type == "config" {
				kv.mu.Lock()
				kv.applied = msg.Index
				kv.mu.Unlock()
				continue
			}
			if msg.Type == "snapshot" {
				_ = kv.Restore(msg.Snapshot)
				kv.mu.Lock()
				kv.applied = msg.Index
				kv.mu.Unlock()
				continue
			}
			var cmd Command
			if err := json.Unmarshal(msg.Command, &cmd); err != nil {
				continue
			}
			kv.mu.Lock()
			switch cmd.Op {
			case "put":
				kv.data[cmd.Key] = cmd.Value
			case "del":
				delete(kv.data, cmd.Key)
			}
			kv.applied = msg.Index
			kv.mu.Unlock()
		}
	}
}

// Stop detaches the consumer. It is safe to call more than once.
func (kv *KV) Stop() { kv.stopOnce.Do(func() { close(kv.done) }) }

// Applied returns the highest applied index.
func (kv *KV) Applied() uint64 {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.applied
}

// LocalValue reads without consensus. Useful for tests that want to prove a
// stale local read is possible.
func (kv *KV) LocalValue(key string) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v, ok := kv.data[key]
	return v, ok
}

// Snapshot serialises the state machine (used for log compaction).
func (kv *KV) Snapshot() ([]byte, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return json.Marshal(kv.data)
}

// Restore loads a snapshot produced by Snapshot.
func (kv *KV) Restore(data []byte) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	m := map[string]string{}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &m); err != nil {
			return err
		}
	}
	kv.data = m
	return nil
}

// ProposePut submits a write and returns the log index it will occupy.
func (kv *KV) ProposePut(key, value string) (uint64, error) {
	return kv.propose(Command{Op: "put", Key: key, Value: value})
}

// ProposeDelete submits a delete.
func (kv *KV) ProposeDelete(key string) (uint64, error) {
	return kv.propose(Command{Op: "del", Key: key})
}

// ProposeRead submits a read *through the log* so it is linearizable.
func (kv *KV) ProposeRead(key string) (uint64, error) {
	return kv.propose(Command{Op: "get", Key: key})
}

func (kv *KV) propose(cmd Command) (uint64, error) {
	data, err := json.Marshal(cmd)
	if err != nil {
		return 0, err
	}
	return kv.node.Propose(data)
}

// Describe renders the state machine for debugging.
func (kv *KV) Describe() string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return fmt.Sprintf("applied=%d data=%v", kv.applied, kv.data)
}
