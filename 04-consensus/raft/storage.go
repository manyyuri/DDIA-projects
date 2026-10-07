package raft

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// HardState is the part of Raft's state that MUST survive a crash before any
// RPC is answered: currentTerm and votedFor. Forgetting to persist these is the
// classic way to end up with two leaders in one term.
type HardState struct {
	Term      uint64 `json:"term"`
	VotedFor  string `json:"voted_for"`
	SnapIndex uint64 `json:"snap_index"`
	SnapTerm  uint64 `json:"snap_term"`
	Snapshot  []byte `json:"snapshot,omitempty"`
}

// PersistedState is everything a node remembers across a restart.
type PersistedState struct {
	Hard    HardState `json:"hard"`
	Entries []Entry   `json:"entries"`
}

// Storage persists a node's log and hard state.
//
// This implementation rewrites the whole state on every change. That is
// obviously not how a real system works (etcd appends incrementally and
// fsyncs a batch), but it keeps the *semantics* — "must be durable before we
// reply" — visible, which is the part that matters for safety.
type Storage interface {
	Save(PersistedState) error
	Load() (PersistedState, error)
}

// MemStorage keeps state in memory: models a replica whose disk is fine but
// which never really restarts.
type MemStorage struct {
	mu sync.Mutex
	st PersistedState
}

// NewMemStorage returns an empty in-memory store.
func NewMemStorage() *MemStorage { return &MemStorage{} }

// Save implements Storage.
func (m *MemStorage) Save(st PersistedState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.st = st
	return nil
}

// Load implements Storage.
func (m *MemStorage) Load() (PersistedState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.st, nil
}

// FileStorage persists to a single JSON file using write-temp + fsync + rename,
// so a crash leaves either the old or the new state, never a torn mixture.
type FileStorage struct {
	path string
	mu   sync.Mutex
}

// NewFileStorage creates a file-backed store at path.
func NewFileStorage(path string) *FileStorage { return &FileStorage{path: path} }

// Save implements Storage.
func (f *FileStorage) Save(st PersistedState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(f.path), 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(st)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(f.path), "raft-state-*.tmp")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), f.path)
}

// Load implements Storage.
func (f *FileStorage) Load() (PersistedState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := os.ReadFile(f.path)
	if err != nil {
		if os.IsNotExist(err) {
			return PersistedState{}, nil
		}
		return PersistedState{}, err
	}
	var st PersistedState
	if err := json.Unmarshal(data, &st); err != nil {
		return PersistedState{}, err
	}
	return st, nil
}

func encodeConfig(peers []string) ([]byte, error) { return json.Marshal(peers) }

func decodeConfig(b []byte) ([]string, error) {
	var peers []string
	err := json.Unmarshal(b, &peers)
	return peers, err
}
