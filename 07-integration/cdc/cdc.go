// Package cdc implements DDIA §12: keeping derived data in sync with a system
// of record.
//
// The whole difficulty is that the source keeps moving while you copy it. The
// tools here are the ones the chapter describes:
//
//	snapshot + tail   take a point-in-time copy, then follow the change stream
//	                  from the exact LSN the snapshot was taken at ("consistent cut")
//	idempotent apply  upsert by primary key, so replaying a batch is harmless
//	schema evolution  the consumer must tolerate a source schema it has never seen
//
// Note the derived view is *not* written through the source. It is fed by the
// change stream, which is what makes the source's own transactional boundaries
// the unit of change.
package cdc

import (
	"fmt"
	"sort"
	"sync"
)

// Column is one field of a table schema.
type Column struct {
	Name    string
	Type    string
	Default any // used when a column is added to an existing table
}

// Schema is one version of a table's shape. Consumers must be able to read a
// row written under an older version (backward compatibility) and a source that
// adds a column must not break them (forward compatibility).
type Schema struct {
	Version int
	Columns []Column
}

// Row is one record. Fields are dynamic because schema evolution means the
// shape changes over time.
type Row map[string]any

func (r Row) Clone() Row {
	out := make(Row, len(r))
	for k, v := range r {
		out[k] = v
	}
	return out
}

// Op is the kind of change.
type Op string

const (
	OpInsert Op = "insert"
	OpUpdate Op = "update"
	OpDelete Op = "delete"
)

// Change is one logical replication event with before/after images — enough to
// build any derived view without re-reading the source table.
type Change struct {
	LSN           int64 // log sequence number: the replication cursor
	Op            Op
	Table         string
	PK            string
	Before        Row // nil for insert
	After         Row // nil for delete
	SchemaVersion int
}

func (c Change) String() string {
	switch c.Op {
	case OpInsert:
		return fmt.Sprintf("#%d insert %s/%s %v", c.LSN, c.Table, c.PK, c.After)
	case OpUpdate:
		return fmt.Sprintf("#%d update %s/%s %v -> %v", c.LSN, c.Table, c.PK, c.Before, c.After)
	default:
		return fmt.Sprintf("#%d delete %s/%s %v", c.LSN, c.Table, c.PK, c.Before)
	}
}

// ------------------------------------------------------------------ source --

// Source is a row store that also keeps a logical replication log. In a real
// system this log is the write-ahead log itself (Postgres logical decoding,
// MySQL binlog); here it is explicit so the mechanics are visible.
type Source struct {
	mu      sync.Mutex
	lsn     int64
	tables  map[string]map[string]Row
	schemas map[string][]Schema
	log     []Change
	primary map[string]string // table -> primary key column
}

// NewSource creates an empty source database.
func NewSource() *Source {
	return &Source{
		tables:  map[string]map[string]Row{},
		schemas: map[string][]Schema{},
		primary: map[string]string{},
	}
}

// CreateTable defines a table and its primary key.
func (s *Source) CreateTable(name, pk string, cols []Column) Schema {
	s.mu.Lock()
	defer s.mu.Unlock()
	sch := Schema{Version: 1, Columns: append([]Column(nil), cols...)}
	s.schemas[name] = []Schema{sch}
	s.tables[name] = map[string]Row{}
	s.primary[name] = pk
	return sch
}

// AddColumn evolves the schema. Adding a nullable column with a default value
// is backward compatible: old rows get the default, and consumers that do not
// know the column simply ignore it.
func (s *Source) AddColumn(table, name, typ string, def any) (Schema, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	versions := s.schemas[table]
	if len(versions) == 0 {
		return Schema{}, fmt.Errorf("cdc: unknown table %q", table)
	}
	prev := versions[len(versions)-1]
	next := Schema{Version: prev.Version + 1, Columns: append(append([]Column(nil), prev.Columns...), Column{Name: name, Type: typ, Default: def})}
	s.schemas[table] = append(versions, next)

	// Backfill every existing row so materialised copies see a consistent shape.
	for _, row := range s.tables[table] {
		if _, ok := row[name]; !ok {
			row[name] = def
		}
	}
	return next, nil
}

// DropColumn is the incompatible change: consumers that still expect the column
// will break unless the registry gives them a compatibility shim.
func (s *Source) DropColumn(table, name string) (Schema, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	versions := s.schemas[table]
	if len(versions) == 0 {
		return Schema{}, fmt.Errorf("cdc: unknown table %q", table)
	}
	prev := versions[len(versions)-1]
	next := Schema{Version: prev.Version + 1}
	for _, c := range prev.Columns {
		if c.Name != name {
			next.Columns = append(next.Columns, c)
		}
	}
	s.schemas[table] = append(versions, next)
	for _, row := range s.tables[table] {
		delete(row, name)
	}
	return next, nil
}

// CurrentSchema returns the newest schema of a table.
func (s *Source) CurrentSchema(table string) (Schema, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.schemas[table]
	if len(v) == 0 {
		return Schema{}, false
	}
	return v[len(v)-1], true
}

// SchemaAt returns the schema in force at a given version (what a consumer
// needs to decode an old change).
func (s *Source) SchemaAt(table string, version int) (Schema, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sch := range s.schemas[table] {
		if sch.Version == version {
			return sch, true
		}
	}
	return Schema{}, false
}

func (s *Source) schemaVersionLocked(table string) int {
	v := s.schemas[table]
	if len(v) == 0 {
		return 0
	}
	return v[len(v)-1].Version
}

// Insert writes a row and emits a change.
func (s *Source) Insert(table string, row Row) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pk := s.primary[table]
	if pk == "" {
		return 0, fmt.Errorf("cdc: unknown table %q", table)
	}
	key := fmt.Sprint(row[pk])
	if _, exists := s.tables[table][key]; exists {
		return 0, fmt.Errorf("cdc: duplicate primary key %q", key)
	}
	after := row.Clone()
	s.tables[table][key] = after
	return s.appendLocked(Change{Op: OpInsert, Table: table, PK: key, After: after.Clone()}), nil
}

// Update patches a row and emits a change with both images.
func (s *Source) Update(table, pk string, patch Row) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.tables[table][pk]
	if !ok {
		return 0, fmt.Errorf("cdc: row %s/%s not found", table, pk)
	}
	before := cur.Clone()
	after := cur.Clone()
	for k, v := range patch {
		after[k] = v
	}
	s.tables[table][pk] = after
	return s.appendLocked(Change{Op: OpUpdate, Table: table, PK: pk, Before: before, After: after.Clone()}), nil
}

// Delete removes a row and emits a delete change (with the old image, so
// consumers can clean up indexes they derived from it).
func (s *Source) Delete(table, pk string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.tables[table][pk]
	if !ok {
		return 0, fmt.Errorf("cdc: row %s/%s not found", table, pk)
	}
	before := cur.Clone()
	delete(s.tables[table], pk)
	return s.appendLocked(Change{Op: OpDelete, Table: table, PK: pk, Before: before}), nil
}

func (s *Source) appendLocked(c Change) int64 {
	s.lsn++
	c.LSN = s.lsn
	c.SchemaVersion = s.schemaVersionLocked(c.Table)
	s.log = append(s.log, c)
	return c.LSN
}

// ChangesSince returns every change after lsn, oldest first. This is the
// replication stream a consumer tails.
func (s *Source) ChangesSince(lsn int64) []Change {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Change
	for _, c := range s.log {
		if c.LSN > lsn {
			out = append(out, c)
		}
	}
	return out
}

// ChangesFor returns changes for one table only.
func (s *Source) ChangesFor(table string, lsn int64) []Change {
	var out []Change
	for _, c := range s.ChangesSince(lsn) {
		if c.Table == table {
			out = append(out, c)
		}
	}
	return out
}

// Snapshot copies a table and returns the LSN it was taken at. Everything after
// that LSN is the tail you must follow; anything before it is already in the copy.
// Getting this cut right is the difference between losing a row and duplicating one.
func (s *Source) Snapshot(table string) (map[string]Row, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]Row{}
	for pk, row := range s.tables[table] {
		out[pk] = row.Clone()
	}
	return out, s.lsn
}

// LSN returns the current log position.
func (s *Source) LSN() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lsn
}

// RowCount returns the number of live rows in a table.
func (s *Source) RowCount(table string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.tables[table])
}

// ------------------------------------------------------------------- view --

// View is a derived data system (a search index, a cache, another database).
// The only requirement it has to meet is idempotent, per-key application.
type View struct {
	mu         sync.Mutex
	rows       map[string]Row
	applied    int64
	opsApplied int
	appliedLSN map[int64]bool
}

// NewView creates an empty derived view.
func NewView() *View {
	return &View{rows: map[string]Row{}, appliedLSN: map[int64]bool{}}
}

// Apply folds one change in. Applying the same change twice is a no-op because
// the write is an upsert by primary key, not an append.
func (v *View) Apply(c Change) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.appliedLSN[c.LSN] {
		// Duplicate delivery (at-least-once): ignore it. Without this, a "count
		// orders" style view would double count.
		return nil
	}
	switch c.Op {
	case OpInsert, OpUpdate:
		if c.After == nil {
			return fmt.Errorf("cdc: %s change without an after image (LSN %d)", c.Op, c.LSN)
		}
		v.rows[c.PK] = c.After.Clone()
	case OpDelete:
		delete(v.rows, c.PK)
	}
	v.appliedLSN[c.LSN] = true
	if c.LSN > v.applied {
		v.applied = c.LSN
	}
	v.opsApplied++
	return nil
}

// Get reads one derived row.
func (v *View) Get(pk string) (Row, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	r, ok := v.rows[pk]
	if !ok {
		return nil, false
	}
	return r.Clone(), true
}

// Rows returns the whole derived table.
func (v *View) Rows() map[string]Row {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make(map[string]Row, len(v.rows))
	for k, r := range v.rows {
		out[k] = r.Clone()
	}
	return out
}

// AppliedLSN is the consumer's committed position (the offset it would store).
func (v *View) AppliedLSN() int64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.applied
}

// OpsApplied counts effective applications (duplicates excluded).
func (v *View) OpsApplied() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.opsApplied
}

// ----------------------------------------------------------------- pipeline --

// Bootstrap performs the initial consistent cut: snapshot the table and tail
// the change log from exactly the LSN the snapshot was taken at.
func Bootstrap(src *Source, view *View, table string) int64 {
	snap, lsn := src.Snapshot(table)
	for pk, row := range snap {
		_ = view.Apply(Change{LSN: -hashKey(pk), Op: OpInsert, Table: table, PK: pk, After: row})
	}
	return Tail(src, view, table, lsn)
}

// Tail applies every change after lsn and returns the new position.
func Tail(src *Source, view *View, table string, lsn int64) int64 {
	for _, c := range src.ChangesFor(table, lsn) {
		_ = view.Apply(c)
	}
	return src.LSN()
}

// Replay re-applies a range of the log (a backfill: rebuild a view after fixing
// a bug, or reindex into a new search engine).
func Replay(src *Source, view *View, table string, from int64) int64 {
	for _, c := range src.ChangesFor(table, from) {
		_ = view.Apply(c)
	}
	return src.LSN()
}

func hashKey(s string) int64 {
	h := int64(1469598103934665603)
	for i := 0; i < len(s); i++ {
		h ^= int64(s[i])
		h *= 1099511628211
	}
	if h < 0 {
		h = -h
	}
	return h + 1
}

// Diff reports keys where the view disagrees with the source: the check you run
// continuously because nothing guarantees convergence for free.
func Diff(src *Source, view *View, table string) []string {
	snap, _ := src.Snapshot(table)
	got := view.Rows()
	var out []string
	for pk, want := range snap {
		have, ok := got[pk]
		if !ok || !rowsEqual(want, have) {
			out = append(out, pk)
		}
	}
	for pk := range got {
		if _, ok := snap[pk]; !ok {
			out = append(out, pk)
		}
	}
	sort.Strings(out)
	return out
}

func rowsEqual(a, b Row) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok || fmt.Sprint(av) != fmt.Sprint(bv) {
			return false
		}
	}
	return true
}
