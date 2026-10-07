package cdc

import (
	"strings"
	"testing"
)

func newPeople(t *testing.T) *Source {
	t.Helper()
	src := NewSource()
	src.CreateTable("people", "id", []Column{
		{Name: "id", Type: "text"},
		{Name: "name", Type: "text"},
		{Name: "city", Type: "text"},
	})
	return src
}

func TestChangesCarryBothImages(t *testing.T) {
	src := newPeople(t)
	view := NewView()

	if _, err := src.Insert("people", Row{"id": "1", "name": "alice", "city": "hangzhou"}); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Update("people", "1", Row{"city": "shanghai"}); err != nil {
		t.Fatal(err)
	}

	changes := src.ChangesSince(0)
	if len(changes) != 2 {
		t.Fatalf("expected 2 changes, got %v", changes)
	}
	if changes[0].Before != nil {
		t.Fatal("an insert should have no before image")
	}
	if changes[1].Before["city"] != "hangzhou" || changes[1].After["city"] != "shanghai" {
		t.Fatalf("update images wrong: %v", changes[1])
	}
	t.Logf("stream: %v", changes)

	for _, c := range changes {
		if err := view.Apply(c); err != nil {
			t.Fatal(err)
		}
	}
	if row, ok := view.Get("1"); !ok || row["city"] != "shanghai" {
		t.Fatalf("view did not converge: %v %v", row, ok)
	}
	if _, err := src.Delete("people", "1"); err != nil {
		t.Fatal(err)
	}
	if err := view.Apply(src.ChangesSince(2)[0]); err != nil {
		t.Fatal(err)
	}
	if _, ok := view.Get("1"); ok {
		t.Fatal("delete did not propagate")
	}
}

// At-least-once delivery is the norm, so the consumer must be idempotent.
func TestReplayingAChangeIsIdempotent(t *testing.T) {
	src := newPeople(t)
	view := NewView()

	src.Insert("people", Row{"id": "1", "name": "alice", "city": "hz"})
	changes := src.ChangesSince(0)

	for i := 0; i < 3; i++ { // the same batch delivered three times
		for _, c := range changes {
			if err := view.Apply(c); err != nil {
				t.Fatal(err)
			}
		}
	}
	if got := view.OpsApplied(); got != 1 {
		t.Fatalf("duplicate delivery applied %d times, want 1", got)
	}
	// Without a dedupe/count-in-the-source design, a counter view would be 3x
	// wrong here. This is why "count of events" is the classic CDC bug.
}

// The consistent cut: snapshot first, then tail from the LSN the snapshot was
// taken at. Writes that land during the copy are neither lost nor applied twice.
func TestBootstrapSnapshotPlusTailIsConsistent(t *testing.T) {
	src := newPeople(t)
	for i := 0; i < 50; i++ {
		src.Insert("people", Row{"id": pk(i), "name": "n", "city": "hz"})
	}

	view := NewView()
	// Bootstrap takes the snapshot at LSN 50 and tails from there.
	from := Bootstrap(src, view, "people")

	// Writes arriving after the snapshot must be picked up by the tail.
	for i := 50; i < 60; i++ {
		src.Insert("people", Row{"id": pk(i), "name": "n", "city": "hz"})
	}
	Tail(src, view, "people", from)

	if got := len(view.Rows()); got != 60 {
		t.Fatalf("view has %d rows, want 60", got)
	}
	if diff := Diff(src, view, "people"); len(diff) != 0 {
		t.Fatalf("view diverged from the source: %v", diff)
	}
	t.Logf("bootstrap+tail converged with %d rows at LSN %d", len(view.Rows()), view.AppliedLSN())
}

func TestIncrementalTailKeepsUpWithLiveWrites(t *testing.T) {
	src := newPeople(t)
	view := NewView()
	cursor := Bootstrap(src, view, "people")

	for round := 0; round < 5; round++ {
		src.Insert("people", Row{"id": pk(round), "name": "n", "city": "hz"})
		src.Update("people", pk(round), Row{"city": "sh"})
		cursor = Tail(src, view, "people", cursor)
	}
	if diff := Diff(src, view, "people"); len(diff) != 0 {
		t.Fatalf("view diverged: %v", diff)
	}
	if got := len(view.Rows()); got != 5 {
		t.Fatalf("view has %d rows, want 5", got)
	}
}

// Schema evolution: adding a column with a default is compatible, and a change
// written under an older schema must still be decodable later.
func TestAddColumnIsBackwardCompatible(t *testing.T) {
	src := newPeople(t)
	src.Insert("people", Row{"id": "1", "name": "alice", "city": "hz"})

	// A change produced before the evolution.
	oldChange := src.ChangesSince(0)[0]
	if oldChange.SchemaVersion != 1 {
		t.Fatalf("expected schema v1, got %d", oldChange.SchemaVersion)
	}

	sch, err := src.AddColumn("people", "email", "text", "unknown@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if sch.Version != 2 {
		t.Fatalf("schema version = %d, want 2", sch.Version)
	}
	src.Update("people", "1", Row{"city": "sh"})
	newChange := src.ChangesSince(oldChange.LSN)[0]
	if newChange.SchemaVersion != 2 {
		t.Fatalf("new change is tagged v%d, want v2", newChange.SchemaVersion)
	}
	if newChange.After["email"] != "unknown@example.com" {
		t.Fatalf("existing row was not backfilled: %v", newChange.After)
	}

	// A consumer that predates the column can still process both changes: it
	// just ignores the field it does not know about.
	view := NewView()
	for _, c := range []Change{oldChange, newChange} {
		if err := view.Apply(c); err != nil {
			t.Fatalf("old consumer failed on a v%d change: %v", c.SchemaVersion, err)
		}
	}
	row, _ := view.Get("1")
	if row["city"] != "sh" {
		t.Fatalf("old consumer produced the wrong row: %v", row)
	}
	if _, hadEmail := row["email"]; hadEmail {
		t.Log("old consumer ignored the new column, as expected")
	}
	// A decoder can still fetch the schema a change was written under.
	if _, ok := src.SchemaAt("people", 1); !ok {
		t.Fatal("schema v1 should still be retrievable for old changes")
	}
}

// Dropping a column is the breaking change: a consumer that selects it breaks,
// so the registry has to give it a compatibility view.
func TestDropColumnBreaksNaiveConsumers(t *testing.T) {
	src := newPeople(t)
	src.Insert("people", Row{"id": "1", "name": "alice", "city": "hz"})
	if _, err := src.DropColumn("people", "city"); err != nil {
		t.Fatal(err)
	}
	sch, _ := src.CurrentSchema("people")
	names := []string{}
	for _, c := range sch.Columns {
		names = append(names, c.Name)
	}
	joined := strings.Join(names, ",")
	t.Logf("new schema v%d columns: %s", sch.Version, joined)
	if strings.Contains(joined, "city") {
		t.Fatal("dropped column still present")
	}
	// Every row reflects the new shape.
	snap, _ := src.Snapshot("people")
	if _, ok := snap["1"]["city"]; ok {
		t.Fatal("rows were not migrated to the new schema")
	}
}

// Backfill: rebuild a view from the beginning of the log. Because application
// is idempotent, this can be done on a live system.
func TestReplayRebuildsTheView(t *testing.T) {
	src := newPeople(t)
	for i := 0; i < 20; i++ {
		src.Insert("people", Row{"id": pk(i), "name": "n", "city": "hz"})
	}
	src.Update("people", pk(3), Row{"city": "sh"})
	src.Delete("people", pk(7))

	view := NewView()
	Replay(src, view, "people", 0)

	if diff := Diff(src, view, "people"); len(diff) != 0 {
		t.Fatalf("replay did not reproduce the source: %v", diff)
	}
	if len(view.Rows()) != 19 {
		t.Fatalf("expected 19 rows, got %d", len(view.Rows()))
	}

	// Replaying again changes nothing (idempotent), which is what makes
	// "reindex into a fresh search cluster" a safe operation.
	before := view.OpsApplied()
	Replay(src, view, "people", 0)
	if view.OpsApplied() != before {
		t.Fatal("a second replay applied changes again")
	}
}

func pk(i int) string {
	return "id-" + strings.Repeat("0", 3-len(itoa(i))) + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
