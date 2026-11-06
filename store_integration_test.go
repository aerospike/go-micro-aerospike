//go:build integration

package aerospike

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	aero "github.com/aerospike/aerospike-client-go/v8"
	"go-micro.dev/v6/store"
)

// testAddr returns seed hosts for the fixture.
func testAddr() string {
	if v := os.Getenv("AEROSPIKE_ADDR"); v != "" {
		return v
	}
	return "127.0.0.1:3000"
}

func newTestStore(t *testing.T, opts ...store.Option) store.Store {
	t.Helper()
	s := NewStore(append([]store.Option{
		store.Nodes(testAddr()),
		store.Database("test"),
	}, opts...)...)
	if err := s.Init(); err != nil {
		t.Fatalf("init: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// table is the ported nats-js-kv fixture, with databases mapped to the
// fixture's test/test2/test3 namespaces.
type trec struct {
	Record   *store.Record
	Database string
	Table    string
}

var table = []trec{
	{Record: &store.Record{Key: "One", Value: []byte("First value")}},
	{Record: &store.Record{Key: "Two", Value: []byte("Second value")}, Table: "prefix_test"},
	{Record: &store.Record{Key: "Third", Value: []byte("Third value")}, Database: "test2"},
	{Record: &store.Record{Key: "Four", Value: []byte("Fourth value")}, Database: "test2", Table: "prefix_test"},
	{Record: &store.Record{Key: "empty-value", Value: []byte{}}, Database: "test2"},
	{Record: &store.Record{Key: "Alex", Value: []byte("Some value")}, Database: "test2", Table: "names"},
	{Record: &store.Record{Key: "Jones", Value: []byte("Some value")}, Database: "test2", Table: "names"},
	{Record: &store.Record{Key: "Adrianna", Value: []byte("Some value")}, Database: "test2", Table: "names"},
	{Record: &store.Record{Key: "MexicoCity", Value: []byte("Some value")}, Database: "test2", Table: "cities"},
	{Record: &store.Record{Key: "HoustonCity", Value: []byte("Some value")}, Database: "test2", Table: "cities"},
	{Record: &store.Record{Key: "ZurichCity", Value: []byte("Some value")}, Database: "test2", Table: "cities"},
	{Record: &store.Record{Key: "Helsinki", Value: []byte("Some value")}, Database: "test2", Table: "cities"},
	{Record: &store.Record{Key: "testKeytest", Value: []byte("Some value")}, Table: "some_table"},
	{Record: &store.Record{Key: "testSecondtest", Value: []byte("Some value")}, Table: "some_table"},
	{Record: &store.Record{Key: "lalala", Value: []byte("Other value")}, Table: "some_table"},
}

// seedTable writes the ported fixture into unique tables per test run so runs
// do not see each other's data.
func seedTable(t *testing.T, s store.Store, run string) {
	t.Helper()
	for _, r := range table {
		tab := r.Table
		if tab != "" {
			tab = run + "/" + tab
		}
		db := r.Database
		if db == "" {
			db = "test"
		}
		if err := s.Write(r.Record, store.WriteTo(db, tab)); err != nil {
			t.Fatalf("seed write %s/%s: %v", db, r.Record.Key, err)
		}
	}
}

func TestString(t *testing.T) {
	s := newTestStore(t)
	if s.String() != "aerospike" {
		t.Fatalf("String() = %q", s.String())
	}
}

func TestBasicRoundTrip(t *testing.T) {
	s := newTestStore(t)
	run := fmt.Sprintf("basic-%d", time.Now().UnixNano())
	seedTable(t, s, run)

	for _, r := range table {
		tab := r.Table
		if tab != "" {
			tab = run + "/" + tab
		}
		db := r.Database
		if db == "" {
			db = "test"
		}
		got, err := s.Read(r.Record.Key, store.ReadFrom(db, tab))
		if err != nil {
			t.Fatalf("read %s/%s: %v", db, r.Record.Key, err)
		}
		if len(got) != 1 {
			t.Fatalf("read %s: got %d records", r.Record.Key, len(got))
		}
		if got[0].Key != r.Record.Key {
			t.Fatalf("record key %q, want %q", got[0].Key, r.Record.Key)
		}
		if string(got[0].Value) != string(r.Record.Value) {
			t.Fatalf("value %q, want %q", got[0].Value, r.Record.Value)
		}
	}

	// Missing key -> ErrNotFound.
	_, err := s.Read("no-such-key", store.ReadFrom("test", run+"/none"))
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestList(t *testing.T) {
	s := newTestStore(t)
	run := fmt.Sprintf("list-%d", time.Now().UnixNano())
	seedTable(t, s, run)

	namesTab := run + "/names"
	citiesTab := run + "/cities"
	someTab := run + "/some_table"
	prefixTab := run + "/prefix_test"

	cases := []struct {
		Database string
		Table    string
		Length   int
		Prefix   string
		Suffix   string
		Offset   int
		Limit    int
	}{
		{Database: "test", Table: "micro", Prefix: "One", Length: 1},
		{Database: "test2", Table: namesTab, Length: 3},
		{Database: "test2", Table: citiesTab, Length: 4},
		{Database: "test2", Table: citiesTab, Suffix: "City", Length: 3},
		{Database: "test2", Table: citiesTab, Suffix: "City", Limit: 2, Length: 2},
		{Database: "test2", Table: citiesTab, Suffix: "City", Offset: 1, Length: 2},
		{Database: "test", Table: someTab, Prefix: "test", Suffix: "test", Length: 2},
		{Database: "test", Table: someTab, Prefix: "test", Length: 2},
		{Database: "test", Table: prefixTab, Length: 1},
		{Database: "test2", Table: namesTab, Offset: 2, Limit: 3, Length: 1},
	}
	for i, c := range cases {
		keys, err := s.List(
			store.ListFrom(c.Database, c.Table),
			store.ListPrefix(c.Prefix),
			store.ListSuffix(c.Suffix),
			store.ListOffset(uint(c.Offset)),
			store.ListLimit(uint(c.Limit)),
		)
		if err != nil {
			t.Fatalf("case %d: %v", i+1, err)
		}
		if len(keys) != c.Length {
			t.Fatalf("case %d (%+v): got %d keys %v", i+1, c, len(keys), keys)
		}
		if !sort.StringsAreSorted(keys) {
			t.Fatalf("case %d: keys not sorted: %v", i+1, keys)
		}
	}
}

// TestListOrdering checks lexicographic order and paging explicitly.
func TestListOrdering(t *testing.T) {
	s := newTestStore(t)
	tab := fmt.Sprintf("ord-%d", time.Now().UnixNano())
	for _, k := range []string{"a", "b", "c", "d", "e", "f", "g"} {
		if err := s.Write(&store.Record{Key: k, Value: []byte(k)}, store.WriteTo("test", tab)); err != nil {
			t.Fatal(err)
		}
	}
	keys, err := s.List(store.ListFrom("test", tab), store.ListOffset(2), store.ListLimit(3))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"c", "d", "e"}
	if fmt.Sprint(keys) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", keys, want)
	}
}

func TestListPrefixMetacharacters(t *testing.T) {
	s := newTestStore(t)
	tab := fmt.Sprintf("meta-%d", time.Now().UnixNano())
	for _, k := range []string{"a.b", "axb", "a|b", "a(b)"} {
		if err := s.Write(&store.Record{Key: k, Value: []byte("v")}, store.WriteTo("test", tab)); err != nil {
			t.Fatal(err)
		}
	}
	keys, err := s.List(store.ListFrom("test", tab), store.ListPrefix("a."))
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != "a.b" {
		t.Fatalf("expected only a.b for literal prefix 'a.', got %v", keys)
	}
	keys, err = s.List(store.ListFrom("test", tab), store.ListSuffix(".b"))
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != "a.b" {
		t.Fatalf("expected only a.b for literal suffix '.b', got %v", keys)
	}
	keys, err = s.List(store.ListFrom("test", tab), store.ListPrefix("a|"))
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != "a|b" {
		t.Fatalf("expected only a|b for literal prefix 'a|', got %v", keys)
	}
}

func TestEmptyTableProbe(t *testing.T) {
	s := newTestStore(t)
	tab := fmt.Sprintf("empty-%d", time.Now().UnixNano())
	start := time.Now()
	keys, err := s.List(store.ListFrom("test", tab), store.ListLimit(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Fatalf("expected empty list, got %v", keys)
	}
	// The index query returns quickly even when the set is populated.
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("empty-table probe took %v", elapsed)
	}
}

func TestDelete(t *testing.T) {
	s := newTestStore(t)
	run := fmt.Sprintf("del-%d", time.Now().UnixNano())
	seedTable(t, s, run)
	for _, r := range table {
		tab := r.Table
		if tab != "" {
			tab = run + "/" + tab
		}
		db := r.Database
		if db == "" {
			db = "test"
		}
		if err := s.Delete(r.Record.Key, store.DeleteFrom(db, tab)); err != nil {
			t.Fatalf("delete: %v", err)
		}
		// Delete of a missing key is idempotent.
		if err := s.Delete(r.Record.Key, store.DeleteFrom(db, tab)); err != nil {
			t.Fatalf("re-delete: %v", err)
		}
		if _, err := s.Read(r.Record.Key, store.ReadFrom(db, tab)); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("expected ErrNotFound after delete, got %v", err)
		}
	}
}

func TestMetaData(t *testing.T) {
	s := newTestStore(t)
	rec := &store.Record{
		Key:   "KeyOne",
		Value: []byte("Some value"),
		Metadata: map[string]interface{}{
			"meta-one": "val",
			"meta-two": 5,
		},
	}
	if err := s.Write(rec); err != nil {
		t.Fatal(err)
	}
	r, err := s.Read(rec.Key)
	if err != nil {
		t.Fatal(err)
	}
	if len(r) == 0 {
		t.Fatal("no results")
	}
	if r[0].Metadata["meta-one"] != "val" {
		t.Fatalf("metadata mismatch: %+v", r[0].Metadata)
	}
}

func TestTTL(t *testing.T) {
	s := newTestStore(t)
	rec := &store.Record{Key: "ephemeral", Value: []byte("v")}
	if err := s.Write(rec, store.WriteTTL(1500*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	// Remaining TTL is reported.
	r, err := s.Read("ephemeral")
	if err != nil {
		t.Fatal(err)
	}
	if r[0].Expiry <= 0 || r[0].Expiry > 3*time.Second {
		t.Fatalf("expected remaining expiry ~1.5s, got %v", r[0].Expiry)
	}
	time.Sleep(3 * time.Second)
	if _, err := s.Read("ephemeral"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected expiry, got %v", err)
	}
}

func TestNoExpiryByDefault(t *testing.T) {
	s := newTestStore(t)
	if err := s.Write(&store.Record{Key: "persist", Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	r, err := s.Read("persist")
	if err != nil {
		t.Fatal(err)
	}
	if r[0].Expiry != 0 {
		t.Fatalf("expected no expiry, got %v", r[0].Expiry)
	}
}

func TestOverwriteReplaces(t *testing.T) {
	s := newTestStore(t)
	rec := &store.Record{Key: "ow", Value: []byte("v1"), Metadata: map[string]interface{}{"a": "b"}}
	if err := s.Write(rec); err != nil {
		t.Fatal(err)
	}
	rec = &store.Record{Key: "ow", Value: []byte("v2")}
	if err := s.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(rec); err != nil {
		t.Fatal(err)
	}
	r, err := s.Read("ow")
	if err != nil {
		t.Fatal(err)
	}
	if string(r[0].Value) != "v2" || len(r[0].Metadata) != 0 {
		t.Fatalf("overwrite did not replace: %+v", r[0])
	}
}

func TestScopeIsolation(t *testing.T) {
	s := newTestStore(t)
	run := fmt.Sprintf("scope-%d", time.Now().UnixNano())
	s1 := store.Scope(s, "test", run+"/t1")
	s2 := store.Scope(s, "test", run+"/t2")
	if err := s1.Write(&store.Record{Key: "shared", Value: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	if err := s2.Write(&store.Record{Key: "shared", Value: []byte("two")}); err != nil {
		t.Fatal(err)
	}
	r1, _ := s1.Read("shared")
	r2, _ := s2.Read("shared")
	if string(r1[0].Value) != "one" || string(r2[0].Value) != "two" {
		t.Fatalf("scope leak: %q %q", r1[0].Value, r2[0].Value)
	}
	keys, err := s1.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 {
		t.Fatalf("scoped list leaked: %v", keys)
	}
}

func TestChunkedRoundTrip(t *testing.T) {
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024))
	val := make([]byte, 10*1024+13)
	rand.Read(val)
	if err := s.Write(&store.Record{Key: "big", Value: val}); err != nil {
		t.Fatal(err)
	}
	r, err := s.Read("big")
	if err != nil {
		t.Fatal(err)
	}
	if string(r[0].Value) != string(val) {
		t.Fatalf("chunked round trip mismatch: %d bytes", len(r[0].Value))
	}
	// Chunks must not appear in listings.
	keys, err := s.List(store.ListFrom("test", "micro"), store.ListPrefix("big"))
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != "big" {
		t.Fatalf("listing returned %v", keys)
	}
	// Overwrite cleans old chunks.
	val2 := make([]byte, 3*1024)
	rand.Read(val2)
	if err := s.Write(&store.Record{Key: "big", Value: val2}); err != nil {
		t.Fatal(err)
	}
	r, err = s.Read("big")
	if err != nil {
		t.Fatal(err)
	}
	if string(r[0].Value) != string(val2) {
		t.Fatal("overwrite mismatch")
	}
	// Delete removes everything.
	if err := s.Delete("big"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Read("big"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestChunkingDisabled(t *testing.T) {
	s := newTestStore(t, WithInlineThreshold(1024), WithChunking(false))
	if err := s.Write(&store.Record{Key: "d", Value: []byte("small")}); err != nil {
		t.Fatal(err)
	}
	err := s.Write(&store.Record{Key: "d", Value: make([]byte, 2048)})
	if !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("expected ErrValueTooLarge, got %v", err)
	}
	r, err := s.Read("d")
	if err != nil || string(r[0].Value) != "small" {
		t.Fatalf("old value lost: %v %+v", err, r)
	}
}

func TestTotalCapPreservesValue(t *testing.T) {
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024), WithMaxValueSize(4096))
	if err := s.Write(&store.Record{Key: "cap", Value: []byte("orig")}); err != nil {
		t.Fatal(err)
	}
	err := s.Write(&store.Record{Key: "cap", Value: make([]byte, 5000)})
	if !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("expected ErrValueTooLarge, got %v", err)
	}
	r, err := s.Read("cap")
	if err != nil || string(r[0].Value) != "orig" {
		t.Fatalf("old value lost: %v", err)
	}
}

func TestSweep(t *testing.T) {
	// Isolated chunk set so orphans from other tests don't affect the count,
	// and no sweep grace so orphans are reclaimed immediately.
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024),
		WithChunkSet(fmt.Sprintf("swp%d", time.Now().UnixNano())), WithSweepGrace(0))
	if err := s.Write(&store.Record{Key: "sw", Value: make([]byte, 4096)}); err != nil {
		t.Fatal(err)
	}
	as := s.(*aerospikeStore)
	n, err := as.Sweep()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("sweep deleted %d referenced chunks", n)
	}
	r, err := s.Read("sw")
	if err != nil || len(r[0].Value) != 4096 {
		t.Fatalf("sweep broke record: %v", err)
	}
	// Orphan the chunks by deleting only the head through the raw client,
	// then confirm sweep removes exactly those chunks.
	head, _ := aero.NewKey("test", "micro", encodeKey("micro", "sw"))
	if _, err := as.client.Delete(nil, head); err != nil {
		t.Fatal(err)
	}
	n, err = as.Sweep()
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("expected 4 orphans deleted, got %d", n)
	}
}

func TestMissingIndexError(t *testing.T) {
	// test2's noindex_set has no index on t, and creation is disabled.
	s := NewStore(
		store.Nodes(testAddr()),
		store.Database("test2"),
		WithDataSet("noindex_set"),
		WithCreateIndex(false),
	)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, err := s.List(store.ListFrom("test2", "anything"))
	var miErr *MissingIndexError
	if !errors.As(err, &miErr) {
		t.Fatalf("expected MissingIndexError, got %v", err)
	}
}

func TestUnknownNamespace(t *testing.T) {
	s := newTestStore(t)
	err := s.Write(&store.Record{Key: "k", Value: []byte("v")}, store.WriteTo("no_such_ns", "t"))
	if err == nil || !strings.Contains(err.Error(), "no_such_ns") {
		t.Fatalf("expected namespace error, got %v", err)
	}
}

func TestNoSetPerTable(t *testing.T) {
	s := newTestStore(t)
	as := s.(*aerospikeStore)
	setNames := func() map[string]bool {
		res, err := as.client.GetNodes()[0].RequestInfo(&aero.InfoPolicy{}, "sets/test")
		if err != nil {
			t.Fatal(err)
		}
		names := map[string]bool{}
		for _, e := range strings.Split(res["sets/test"], ";") {
			if i := strings.Index(e, "set="); i >= 0 {
				names[e[i+4:strings.Index(e[i:], ":")+i]] = true
			}
		}
		return names
	}
	before := setNames()
	run := fmt.Sprintf("sets-%d", time.Now().UnixNano())
	for i := 0; i < 50; i++ {
		tab := fmt.Sprintf("%s/%d", run, i)
		if err := s.Write(&store.Record{Key: "k", Value: []byte("v")}, store.WriteTo("test", tab)); err != nil {
			t.Fatal(err)
		}
	}
	after := setNames()
	if len(after) != len(before) {
		t.Fatalf("tables created sets.\nbefore: %v\nafter:  %v", before, after)
	}
}

func TestConcurrentWriters(t *testing.T) {
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024))
	a := make([]byte, 8192)
	b := make([]byte, 8192)
	for i := range a {
		a[i] = 'a'
	}
	for i := range b {
		b[i] = 'b'
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); s.Write(&store.Record{Key: "race", Value: a}) }()
		go func() { defer wg.Done(); s.Write(&store.Record{Key: "race", Value: b}) }()
	}
	wg.Wait()
	r, err := s.Read("race")
	if err != nil {
		t.Fatal(err)
	}
	got := r[0].Value
	if string(got) != string(a) && string(got) != string(b) {
		t.Fatalf("read returned torn value of %d bytes", len(got))
	}
}

func TestTTLPrecedence(t *testing.T) {
	s := newTestStore(t)
	// WriteTTL beats WriteExpiry and Record.Expiry.
	rec := &store.Record{Key: "ttl-prec", Value: []byte("v"), Expiry: time.Hour}
	err := s.Write(rec, store.WriteTTL(2500*time.Millisecond), store.WriteExpiry(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Read("ttl-prec")
	if err != nil {
		t.Fatal(err)
	}
	if r[0].Expiry <= 0 || r[0].Expiry > 10*time.Second {
		t.Fatalf("WriteTTL should win (~2.5s), got %v", r[0].Expiry)
	}
}

func TestCorruptionMissingChunk(t *testing.T) {
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024), WithReaderRetries(0))
	if err := s.Write(&store.Record{Key: "corrupt", Value: make([]byte, 4096)}); err != nil {
		t.Fatal(err)
	}
	as := s.(*aerospikeStore)
	// Read the head's version and delete one chunk directly.
	head, _ := aero.NewKey("test", "micro", encodeKey("micro", "corrupt"))
	h, err := as.client.Get(nil, head, binChunkVersion)
	if err != nil {
		t.Fatal(err)
	}
	version := h.Bins[binChunkVersion].(string)
	ck, _ := aero.NewKey("test", "micro_chunks", chunkKey(encodeKey("micro", "corrupt"), version, 2))
	if _, err := as.client.Delete(nil, ck); err != nil {
		t.Fatal(err)
	}
	_, rerr := s.Read("corrupt")
	if !errors.Is(rerr, ErrCorruption) {
		t.Fatalf("expected ErrCorruption, got %v", rerr)
	}
}

func TestUnknownLayoutVersion(t *testing.T) {
	s := newTestStore(t)
	as := s.(*aerospikeStore)
	head, _ := aero.NewKey("test", "micro", encodeKey("micro", "oldrec"))
	wp := aero.NewWritePolicy(0, aero.TTLDontExpire)
	if err := as.client.PutBins(wp, head,
		aero.NewBin(binTable, "micro"),
		aero.NewBin(binKey, "oldrec"),
		aero.NewBin(binChunkCount, 1),
		aero.NewBin(binChunkVersion, "abc"),
		aero.NewBin(binLayoutVersion, 99),
	); err != nil {
		t.Fatal(err)
	}
	_, err := s.Read("oldrec")
	if !errors.Is(err, ErrUnknownLayout) {
		t.Fatalf("expected ErrUnknownLayout, got %v", err)
	}
}

func TestReaderRetriesAgainstWriter(t *testing.T) {
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024))
	v1 := make([]byte, 4096)
	v2 := make([]byte, 4096)
	for i := range v1 {
		v1[i] = '1'
		v2[i] = '2'
	}
	if err := s.Write(&store.Record{Key: "race2", Value: v1}); err != nil {
		t.Fatal(err)
	}
	as := s.(*aerospikeStore)
	injected := false
	as.beforeChunkFetch = func() {
		if injected {
			return
		}
		injected = true
		// A writer commits v2 and deletes v1's chunks before our chunk read.
		if err := s.Write(&store.Record{Key: "race2", Value: v2}); err != nil {
			t.Errorf("inject write: %v", err)
		}
	}
	r, err := s.Read("race2")
	as.beforeChunkFetch = nil
	if err != nil {
		t.Fatal(err)
	}
	if string(r[0].Value) != string(v2) {
		t.Fatalf("expected complete v2 value, got %d bytes of '%c'", len(r[0].Value), r[0].Value[0])
	}
}

func chunkCount(t *testing.T, s *aerospikeStore) int {
	t.Helper()
	res, err := s.client.GetNodes()[0].RequestInfo(&aero.InfoPolicy{}, "sets/test")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range strings.Split(res["sets/test"], ";") {
		if strings.Contains(e, "set=micro_chunks") {
			var n int
			fmt.Sscanf(e[strings.Index(e, "objects="):], "objects=%d", &n)
			return n
		}
	}
	return 0
}

func TestVerifyInlineOverwriteCleanup(t *testing.T) {
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024))
	as := s.(*aerospikeStore)
	if err := s.Write(&store.Record{Key: "orph2", Value: make([]byte, 4096)}); err != nil {
		t.Fatal(err)
	}
	before := chunkCount(t, as)
	if err := s.Write(&store.Record{Key: "orph2", Value: []byte("small")}); err != nil {
		t.Fatal(err)
	}
	after := chunkCount(t, as)
	if after != before-4 {
		t.Fatalf("expected old chunks cleaned (%d -> %d)", before, after)
	}
	r, err := s.Read("orph2")
	if err != nil || string(r[0].Value) != "small" {
		t.Fatalf("read: %v", err)
	}
}

func TestVerifyMixedVersion(t *testing.T) {
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024))
	v1 := make([]byte, 4096)
	v2 := make([]byte, 4096)
	for i := range v2 {
		v2[i] = 'z'
	}
	if err := s.Write(&store.Record{Key: "mv2", Value: v1, Metadata: map[string]interface{}{"rev": 1}}); err != nil {
		t.Fatal(err)
	}
	as := s.(*aerospikeStore)
	injected := false
	as.beforeChunkFetch = func() {
		if injected {
			return
		}
		injected = true
		if err := s.Write(&store.Record{Key: "mv2", Value: v2, Metadata: map[string]interface{}{"rev": 2}}); err != nil {
			t.Errorf("inject: %v", err)
		}
	}
	r, err := s.Read("mv2")
	as.beforeChunkFetch = nil
	if err != nil {
		t.Fatal(err)
	}
	if r[0].Metadata["rev"] != 2 {
		t.Fatalf("expected metadata rev 2 with v2 value, got %v", r[0].Metadata)
	}
}

func TestVerifyOverlapPrefixSuffix(t *testing.T) {
	s := newTestStore(t)
	tab := fmt.Sprintf("ovl2-%d", time.Now().UnixNano())
	for _, k := range []string{"aba", "abXba", "abba"} {
		if err := s.Write(&store.Record{Key: k, Value: []byte("v")}, store.WriteTo("test", tab)); err != nil {
			t.Fatal(err)
		}
	}
	keys, err := s.List(store.ListFrom("test", tab), store.ListPrefix("ab"), store.ListSuffix("ba"))
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 3 {
		t.Fatalf("expected all 3 keys, got %v", keys)
	}
}

// Ensure the corruption path still fires for a genuinely missing chunk.
func TestVerifyCorruptionStillDetected(t *testing.T) {
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024), WithReaderRetries(1))
	if err := s.Write(&store.Record{Key: "corr2", Value: make([]byte, 4096)}); err != nil {
		t.Fatal(err)
	}
	as := s.(*aerospikeStore)
	head, _ := aero.NewKey("test", "micro", encodeKey("micro", "corr2"))
	h, err := as.client.Get(nil, head, binChunkVersion)
	if err != nil {
		t.Fatal(err)
	}
	ck, _ := aero.NewKey("test", "micro_chunks", chunkKey(encodeKey("micro", "corr2"), h.Bins[binChunkVersion].(string), 2))
	if _, err := as.client.Delete(nil, ck); err != nil {
		t.Fatal(err)
	}
	_, rerr := s.Read("corr2")
	if !errors.Is(rerr, ErrCorruption) {
		t.Fatalf("expected ErrCorruption, got %v", rerr)
	}
}

// Chunked head replaced by an inline value mid-read must return the inline value.
func TestVerifyInlineTransition(t *testing.T) {
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024))
	if err := s.Write(&store.Record{Key: "tr2", Value: make([]byte, 4096)}); err != nil {
		t.Fatal(err)
	}
	as := s.(*aerospikeStore)
	injected := false
	as.beforeChunkFetch = func() {
		if injected {
			return
		}
		injected = true
		if err := s.Write(&store.Record{Key: "tr2", Value: []byte("now-inline")}); err != nil {
			t.Errorf("inject: %v", err)
		}
	}
	r, err := s.Read("tr2")
	as.beforeChunkFetch = nil
	if err != nil {
		t.Fatal(err)
	}
	if string(r[0].Value) != "now-inline" {
		t.Fatalf("expected 'now-inline', got %d bytes", len(r[0].Value))
	}
}

// Chunks written by an in-flight (uncommitted) write must survive Sweep; only
// chunks older than the grace period are reclaimed.
func TestSweepGrace(t *testing.T) {
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024),
		WithChunkSet(fmt.Sprintf("grc%d", time.Now().UnixNano())), WithSweepGrace(1200*time.Millisecond))
	as := s.(*aerospikeStore)

	putChunks := func(enc, version string, n int) {
		wp := aero.NewWritePolicy(0, aero.TTLDontExpire)
		wp.RecordExistsAction = aero.REPLACE
		for i := 0; i < n; i++ {
			ck, _ := aero.NewKey("test", as.cfg.chunkSet, chunkKey(enc, version, i))
			if err := as.client.PutBins(wp, ck,
				aero.NewBin(binValue, make([]byte, 1024)),
				aero.NewBin(binChunkHead, enc),
				aero.NewBin(binChunkVer, version)); err != nil {
				t.Fatal(err)
			}
		}
	}
	commitHead := func(key, version string, n int) {
		hk, _ := aero.NewKey("test", as.cfg.dataSet, encodeKey("micro", key))
		wp := aero.NewWritePolicy(0, aero.TTLDontExpire)
		wp.RecordExistsAction = aero.REPLACE
		if err := as.client.PutBins(wp, hk,
			aero.NewBin(binTable, "micro"), aero.NewBin(binKey, key),
			aero.NewBin(binChunkCount, n), aero.NewBin(binChunkVersion, version),
			aero.NewBin(binLayoutVersion, layoutVersion)); err != nil {
			t.Fatal(err)
		}
	}

	// In-flight write: chunks present, head not yet committed.
	enc := encodeKey("micro", "inflight")
	putChunks(enc, "vnew", 4)
	if n, err := as.Sweep(); err != nil || n != 0 {
		t.Fatalf("sweep deleted %d in-flight chunks (err %v)", n, err)
	}
	commitHead("inflight", "vnew", 4)
	r, err := s.Read("inflight")
	if err != nil || len(r[0].Value) != 4096 {
		t.Fatalf("read after commit: %v %v", err, r)
	}

	// Genuine orphan older than the grace is reclaimed.
	putChunks(encodeKey("micro", "dead"), "vdead", 4)
	time.Sleep(1300 * time.Millisecond)
	if n, err := as.Sweep(); err != nil || n != 4 {
		t.Fatalf("expected 4 orphans swept, got %d (err %v)", n, err)
	}
	// The committed record's chunks are untouched.
	r, err = s.Read("inflight")
	if err != nil || len(r[0].Value) != 4096 {
		t.Fatalf("sweep broke record: %v", err)
	}
}

// Delete cleans chunks for the version committed at delete time.
func TestDeleteReadsVersionAtDeleteTime(t *testing.T) {
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024))
	if err := s.Write(&store.Record{Key: "dv", Value: make([]byte, 4096)}); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(&store.Record{Key: "dv", Value: make([]byte, 4096)}); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("dv"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Read("dv"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
