//go:build integration

package aerospike

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	aero "github.com/aerospike/aerospike-client-go/v8"
	"go-micro.dev/v6/store"
)

// rawClient returns the test store's Aerospike client for direct record
// manipulation. Fault states are crafted with the raw client because the
// store exposes no write-path injection seam (see TESTING.md section 3).
func rawClient(t *testing.T, s store.Store) *aero.Client {
	t.Helper()
	return s.(*aerospikeStore).client
}

// putChunks writes n chunk records for a version exactly as writeChunked does.
func putChunks(t *testing.T, c *aero.Client, as *aerospikeStore, ns, enc, version string, n int, payload []byte) {
	t.Helper()
	wp := aero.NewWritePolicy(0, aero.TTLDontExpire)
	wp.RecordExistsAction = aero.REPLACE
	for i := 0; i < n; i++ {
		ck, _ := aero.NewKey(ns, as.cfg.chunkSet, chunkKey(enc, version, i))
		if err := c.PutBins(wp, ck,
			aero.NewBin(binValue, payload),
			aero.NewBin(binChunkHead, enc),
			aero.NewBin(binChunkVer, version)); err != nil {
			t.Fatal(err)
		}
	}
}

// commitHead writes a head record for a chunked value directly.
func commitHead(t *testing.T, c *aero.Client, as *aerospikeStore, ns, table, key, version string, n int) {
	t.Helper()
	hk, _ := aero.NewKey(ns, as.cfg.dataSet, encodeKey(table, key))
	wp := aero.NewWritePolicy(0, aero.TTLDontExpire)
	wp.RecordExistsAction = aero.REPLACE
	if err := c.PutBins(wp, hk,
		aero.NewBin(binTable, table), aero.NewBin(binKey, key),
		aero.NewBin(binChunkCount, n), aero.NewBin(binChunkVersion, version),
		aero.NewBin(binLayoutVersion, layoutVersion)); err != nil {
		t.Fatal(err)
	}
}

// chunkVersions returns every chunk (head, version) pair present in a chunk
// set, via a scan of the bookkeeping bins.
func chunkVersions(t *testing.T, c *aero.Client, chunkSet string) map[string][]string {
	t.Helper()
	rs, err := c.ScanAll(nil, "test", chunkSet, binChunkHead, binChunkVer)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	out := map[string][]string{}
	for res := range rs.Results() {
		if res.Err != nil {
			t.Fatal(res.Err)
		}
		if res.Record == nil {
			continue
		}
		enc, _ := res.Record.Bins[binChunkHead].(string)
		ver, _ := res.Record.Bins[binChunkVer].(string)
		out[enc] = append(out[enc], ver)
	}
	return out
}

func hasVersion(versions []string, v string) bool {
	for _, x := range versions {
		if x == v {
			return true
		}
	}
	return false
}

// --- Section 3: persistence fault assurance ---
//
// The store has no write-path fault-injection seam, so failure STATES are
// crafted with the raw client (partial chunk sets, uncommitted versions,
// orphaned chunks) and the contract invariants are asserted against the real
// server. See TESTING.md "Reported package defects / testability boundaries".

// TestPartialChunkWritePreservesOld crafts the state left by a chunk batch
// that fails part way: some v2 chunks written, head still v1.
func TestPartialChunkWritePreservesOld(t *testing.T) {
	chunkSet := fmt.Sprintf("fl%d", time.Now().UnixNano())
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024),
		WithChunkSet(chunkSet), WithSweepGrace(0))
	as := s.(*aerospikeStore)
	c := rawClient(t, s)

	v1 := []byte(strings.Repeat("a", 4096))
	if err := s.Write(&store.Record{Key: "pw", Value: v1}); err != nil {
		t.Fatal(err)
	}
	enc := encodeKey("micro", "pw")

	// Simulate a partial v2 chunk write: only 2 of 4 chunks land, head never
	// commits. (A whole-call BatchOperate error leaves this state.)
	putChunks(t, c, as, "test", enc, "v2partial", 2, []byte(strings.Repeat("b", 1024)))

	r, err := s.Read("pw")
	if err != nil {
		t.Fatal(err)
	}
	if string(r[0].Value) != string(v1) {
		t.Fatal("old value not preserved after partial chunk write")
	}
	// The uncommitted v2 chunks are orphans: sweep removes exactly them.
	n, err := as.Sweep()
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("expected 2 orphan chunks swept, got %d", n)
	}
	if r, err = s.Read("pw"); err != nil || string(r[0].Value) != string(v1) {
		t.Fatalf("sweep removed referenced chunks: %v", err)
	}
}

// TestPerRecordChunkErrorPreservesOld crafts the state left by a per-record
// batch error: all but one v2 chunk lands, head still v1.
func TestPerRecordChunkErrorPreservesOld(t *testing.T) {
	chunkSet := fmt.Sprintf("pr%d", time.Now().UnixNano())
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024),
		WithChunkSet(chunkSet), WithSweepGrace(0))
	as := s.(*aerospikeStore)
	c := rawClient(t, s)

	v1 := []byte(strings.Repeat("c", 3072))
	if err := s.Write(&store.Record{Key: "pe", Value: v1}); err != nil {
		t.Fatal(err)
	}
	enc := encodeKey("micro", "pe")
	// 3 chunks needed for v2; only chunks 0 and 2 land (chunk 1 failed).
	putChunks(t, c, as, "test", enc, "v2rec", 1, []byte(strings.Repeat("d", 1024)))
	wp := aero.NewWritePolicy(0, aero.TTLDontExpire)
	ck, _ := aero.NewKey("test", chunkSet, chunkKey(enc, "v2rec", 2))
	if err := c.PutBins(wp, ck,
		aero.NewBin(binValue, []byte(strings.Repeat("d", 1024))),
		aero.NewBin(binChunkHead, enc), aero.NewBin(binChunkVer, "v2rec")); err != nil {
		t.Fatal(err)
	}

	r, err := s.Read("pe")
	if err != nil || string(r[0].Value) != string(v1) {
		t.Fatalf("old value lost: %v", err)
	}
	if n, err := as.Sweep(); err != nil || n != 2 {
		t.Fatalf("expected 2 orphans swept, got %d (err %v)", n, err)
	}
}

// TestHeadCommitRejectedPreservesOld crafts the definite-rejection state:
// all v2 chunks written, but the head replacement was rejected before
// application, so the head still commits to v1.
func TestHeadCommitRejectedPreservesOld(t *testing.T) {
	chunkSet := fmt.Sprintf("hr%d", time.Now().UnixNano())
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024),
		WithChunkSet(chunkSet), WithSweepGrace(0))
	as := s.(*aerospikeStore)
	c := rawClient(t, s)

	v1 := []byte(strings.Repeat("e", 4096))
	if err := s.Write(&store.Record{Key: "hr", Value: v1}); err != nil {
		t.Fatal(err)
	}
	enc := encodeKey("micro", "hr")
	putChunks(t, c, as, "test", enc, "v2rej", 4, []byte(strings.Repeat("f", 1024)))
	// Head commit definitely rejected before application: head unchanged.

	r, err := s.Read("hr")
	if err != nil || string(r[0].Value) != string(v1) {
		t.Fatalf("old value lost after definite head rejection: %v", err)
	}
	if n, err := as.Sweep(); err != nil || n != 4 {
		t.Fatalf("expected 4 orphans swept, got %d (err %v)", n, err)
	}
}

// TestUncertainCommitOutcome crafts the applied-but-response-lost state: the
// v2 head IS committed (the writer never learned). Readers must see only a
// complete committed version.
func TestUncertainCommitOutcome(t *testing.T) {
	chunkSet := fmt.Sprintf("uc%d", time.Now().UnixNano())
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024),
		WithChunkSet(chunkSet), WithSweepGrace(0))
	as := s.(*aerospikeStore)
	c := rawClient(t, s)

	v1 := []byte(strings.Repeat("g", 2048))
	if err := s.Write(&store.Record{Key: "uc", Value: v1}); err != nil {
		t.Fatal(err)
	}
	enc := encodeKey("micro", "uc")
	putChunks(t, c, as, "test", enc, "v2lost", 2, []byte(strings.Repeat("h", 1024)))
	commitHead(t, c, as, "test", "micro", "uc", "v2lost", 2)

	r, err := s.Read("uc")
	if err != nil {
		t.Fatal(err)
	}
	if string(r[0].Value) != strings.Repeat("h", 2048) {
		t.Fatalf("expected complete committed v2, got %d bytes", len(r[0].Value))
	}
	// Orphaned v1 chunks are still reclaimable.
	if n, err := as.Sweep(); err != nil || n != 2 {
		t.Fatalf("expected 2 v1 orphans swept, got %d (err %v)", n, err)
	}
}

// TestCleanupFailureSweepRecovery crafts the state left when old-chunk
// cleanup fails after a successful commit: v2 committed and readable, v1
// chunks still present, then sweep reclaims exactly the v1 chunks.
func TestCleanupFailureSweepRecovery(t *testing.T) {
	chunkSet := fmt.Sprintf("cf%d", time.Now().UnixNano())
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024),
		WithChunkSet(chunkSet), WithSweepGrace(0))
	as := s.(*aerospikeStore)
	c := rawClient(t, s)

	v1 := []byte(strings.Repeat("i", 4096))
	if err := s.Write(&store.Record{Key: "cf", Value: v1}); err != nil {
		t.Fatal(err)
	}
	enc := encodeKey("micro", "cf")
	putChunks(t, c, as, "test", enc, "v2keep", 4, []byte(strings.Repeat("j", 1024)))
	commitHead(t, c, as, "test", "micro", "cf", "v2keep", 4)
	// Cleanup of v1 chunks "failed" — they remain.

	r, err := s.Read("cf")
	if err != nil || string(r[0].Value) != strings.Repeat("j", 4096) {
		t.Fatalf("new value not readable: %v", err)
	}
	n, err := as.Sweep()
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("expected exactly the 4 v1 orphans swept, got %d", n)
	}
	// v2 chunks survive: exact-version inspection.
	versions := chunkVersions(t, c, chunkSet)
	if got := versions[enc]; len(got) != 4 || !hasVersion(got, "v2keep") {
		t.Fatalf("referenced v2 chunks removed: %v", got)
	}
}

// TestDeleteLeavesExactVersionOrphans: a delete that fails to clean chunks
// orphans exactly the version committed at delete time.
func TestDeleteLeavesExactVersionOrphans(t *testing.T) {
	chunkSet := fmt.Sprintf("dv%d", time.Now().UnixNano())
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024),
		WithChunkSet(chunkSet), WithSweepGrace(0))
	as := s.(*aerospikeStore)
	c := rawClient(t, s)

	if err := s.Write(&store.Record{Key: "dk", Value: make([]byte, 4096)}); err != nil {
		t.Fatal(err)
	}
	enc := encodeKey("micro", "dk")
	hk, _ := aero.NewKey("test", as.cfg.dataSet, enc)
	if _, err := c.Delete(nil, hk); err != nil { // head gone, chunks stay
		t.Fatal(err)
	}
	n, err := as.Sweep()
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("expected 4 orphans after head delete, got %d", n)
	}
}

// --- Section 4: CE read, query and index assurance ---

// TestReadPrefixSuffix covers prefix/suffix Read pages: filtering, ordering,
// pagination, empty selections, literal metacharacters, mixed inline/chunked
// values and per-record metadata.
func TestReadPrefixSuffix(t *testing.T) {
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024))
	tab := fmt.Sprintf("rp-%d", time.Now().UnixNano())
	chunked := make([]byte, 4096)
	rand.Read(chunked)
	recs := []*store.Record{
		{Key: "user.a.b", Value: []byte("v-ab"), Metadata: map[string]interface{}{"i": 1}},
		{Key: "useraxb", Value: []byte("v-axb"), Metadata: map[string]interface{}{"i": 2}},
		{Key: "user.c", Value: chunked, Metadata: map[string]interface{}{"i": 3}},
		{Key: "other", Value: []byte("v-o")},
	}
	for _, r := range recs {
		if err := s.Write(r, store.WriteTo("test", tab)); err != nil {
			t.Fatal(err)
		}
	}
	// Prefix only: literal '.' metacharacter (Read uses the key argument as
	// the prefix/suffix pattern).
	prefs, err := s.Read("user.", store.ReadFrom("test", tab), store.ReadPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if len(prefs) != 2 || prefs[0].Key != "user.a.b" || prefs[1].Key != "user.c" {
		t.Fatalf("prefix read keys/order wrong: %+v", keysOf(prefs))
	}
	if string(prefs[0].Value) != "v-ab" || string(prefs[1].Value) != string(chunked) {
		t.Fatal("prefix read values wrong (mixed inline/chunked)")
	}
	if prefs[0].Metadata["i"] != float64(1) && prefs[0].Metadata["i"] != 1 {
		t.Fatalf("multi-read metadata missing: %+v", prefs[0].Metadata)
	}
	// Suffix only.
	sufs, err := s.Read("b", store.ReadFrom("test", tab), store.ReadSuffix())
	if err != nil {
		t.Fatal(err)
	}
	if len(sufs) != 2 || sufs[0].Key != "user.a.b" || sufs[1].Key != "useraxb" {
		t.Fatalf("suffix read wrong: %+v", keysOf(sufs))
	}
	// Combined prefix+suffix: the key argument is used for both patterns, so
	// only keys that start AND end with it match.
	both, err := s.Read("user.a.b", store.ReadFrom("test", tab), store.ReadPrefix(), store.ReadSuffix())
	if err != nil {
		t.Fatal(err)
	}
	if len(both) != 1 || both[0].Key != "user.a.b" {
		t.Fatalf("combined read wrong: %+v", keysOf(both))
	}
	// Pagination.
	page, err := s.Read("user", store.ReadFrom("test", tab), store.ReadPrefix(), store.ReadOffset(1), store.ReadLimit(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1 || page[0].Key != "user.c" {
		t.Fatalf("paged read wrong: %+v", keysOf(page))
	}
	// Empty selection.
	empty, err := s.Read("nosuch", store.ReadFrom("test", tab), store.ReadPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("expected empty selection, got %+v", keysOf(empty))
	}
}

func keysOf(recs []*store.Record) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = r.Key
	}
	return out
}

// TestReadChunkKeyLikeUserKeys: a user key that resembles a chunk key does
// not interfere with a chunked record of a similar name.
func TestReadChunkKeyLikeUserKeys(t *testing.T) {
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024))
	tab := fmt.Sprintf("ck-%d", time.Now().UnixNano())
	if err := s.Write(&store.Record{Key: "foo", Value: make([]byte, 4096)}, store.WriteTo("test", tab)); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(&store.Record{Key: "foo|abc|0", Value: []byte("notachunk")}, store.WriteTo("test", tab)); err != nil {
		t.Fatal(err)
	}
	// Chunked record reads back fully.
	r, err := s.Read("foo", store.ReadFrom("test", tab))
	if err != nil || len(r[0].Value) != 4096 {
		t.Fatalf("chunked record broken: %v %v", err, r)
	}
	// The lookalike key is a normal record.
	r, err = s.Read("foo|abc|0", store.ReadFrom("test", tab))
	if err != nil || string(r[0].Value) != "notachunk" {
		t.Fatalf("lookalike key broken: %v %v", err, r)
	}
	// Prefix reads see both as ordinary records.
	recs, err := s.Read("foo", store.ReadFrom("test", tab), store.ReadPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("expected 2 records, got %+v", keysOf(recs))
	}
}

// TestReadPrefixRetryAgainstWriter forces a v1→v2 commit between a prefix
// read's head fetch and chunk fetch. The approved contract requires bounded
// head re-read and the complete v2.
//
// REPORTED DEFECT: the multi-record Read path calls recordFromHead directly
// (store.go:582) with no bounded retry, so the missing chunk yields
// ErrCorruption instead of a retried, complete v2. This regression is
// preserved failing until the production fix lands.
func TestReadPrefixRetryAgainstWriter(t *testing.T) {
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024))
	tab := fmt.Sprintf("rr-%d", time.Now().UnixNano())
	v1 := []byte(strings.Repeat("1", 4096))
	v2 := []byte(strings.Repeat("2", 4096))
	if err := s.Write(&store.Record{Key: "rr", Value: v1}, store.WriteTo("test", tab)); err != nil {
		t.Fatal(err)
	}
	as := s.(*aerospikeStore)
	injected := false
	as.beforeChunkFetch = func() {
		if injected {
			return
		}
		injected = true
		if err := s.Write(&store.Record{Key: "rr", Value: v2}, store.WriteTo("test", tab)); err != nil {
			t.Errorf("inject: %v", err)
		}
	}
	recs, err := s.Read("rr", store.ReadFrom("test", tab), store.ReadPrefix())
	as.beforeChunkFetch = nil
	if err != nil {
		t.Fatalf("multi-read must retry and return v2, got error %v", err)
	}
	if string(recs[0].Value) != string(v2) {
		t.Fatal("multi-read returned torn value")
	}
}

// TestReadSelectedHeadDisappears documents the approved disappearance
// semantics: a head deleted between selection and fetch is omitted.
// There is no hook between listKeys and BatchGet, so the disappearance window
// cannot be forced deterministically; this test verifies the steady-state
// half (deleted records are simply absent from the page). The injection
// boundary is reported in TESTING.md as a testability gap.
func TestReadSelectedHeadDisappears(t *testing.T) {
	s := newTestStore(t)
	tab := fmt.Sprintf("dh-%d", time.Now().UnixNano())
	for _, k := range []string{"a", "b", "c"} {
		if err := s.Write(&store.Record{Key: k, Value: []byte(k)}, store.WriteTo("test", tab)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Delete("b", store.DeleteFrom("test", tab)); err != nil {
		t.Fatal(err)
	}
	recs, err := s.Read("", store.ReadFrom("test", tab), store.ReadPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 || recs[0].Key != "a" || recs[1].Key != "c" {
		t.Fatalf("expected [a c], got %+v", keysOf(recs))
	}
}

// TestReadRetryExhaustion forces continuous head changes past the retry
// budget; the approved terminal outcome is ErrCorruption.
func TestReadRetryExhaustion(t *testing.T) {
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024), WithReaderRetries(2))
	if err := s.Write(&store.Record{Key: "ex", Value: make([]byte, 2048)}); err != nil {
		t.Fatal(err)
	}
	as := s.(*aerospikeStore)
	n := 0
	as.beforeChunkFetch = func() {
		n++
		// Every chunk fetch is preceded by a new committed version.
		v := []byte(strings.Repeat(fmt.Sprint(n), 2048))
		if err := s.Write(&store.Record{Key: "ex", Value: v}); err != nil {
			t.Errorf("inject: %v", err)
		}
	}
	_, err := s.Read("ex")
	as.beforeChunkFetch = nil
	if !errors.Is(err, ErrCorruption) {
		t.Fatalf("expected ErrCorruption on retry exhaustion, got %v", err)
	}
}

// TestMaxListKeys verifies the listing cap at and beyond the limit for List
// and multi-record Read, with and without a page limit, and that a rejected
// write leaves existing records intact.
func TestMaxListKeys(t *testing.T) {
	s := newTestStore(t, WithMaxListKeys(3))
	tab := fmt.Sprintf("ml-%d", time.Now().UnixNano())
	for i, k := range []string{"a", "b", "c"} {
		if err := s.Write(&store.Record{Key: k, Value: []byte{byte(i)}}, store.WriteTo("test", tab)); err != nil {
			t.Fatal(err)
		}
	}
	if keys, err := s.List(store.ListFrom("test", tab)); err != nil || len(keys) != 3 {
		t.Fatalf("at cap: %v %v", keys, err)
	}
	if err := s.Write(&store.Record{Key: "d", Value: []byte("x")}, store.WriteTo("test", tab)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.List(store.ListFrom("test", tab)); !errors.Is(err, ErrTooManyListKeys) {
		t.Fatalf("over cap List: expected ErrTooManyListKeys, got %v", err)
	}
	// Cap precedes paging: a page limit does not rescue it.
	if _, err := s.List(store.ListFrom("test", tab), store.ListLimit(2)); !errors.Is(err, ErrTooManyListKeys) {
		t.Fatalf("over cap paged List: expected ErrTooManyListKeys, got %v", err)
	}
	if _, err := s.Read("", store.ReadFrom("test", tab), store.ReadPrefix()); !errors.Is(err, ErrTooManyListKeys) {
		t.Fatalf("over cap Read: expected ErrTooManyListKeys, got %v", err)
	}
	// Existing records are intact.
	r, err := s.Read("a", store.ReadFrom("test", tab))
	if err != nil || len(r) != 1 {
		t.Fatalf("existing record lost: %v", err)
	}
}

// TestIndexLifecycle covers pre-created index reuse, per-call-namespace
// ensure, and missing-index failure with creation disabled.
func TestIndexLifecycle(t *testing.T) {
	// Pre-created index + creation disabled: Init succeeds, queries work.
	s := NewStore(store.Nodes(testAddr()), store.Database("test"),
		WithDataSet("micro"), WithCreateIndex(false))
	if err := s.Init(); err != nil {
		t.Fatalf("Init with pre-created index: %v", err)
	}
	if _, err := s.List(store.ListFrom("test", "micro")); err != nil {
		t.Fatalf("List on pre-created index: %v", err)
	}
	s.Close()

	// Per-call namespace, creation enabled: first use ensures the index.
	s = newTestStore(t)
	if err := s.Write(&store.Record{Key: "k", Value: []byte("v")}, store.WriteTo("test2", "t1")); err != nil {
		t.Fatalf("write to per-call namespace: %v", err)
	}
	if _, err := s.List(store.ListFrom("test2", "t1")); err != nil {
		t.Fatalf("list after per-call ensure: %v", err)
	}

	// Creation disabled + missing index on a fresh data set: query-time
	// MissingIndexError (approved fail-at-query semantics), no scan fallback.
	s2 := NewStore(store.Nodes(testAddr()), store.Database("test2"),
		WithDataSet(fmt.Sprintf("noix%d", time.Now().UnixNano())),
		WithCreateIndex(false))
	if err := s2.Init(); err != nil {
		t.Fatalf("Init (creation disabled) should succeed without index check: %v", err)
	}
	var miErr *MissingIndexError
	if _, err := s2.List(store.ListFrom("test2", "t")); !errors.As(err, &miErr) {
		t.Fatalf("expected MissingIndexError, got %v", err)
	}
	s2.Close()
}

// --- Section 5: concurrency and TTL assurance ---

// TestConcurrentWritersResults strengthens the concurrent-writer test: every
// write result is collected and asserted, then the committed record's value,
// metadata and expiry are verified coherent.
func TestConcurrentWritersResults(t *testing.T) {
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024))
	a := []byte(strings.Repeat("a", 8192))
	b := []byte(strings.Repeat("b", 8192))
	var wg sync.WaitGroup
	errs := make(chan error, 40)
	start := make(chan struct{})
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			errs <- s.Write(&store.Record{Key: "race", Value: a,
				Metadata: map[string]interface{}{"v": "a"}}, store.WriteTTL(30*time.Second))
		}()
		go func() {
			defer wg.Done()
			<-start
			errs <- s.Write(&store.Record{Key: "race", Value: b,
				Metadata: map[string]interface{}{"v": "b"}}, store.WriteTTL(30*time.Second))
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent write failed: %v", err)
		}
	}
	r, err := s.Read("race")
	if err != nil {
		t.Fatal(err)
	}
	v := r[0].Metadata["v"]
	var want []byte
	switch v {
	case "a":
		want = a
	case "b":
		want = b
	default:
		t.Fatalf("incoherent metadata: %v", v)
	}
	if string(r[0].Value) != string(want) {
		t.Fatalf("value/metadata from different versions: meta=%v", v)
	}
	if r[0].Expiry <= 0 || r[0].Expiry > 30*time.Second {
		t.Fatalf("expiry lost: %v", r[0].Expiry)
	}
}

// TestOverwriteDeleteInterleaving uses barrier-controlled goroutines plus
// exact-version inspection: an overwrite commits while a delete runs; the
// outcome is a coherent record or a clean delete, and chunk versions are
// checked exactly.
func TestOverwriteDeleteInterleaving(t *testing.T) {
	for i := 0; i < 5; i++ {
		chunkSet := fmt.Sprintf("oi%d-%d", time.Now().UnixNano(), i)
		s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024),
			WithChunkSet(chunkSet), WithSweepGrace(0))
		as := s.(*aerospikeStore)
		c := rawClient(t, s)
		key := fmt.Sprintf("od%d", i)
		if err := s.Write(&store.Record{Key: key, Value: make([]byte, 4096)}); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		start := make(chan struct{})
		var werr, derr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			werr = s.Write(&store.Record{Key: key, Value: []byte(strings.Repeat("z", 4096))})
		}()
		go func() {
			defer wg.Done()
			<-start
			derr = s.Delete(key)
		}()
		close(start)
		wg.Wait()
		if werr != nil || derr != nil {
			t.Fatalf("write=%v delete=%v", werr, derr)
		}
		_, rerr := s.Read(key)
		versions := chunkVersions(t, c, chunkSet)
		enc := encodeKey("micro", key)
		if errors.Is(rerr, store.ErrNotFound) {
			// Delete won: no chunks may survive.
			if len(versions[enc]) != 0 {
				t.Fatalf("delete left chunks: %v", versions[enc])
			}
		} else if rerr == nil {
			// Write won: exactly one version's chunks remain, all referenced.
			if len(versions[enc]) == 0 {
				t.Fatal("record readable but no chunks present")
			}
			if n, err := as.Sweep(); err != nil || n != 0 {
				t.Fatalf("sweep removed referenced chunks: n=%d err=%v", n, err)
			}
		} else {
			t.Fatalf("read after interleave: %v", rerr)
		}
		s.Close()
	}
}

// TestSweepDelayedWriter asserts the approved sweep contract: chunks older
// than the grace with no referencing head are reclaimed even if a writer was
// delayed beyond the grace (documented bounded-window assumption), and
// committed versions are never touched.
func TestSweepDelayedWriter(t *testing.T) {
	chunkSet := fmt.Sprintf("sd%d", time.Now().UnixNano())
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024),
		WithChunkSet(chunkSet), WithSweepGrace(1200*time.Millisecond))
	as := s.(*aerospikeStore)
	c := rawClient(t, s)
	enc := encodeKey("micro", "delayed")

	// Committed, referenced chunks.
	putChunks(t, c, as, "test", enc, "vref", 2, make([]byte, 1024))
	commitHead(t, c, as, "test", "micro", "delayed", "vref", 2)
	// Uncommitted chunks from a "delayed" write, allowed to age past grace.
	putChunks(t, c, as, "test", encodeKey("micro", "stalled"), "vlate", 2, make([]byte, 1024))
	time.Sleep(1300 * time.Millisecond)

	n, err := as.Sweep()
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("expected 2 aged orphans swept, got %d", n)
	}
	versions := chunkVersions(t, c, chunkSet)
	if got := versions[enc]; len(got) != 2 || !hasVersion(got, "vref") {
		t.Fatalf("referenced chunks touched: %v", got)
	}
}

// TestChunkedTTLLifecycle covers finite and non-expiring chunked values,
// chunk TTL grace, head expiry, and expiring/non-expiring overwrite
// transitions, asserting server-side expiration values directly.
func TestChunkedTTLLifecycle(t *testing.T) {
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024))
	as := s.(*aerospikeStore)
	c := rawClient(t, s)
	headExp := func(key string) uint32 {
		hk, _ := aero.NewKey("test", as.cfg.dataSet, encodeKey("micro", key))
		rec, err := c.Get(nil, hk)
		if err != nil || rec == nil {
			t.Fatalf("head get %s: %v", key, err)
		}
		return rec.Expiration
	}
	chunkExp := func(key, version string) uint32 {
		ck, _ := aero.NewKey("test", as.cfg.chunkSet, chunkKey(encodeKey("micro", key), version, 0))
		rec, err := c.Get(nil, ck)
		if err != nil || rec == nil {
			t.Fatalf("chunk get: %v", err)
		}
		return rec.Expiration
	}
	headVersionOf := func(key string) string {
		hk, _ := aero.NewKey("test", as.cfg.dataSet, encodeKey("micro", key))
		rec, err := c.Get(nil, hk, binChunkVersion)
		if err != nil {
			t.Fatal(err)
		}
		return rec.Bins[binChunkVersion].(string)
	}

	// Finite TTL chunked value: chunks live head TTL + grace.
	if err := s.Write(&store.Record{Key: "tchunk", Value: make([]byte, 4096)},
		store.WriteTTL(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	he := headExp("tchunk")
	if he == 0 || he > 30 {
		t.Fatalf("head expiration %d, want ~30", he)
	}
	ce := chunkExp("tchunk", headVersionOf("tchunk"))
	if ce < he+50 || ce > he+70 {
		t.Fatalf("chunk expiration %d, want ~head+60", ce)
	}
	// Record readable before expiry.
	if _, err := s.Read("tchunk"); err != nil {
		t.Fatal(err)
	}

	// Non-expiring chunked value: head and chunks never expire.
	if err := s.Write(&store.Record{Key: "tperm", Value: make([]byte, 4096)}); err != nil {
		t.Fatal(err)
	}
	if e := headExp("tperm"); e != aero.TTLDontExpire {
		t.Fatalf("non-expiring head expiration %d", e)
	}
	if e := chunkExp("tperm", headVersionOf("tperm")); e != aero.TTLDontExpire {
		t.Fatalf("non-expiring chunk expiration %d", e)
	}

	// Transitions: expiring -> non-expiring and back.
	if err := s.Write(&store.Record{Key: "ttr", Value: make([]byte, 4096)},
		store.WriteTTL(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(&store.Record{Key: "ttr", Value: make([]byte, 2048)}); err != nil {
		t.Fatal(err)
	}
	if e := headExp("ttr"); e != aero.TTLDontExpire {
		t.Fatalf("expiring->non-expiring transition failed: %d", e)
	}
	if err := s.Write(&store.Record{Key: "ttr", Value: make([]byte, 4096)},
		store.WriteTTL(45*time.Second)); err != nil {
		t.Fatal(err)
	}
	if e := headExp("ttr"); e == 0 || e > 45 {
		t.Fatalf("non-expiring->expiring transition failed: %d", e)
	}
}

// TestHeadExpiryReclaimsChunks: after a finite-TTL head expires, its
// chunks survive only for the grace window and are then reclaimable.
func TestHeadExpiryReclaimsChunks(t *testing.T) {
	chunkSet := fmt.Sprintf("he%d", time.Now().UnixNano())
	s := newTestStore(t, WithInlineThreshold(1024), WithChunkSize(1024),
		WithChunkSet(chunkSet), WithSweepGrace(0))
	as := s.(*aerospikeStore)
	c := rawClient(t, s)
	if err := s.Write(&store.Record{Key: "he", Value: make([]byte, 2048)},
		store.WriteTTL(1500*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	enc := encodeKey("micro", "he")
	// Bounded poll for head expiry (nsup-period is 1s).
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := s.Read("he"); errors.Is(err, store.ErrNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("head did not expire")
		}
		time.Sleep(250 * time.Millisecond)
	}
	// Orphaned chunks are reclaimed by sweep.
	n, err := as.Sweep()
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("expected 2 expired-head orphans swept, got %d", n)
	}
	if got := chunkVersions(t, c, chunkSet)[enc]; len(got) != 0 {
		t.Fatalf("chunks remain: %v", got)
	}
}

// TestNonExpiryAgainstNamespaceDefaultTTL proves explicit non-expiry beats a
// nonzero namespace default-ttl (the fixture's "test" namespace sets
// default-ttl 30s; a record that did not send TTLDontExpire would be
// assigned ~30s).
func TestNonExpiryAgainstNamespaceDefaultTTL(t *testing.T) {
	s := newTestStore(t)
	as := s.(*aerospikeStore)
	c := rawClient(t, s)
	if err := s.Write(&store.Record{Key: "persist3", Value: []byte("v")},
		store.WriteTo("test", "t")); err != nil {
		t.Fatal(err)
	}
	hk, _ := aero.NewKey("test", as.cfg.dataSet, encodeKey("t", "persist3"))
	rec, err := c.Get(nil, hk)
	if err != nil || rec == nil {
		t.Fatalf("head get: %v", err)
	}
	if rec.Expiration != aero.TTLDontExpire {
		t.Fatalf("expected never-expire against 30s namespace default, got %d", rec.Expiration)
	}
	r, rerr := s.Read("persist3", store.ReadFrom("test", "t"))
	if rerr != nil {
		t.Fatal(rerr)
	}
	if r[0].Expiry != 0 {
		t.Fatalf("expected no reported expiry, got %v", r[0].Expiry)
	}
}

// --- Lifecycle (approved guarantees from 1.7) ---

// TestCloseReleasesClient asserts the client is released on Close.
func TestCloseReleasesClient(t *testing.T) {
	s := newTestStore(t)
	as := s.(*aerospikeStore)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if as.client != nil {
		t.Fatal("client not released on Close")
	}
}

// TestRepeatedInitClosesClient asserts the approved repeated-Init guarantee:
// a second Init must not leak the previous client.
//
// REPORTED DEFECT: Init assigns s.client without closing the existing client
// (store.go:110). This regression is preserved failing until the production
// fix lands.
func TestRepeatedInitClosesClient(t *testing.T) {
	s := newTestStore(t)
	as := s.(*aerospikeStore)
	old := as.client
	if err := s.Init(); err != nil {
		t.Fatalf("re-Init: %v", err)
	}
	if old.IsConnected() {
		t.Fatal("previous client not closed on repeated Init (leaked)")
	}
}
