package aerospike

import (
	"crypto/tls"
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	aero "github.com/aerospike/aerospike-client-go/v8"
	"go-micro.dev/v6/store"
)

func newRec(key string, val []byte) *store.Record {
	return &store.Record{Key: key, Value: val}
}

func TestEncodeKeyCollisionFree(t *testing.T) {
	pairs := [][2]string{
		{"a", "bc"},
		{"ab", "c"},
		{"", ""},
		{"table", "key:with:colons"},
		{"t/able", "key with spaces"},
	}
	seen := map[string][2]string{}
	for _, p := range pairs {
		ek := encodeKey(p[0], p[1])
		if prev, dup := seen[ek]; dup {
			t.Fatalf("collision: %v and %v both encode to %q", prev, p, ek)
		}
		seen[ek] = p
	}
}

func TestEncodeKeySpecialCharacters(t *testing.T) {
	table := "agent_sessions"
	key := `runs/1: weïrd 🔑 key`
	ek := encodeKey(table, key)
	if !strings.Contains(ek, table+":"+key) {
		t.Fatalf("encoded key %q does not contain raw key", ek)
	}
	if !strings.HasPrefix(ek, "14:") {
		t.Fatalf("expected length prefix 14, got %q", ek)
	}
}

func TestEncodeKeyLongTable(t *testing.T) {
	table := strings.Repeat("a", 64) // sha256 hex session table names
	ek := encodeKey(table, "k")
	if !strings.HasPrefix(ek, "64:") {
		t.Fatalf("unexpected prefix: %q", ek[:4])
	}
}

// parseChunkKey splits a chunk user key into its encoded head key, version and
// index. The encoded head key itself contains colons but never "|". Test-only:
// production code maps chunks to heads via the ek/cv bins, not the user key.
func parseChunkKey(ck string) (encoded, version string, index int, ok bool) {
	i := strings.LastIndex(ck, "|")
	if i < 0 {
		return "", "", 0, false
	}
	j := strings.LastIndex(ck[:i], "|")
	if j < 0 {
		return "", "", 0, false
	}
	n, err := strconv.Atoi(ck[i+1:])
	if err != nil {
		return "", "", 0, false
	}
	return ck[:j], ck[j+1 : i], n, true
}

func TestChunkKeyRoundTrip(t *testing.T) {
	encoded := encodeKey("some:weird|table", "key|with|pipes")
	ck := chunkKey(encoded, "abcdef0123", 7)
	gotEnc, gotVer, gotIdx, ok := parseChunkKey(ck)
	if !ok || gotEnc != encoded || gotVer != "abcdef0123" || gotIdx != 7 {
		t.Fatalf("parse failed: %q %q %d %v", gotEnc, gotVer, gotIdx, ok)
	}
	if _, _, _, ok := parseChunkKey("notachunkkey"); ok {
		t.Fatal("parsed non-chunk key")
	}
}

func TestEnforceLimits(t *testing.T) {
	s := []string{"a", "b", "c", "d"}
	cases := []struct {
		name          string
		offset, limit uint
		want          []string
	}{
		{"plain", 0, 0, []string{"a", "b", "c", "d"}},
		{"offset+limit", 1, 3, []string{"b", "c", "d"}},
		{"small limit", 1, 1, []string{"b"}},
		{"offset=length", 4, 0, []string{}},
		{"offset>length", 222, 0, []string{}},
		{"limit>length", 0, 36, []string{"a", "b", "c", "d"}},
	}
	for _, tc := range cases {
		got := enforceLimits(s, tc.limit, tc.offset)
		if len(got) != len(tc.want) {
			t.Fatalf("%s: got %v, want %v", tc.name, got, tc.want)
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Fatalf("%s: got %v, want %v", tc.name, got, tc.want)
			}
		}
	}
}

func TestResolveTTL(t *testing.T) {
	cases := []struct {
		in   time.Duration
		want uint32
	}{
		{0, 4294967295}, // TTLDontExpire
		{-time.Second, 4294967295},
		{time.Nanosecond, 1}, // minimum 1 second, rounds up
		{1500 * time.Millisecond, 2},
		{10 * time.Second, 10},
	}
	for _, c := range cases {
		if got := resolveTTL(c.in); got != c.want {
			t.Fatalf("resolveTTL(%v) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestValidateConfig(t *testing.T) {
	c := defaultConfig()
	if err := validateConfig(c); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}

	c.dataSet = ""
	if err := validateConfig(c); err == nil || !strings.Contains(err.Error(), "data set") {
		t.Fatalf("expected data set error, got %v", err)
	}
	c = defaultConfig()
	c.chunkSet = string(make([]byte, 70))
	if err := validateConfig(c); err == nil || !strings.Contains(err.Error(), "chunk set") {
		t.Fatalf("expected chunk set error, got %v", err)
	}
	c = defaultConfig()
	c.chunkSize = 2 * 1024 * 1024
	if err := validateConfig(c); err == nil || !strings.Contains(err.Error(), "chunk size") {
		t.Fatalf("expected chunk size error, got %v", err)
	}
	c = defaultConfig()
	c.maxValueSize = c.inlineThresh - 1
	if err := validateConfig(c); err == nil || !strings.Contains(err.Error(), "total value cap") {
		t.Fatalf("expected value cap error, got %v", err)
	}
}

func TestInitValidationErrors(t *testing.T) {
	// Config validation runs before connecting, so these fail fast with no server.
	s := NewStore(WithDataSet(strings.Repeat("x", 64)))
	if err := s.Init(); err == nil || !strings.Contains(err.Error(), "data set") {
		t.Fatalf("expected data set validation error, got %v", err)
	}
	s = NewStore(WithChunkSize(8 * 1024 * 1024))
	if err := s.Init(); err == nil || !strings.Contains(err.Error(), "chunk size") {
		t.Fatalf("expected chunk size validation error, got %v", err)
	}
}

func TestValidateSizes(t *testing.T) {
	s := &aerospikeStore{cfg: defaultConfig()}
	rec := newRec("k", []byte("v"))

	if err := s.validateSizes(s.cfg, "t", rec); err != nil {
		t.Fatal(err)
	}
	rec.Key = strings.Repeat("k", 2048)
	if err := s.validateSizes(s.cfg, "t", rec); err == nil {
		t.Fatal("expected ErrKeyTooLarge")
	}
	rec = newRec("k", []byte("v"))
	rec.Metadata = map[string]interface{}{"big": strings.Repeat("m", 70*1024)}
	if err := s.validateSizes(s.cfg, "t", rec); err == nil {
		t.Fatal("expected ErrMetadataTooLarge")
	}
	rec = newRec("k", make([]byte, 17*1024*1024))
	if err := s.validateSizes(s.cfg, "t", rec); err == nil {
		t.Fatal("expected ErrValueTooLarge")
	}
	rec = newRec("k", make([]byte, 600*1024))
	s.cfg.chunking = false
	if err := s.validateSizes(s.cfg, "t", rec); err == nil {
		t.Fatal("expected ErrValueTooLarge when chunking disabled")
	}
	if err := s.validateSizes(s.cfg, strings.Repeat("t", 300), newRec("k", nil)); err == nil {
		t.Fatal("expected ErrTableTooLarge")
	}
}

func TestUnreachableCluster(t *testing.T) {
	s := NewStore(store.Nodes("127.0.0.1:1"))
	if err := s.Init(); err == nil {
		s.Close()
		t.Fatal("expected connect error")
	}
}

func TestCloseBehaviour(t *testing.T) {
	s := NewStore()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Read("k"); err == nil {
		t.Fatal("expected error after Close")
	}
}

func TestVerifyEnforceLimits(t *testing.T) {
	got := enforceLimits([]string{"a", "b", "c"}, math.MaxUint, 3)
	if len(got) != 0 {
		t.Fatalf("expected empty, got %v", got)
	}
}

func TestVerifyResolveTTL(t *testing.T) {
	if got := resolveTTL(time.Duration(math.MaxUint32+5) * time.Second); got != math.MaxUint32 {
		t.Fatalf("expected TTLDontExpire, got %d", got)
	}
}

func TestVerifyPastExpiry(t *testing.T) {
	if got := writeTTL(&store.Record{}, &store.WriteOptions{Expiry: time.Now().Add(-time.Hour)}); got != 1 {
		t.Fatalf("expected 1s TTL, got %d", got)
	}
}

// --- Section 2: serverless configuration and boundary tests ---

// TestOptionPropagation verifies every typed option lands in the resolved
// config, including the auth/TLS/client-policy options.
func TestOptionPropagation(t *testing.T) {
	pol := &aero.ClientPolicy{}
	tlsc := &tls.Config{ServerName: "db.internal"}
	s := NewStore(
		WithDataSet("d"), WithChunkSet("c"),
		WithMaxKeySize(8), WithMaxTableSize(9), WithMaxMetadataSize(10),
		WithInlineThreshold(64), WithChunkSize(32), WithMaxValueSize(4096),
		WithChunking(false), WithMaxListKeys(7), WithCreateIndex(false),
		WithAuth("u", "p"), WithTLS(tlsc), WithClientPolicy(pol),
		WithReaderRetries(5), WithSweepGrace(3*time.Second),
	)
	c := s.(*aerospikeStore).cfg
	if c.dataSet != "d" || c.chunkSet != "c" || c.maxKeySize != 8 ||
		c.maxTableSize != 9 || c.maxMetadata != 10 || c.inlineThresh != 64 ||
		c.chunkSize != 32 || c.maxValueSize != 4096 || c.chunking ||
		c.maxListKeys != 7 || c.createIndex || c.readerRetries != 5 ||
		c.sweepGrace != 3*time.Second {
		t.Fatalf("options not propagated: %+v", c)
	}
	if c.auth == nil || c.auth.user != "u" || c.auth.password != "p" {
		t.Fatalf("auth not propagated: %+v", c.auth)
	}
	if c.tlsConfig != tlsc {
		t.Fatal("TLS config not propagated")
	}
	if c.clientPolicy != pol {
		t.Fatal("client policy not propagated")
	}
}

// TestAbsentOptionsPreserveDefaults verifies absent typed options leave the
// default/base configuration untouched.
func TestAbsentOptionsPreserveDefaults(t *testing.T) {
	s := NewStore(WithClientPolicy(&aero.ClientPolicy{User: "base-user"}))
	c := s.(*aerospikeStore).cfg
	if c.auth != nil {
		t.Fatalf("auth should be absent, got %+v", c.auth)
	}
	if c.tlsConfig != nil {
		t.Fatal("tlsConfig should be absent")
	}
	if c.clientPolicy.User != "base-user" {
		t.Fatal("base policy fields not preserved")
	}
}

// TestCallerPolicyNotMutated asserts the approved precedence contract: the
// store must not write auth/TLS onto a caller-supplied ClientPolicy.
// REPORTED DEFECT: Init assigns User/Password/TlsConfig directly onto the
// caller's struct (store.go:84-94). This regression is preserved failing
// until the production fix lands.
func TestCallerPolicyNotMutated(t *testing.T) {
	pol := &aero.ClientPolicy{}
	s := NewStore(
		store.Nodes("127.0.0.1:1"), // unreachable: Init fails after policy wiring
		WithClientPolicy(pol),
		WithAuth("secret-user", "secret-pass"),
		WithTLS(&tls.Config{}),
	)
	_ = s.Init()
	if pol.User != "" || pol.Password != "" || pol.TlsConfig != nil {
		t.Fatalf("caller's ClientPolicy was mutated: user=%q tls=%v", pol.User, pol.TlsConfig != nil)
	}
}

// TestParseHostsForms covers documented seed forms and malformed addresses.
func TestParseHostsForms(t *testing.T) {
	cases := []struct {
		name  string
		nodes []string
		want  [][2]interface{} // host, port
		err   bool
	}{
		{"default", nil, [][2]interface{}{{"127.0.0.1", 3000}}, false},
		{"host:port", []string{"db:4000"}, [][2]interface{}{{"db", 4000}}, false},
		{"bare host", []string{"db"}, [][2]interface{}{{"db", 3000}}, false},
		{"multiple", []string{"a:1", "b:2", "c"}, [][2]interface{}{{"a", 1}, {"b", 2}, {"c", 3000}}, false},
		{"bad port", []string{"db:abc"}, nil, true},
		{"empty port", []string{"db:"}, nil, true},
		{"extra colons", []string{"a:b:c"}, nil, true},
	}
	for _, tc := range cases {
		hosts, err := parseHosts(tc.nodes)
		if tc.err {
			if err == nil {
				t.Fatalf("%s: expected error, got %v", tc.name, hosts)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if len(hosts) != len(tc.want) {
			t.Fatalf("%s: got %d hosts", tc.name, len(hosts))
		}
		for i, w := range tc.want {
			if hosts[i].Name != w[0] || hosts[i].Port != w[1] {
				t.Fatalf("%s host %d: got %v:%d, want %v", tc.name, i, hosts[i].Name, hosts[i].Port, w)
			}
		}
	}
}

// TestValidateConfigBranches covers every Init validation branch.
func TestValidateConfigBranches(t *testing.T) {
	mut := func(f func(*config)) config {
		c := defaultConfig()
		f(&c)
		return c
	}
	cases := []struct {
		name string
		cfg  config
		want string
	}{
		{"empty data set", mut(func(c *config) { c.dataSet = "" }), "data set"},
		{"data set at limit ok", mut(func(c *config) { c.dataSet = strings.Repeat("x", 63) }), ""},
		{"data set over limit", mut(func(c *config) { c.dataSet = strings.Repeat("x", 64) }), "data set"},
		{"empty chunk set", mut(func(c *config) { c.chunkSet = "" }), "chunk set"},
		{"zero maxKeySize", mut(func(c *config) { c.maxKeySize = 0 }), "positive"},
		{"zero maxListKeys", mut(func(c *config) { c.maxListKeys = 0 }), "positive"},
		{"zero inlineThresh", mut(func(c *config) { c.inlineThresh = 0 }), "inline threshold"},
		{"inlineThresh at 1MiB", mut(func(c *config) { c.inlineThresh = 1024 * 1024 }), "inline threshold"},
		{"zero chunkSize", mut(func(c *config) { c.chunkSize = 0 }), "chunk size"},
		{"chunkSize at 1MiB", mut(func(c *config) { c.chunkSize = 1024 * 1024 }), "chunk size"},
		{"cap below inline", mut(func(c *config) { c.maxValueSize = c.inlineThresh - 1 }), "total value cap"},
		{"cap equal inline ok", mut(func(c *config) { c.maxValueSize = c.inlineThresh }), ""},
	}
	for _, tc := range cases {
		err := validateConfig(tc.cfg)
		if tc.want == "" {
			if err != nil {
				t.Fatalf("%s: unexpected error %v", tc.name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: expected error containing %q, got %v", tc.name, tc.want, err)
		}
	}
}

// TestSizeBoundaries asserts acceptance exactly at each cap and the typed
// error one byte beyond it.
func TestSizeBoundaries(t *testing.T) {
	s := &aerospikeStore{cfg: defaultConfig()}
	c := s.cfg

	at := func(rec *store.Record, table string) error { return s.validateSizes(s.cfg, table, rec) }

	// Key cap.
	if err := at(&store.Record{Key: strings.Repeat("k", c.maxKeySize)}, "t"); err != nil {
		t.Fatalf("key at cap rejected: %v", err)
	}
	if err := at(&store.Record{Key: strings.Repeat("k", c.maxKeySize+1)}, "t"); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("key over cap: expected ErrKeyTooLarge, got %v", err)
	}
	// Table cap.
	if err := at(&store.Record{Key: "k"}, strings.Repeat("t", c.maxTableSize)); err != nil {
		t.Fatalf("table at cap rejected: %v", err)
	}
	if err := at(&store.Record{Key: "k"}, strings.Repeat("t", c.maxTableSize+1)); !errors.Is(err, ErrTableTooLarge) {
		t.Fatalf("table over cap: expected ErrTableTooLarge, got %v", err)
	}
	// Metadata cap: build a map that serialises just under and just over.
	under := c.maxMetadata - len(`{"m":""}`) // exact fit
	if err := at(&store.Record{Key: "k", Metadata: map[string]interface{}{"m": strings.Repeat("x", under)}}, "t"); err != nil {
		t.Fatalf("metadata at cap rejected: %v", err)
	}
	if err := at(&store.Record{Key: "k", Metadata: map[string]interface{}{"m": strings.Repeat("x", under+1)}}, "t"); !errors.Is(err, ErrMetadataTooLarge) {
		t.Fatalf("metadata over cap: expected ErrMetadataTooLarge, got %v", err)
	}
	// Total value cap.
	if err := at(&store.Record{Key: "k", Value: make([]byte, c.maxValueSize)}, "t"); err != nil {
		t.Fatalf("value at cap rejected: %v", err)
	}
	if err := at(&store.Record{Key: "k", Value: make([]byte, c.maxValueSize+1)}, "t"); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("value over cap: expected ErrValueTooLarge, got %v", err)
	}
	// Inline/chunk threshold boundary with chunking disabled: at threshold ok,
	// one byte over rejected.
	s.cfg.chunking = false
	if err := at(&store.Record{Key: "k", Value: make([]byte, c.inlineThresh)}, "t"); err != nil {
		t.Fatalf("value at inline threshold rejected: %v", err)
	}
	if err := at(&store.Record{Key: "k", Value: make([]byte, c.inlineThresh+1)}, "t"); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("value over inline threshold: expected ErrValueTooLarge, got %v", err)
	}
}

// TestMetadataSerializationFailure covers unmarshalable metadata.
func TestMetadataSerializationFailure(t *testing.T) {
	s := &aerospikeStore{cfg: defaultConfig()}
	rec := &store.Record{Key: "k", Metadata: map[string]interface{}{"bad": func() {}}}
	if err := s.validateSizes(s.cfg, "t", rec); err == nil || !strings.Contains(err.Error(), "serialise metadata") {
		t.Fatalf("expected metadata serialisation error, got %v", err)
	}
}

// TestClosedStoreOperations asserts every data operation fails with ErrClosed
// after Close, and that Close is idempotent (approved lifecycle guarantee).
func TestClosedStoreOperations(t *testing.T) {
	s := NewStore()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close should succeed (idempotent): %v", err)
	}
	if _, err := s.Read("k"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Read after Close: expected ErrClosed, got %v", err)
	}
	if err := s.Write(newRec("k", []byte("v"))); !errors.Is(err, ErrClosed) {
		t.Fatalf("Write after Close: expected ErrClosed, got %v", err)
	}
	if err := s.Delete("k"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Delete after Close: expected ErrClosed, got %v", err)
	}
	if _, err := s.List(); !errors.Is(err, ErrClosed) {
		t.Fatalf("List after Close: expected ErrClosed, got %v", err)
	}
}
