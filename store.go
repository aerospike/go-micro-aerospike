// Package aerospike is a Go Micro store.Store plugin backed by Aerospike.
//
// Database maps to an Aerospike namespace (which must already exist), Table is
// stored in bin "t" of every record in one configurable data set and served by
// a string secondary index, and each record's user key is a length-prefixed
// encoding of (table, key). Values larger than the inline threshold are split
// into versioned chunk records in a separate chunk set, with the head record
// as the commit point.
package aerospike

import (
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	aero "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"go-micro.dev/v6/store"
)

// maxSetNameLen is Aerospike's set name limit in bytes.
const maxSetNameLen = 63

// chunkTTLGrace is added to a head's TTL so chunks never expire before it.
const chunkTTLGrace = 60 // seconds

// indexReadyTimeout bounds how long Init waits for the secondary index.
const indexReadyTimeout = 30 * time.Second

type aerospikeStore struct {
	initMu           sync.Mutex
	mu               sync.RWMutex
	opts             store.Options
	cfg              config
	client           *aero.Client
	namespaces       map[string]bool
	beforeChunkFetch func()
}

// Sweeper is implemented by the Aerospike store. NewStore returns the
// store.Store interface Go Micro expects; assert to Sweeper to run chunk
// maintenance:
//
//	if sw, ok := s.(aerospike.Sweeper); ok {
//		n, err := sw.Sweep()
//		...
//	}
type Sweeper interface {
	store.Store
	// Sweep deletes chunk records in a namespace whose version no head
	// references. It is explicit maintenance and never runs automatically.
	Sweep(opts ...store.Option) (int, error)
}

// NewStore creates an Aerospike-backed store.Store. Call Init to connect.
func NewStore(opts ...store.Option) store.Store {
	s := &aerospikeStore{
		opts: store.Options{
			Database: "test",
			Context:  nil,
		},
		namespaces: map[string]bool{},
	}
	for _, o := range opts {
		o(&s.opts)
	}
	s.cfg = loadConfig(s.opts)
	return s
}

// Init connects to the cluster, validates the configuration and namespace, and
// ensures the secondary index exists (unless disabled). Re-Init reconnects and
// resets the per-cluster namespace and index caches.
func (s *aerospikeStore) Init(opts ...store.Option) error {
	s.initMu.Lock()
	defer s.initMu.Unlock()
	s.mu.Lock()
	for _, o := range opts {
		o(&s.opts)
	}
	s.cfg = loadConfig(s.opts)
	cfg := s.cfg
	so := s.opts
	s.mu.Unlock()

	if err := validateConfig(cfg); err != nil {
		return err
	}
	if so.Database == "" {
		return fmt.Errorf("aerospike: no namespace configured (store Database option)")
	}

	policy := cfg.clientPolicy
	if policy == nil {
		policy = aero.NewClientPolicy()
	} else {
		// Copy: auth and TLS overrides apply to the effective policy without
		// mutating the caller's ClientPolicy.
		p := *policy
		policy = &p
	}
	if cfg.auth != nil {
		policy.User = cfg.auth.user
		policy.Password = cfg.auth.password
	}
	if cfg.tlsSet {
		policy.TlsConfig = cfg.tlsConfig
	}

	hosts, err := parseHosts(so.Nodes)
	if err != nil {
		return err
	}

	client, err := aero.NewClientWithPolicyAndHost(policy, hosts...)
	if err != nil {
		return fmt.Errorf("aerospike: connect: %w", err)
	}
	s.mu.Lock()
	old := s.client
	s.client = client
	s.namespaces = map[string]bool{}
	s.mu.Unlock()
	fail := func(err error) error {
		s.mu.Lock()
		s.client = old
		s.mu.Unlock()
		client.Close()
		return err
	}

	if err := s.checkNamespace(client, so.Database); err != nil {
		return fail(err)
	}

	if cfg.createIndex {
		if err := s.ensureIndex(client, cfg, so.Database); err != nil {
			return fail(err)
		}
	}
	if old != nil {
		old.Close()
	}
	return nil
}

func validateConfig(c config) error {
	for name, set := range map[string]string{"data set": c.dataSet, "chunk set": c.chunkSet} {
		if set == "" || len(set) > maxSetNameLen {
			return fmt.Errorf("aerospike: %s name %q is empty or exceeds the %d-byte set name limit", name, set, maxSetNameLen)
		}
	}
	if c.maxKeySize <= 0 || c.maxTableSize <= 0 || c.maxMetadata <= 0 || c.maxListKeys <= 0 {
		return fmt.Errorf("aerospike: size caps and MaxListKeys must be positive")
	}
	if c.inlineThresh <= 0 || c.inlineThresh >= 1024*1024 {
		return fmt.Errorf("aerospike: inline threshold %d must be positive and below the 1 MiB record limit", c.inlineThresh)
	}
	if c.chunkSize <= 0 || c.chunkSize >= 1024*1024 {
		return fmt.Errorf("aerospike: chunk size %d must be positive and below the 1 MiB record limit", c.chunkSize)
	}
	if c.maxValueSize < c.inlineThresh {
		return fmt.Errorf("aerospike: total value cap %d is smaller than the inline threshold %d", c.maxValueSize, c.inlineThresh)
	}
	return nil
}

func parseHosts(nodes []string) ([]*aero.Host, error) {
	if len(nodes) == 0 {
		nodes = []string{"127.0.0.1:3000"}
	}
	hosts := make([]*aero.Host, 0, len(nodes))
	for _, n := range nodes {
		host, port := n, 3000
		if i := strings.LastIndex(n, ":"); i >= 0 {
			host = n[:i]
			p, err := strconv.Atoi(n[i+1:])
			if err != nil {
				return nil, fmt.Errorf("aerospike: bad node address %q", n)
			}
			port = p
		}
		hosts = append(hosts, aero.NewHost(host, port))
	}
	return hosts, nil
}

// Options returns the store's current options.
func (s *aerospikeStore) Options() store.Options {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.opts
}

// Close releases the Aerospike client. It must not run concurrently with
// Read, Write, Delete, List or Sweep: in-flight operations holding the client
// fail with closed-client errors. Calls made after Close returns ErrClosed.
func (s *aerospikeStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client == nil {
		return nil
	}
	s.client.Close()
	s.client = nil
	return nil
}

func (s *aerospikeStore) String() string { return "aerospike" }

func (s *aerospikeStore) getClient() (*aero.Client, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.client == nil {
		return nil, ErrClosed
	}
	return s.client, nil
}

// getState returns the client together with a consistent snapshot of the
// store's options and resolved config under a single read lock, so a
// concurrent Init or Sweep cannot be observed mid-mutation. The lock protects
// acquisition only — a concurrent Close may still close the client out from
// under an operation, which surfaces as closed-client errors, not a panic.
func (s *aerospikeStore) getState() (*aero.Client, store.Options, config, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.client == nil {
		return nil, store.Options{}, config{}, ErrClosed
	}
	return s.client, s.opts, s.cfg, nil
}

// checkNamespace verifies a namespace exists, caching the result.
func (s *aerospikeStore) checkNamespace(client *aero.Client, ns string) error {
	s.mu.RLock()
	known, ok := s.namespaces[ns]
	s.mu.RUnlock()
	if ok && known {
		return nil
	}

	nodes := client.GetNodes()
	if len(nodes) == 0 {
		return fmt.Errorf("aerospike: no cluster nodes available")
	}
	res, err := nodes[0].RequestInfo(&aero.InfoPolicy{}, "namespaces")
	if err != nil {
		return fmt.Errorf("aerospike: namespace check: %w", err)
	}
	exists := false
	for _, n := range strings.Split(res["namespaces"], ";") {
		if n == ns {
			exists = true
			break
		}
	}
	s.mu.Lock()
	s.namespaces[ns] = exists
	s.mu.Unlock()
	if !exists {
		return fmt.Errorf("aerospike: namespace %q does not exist; create it in the server configuration", ns)
	}
	return nil
}

// ensureIndex creates the secondary index on bin t if absent and waits for it
// to be built.
func (s *aerospikeStore) ensureIndex(client *aero.Client, cfg config, ns string) error {
	nodes := client.GetNodes()
	if len(nodes) == 0 {
		return fmt.Errorf("aerospike: no cluster nodes available")
	}
	res, err := nodes[0].RequestInfo(&aero.InfoPolicy{}, "sindex/"+ns)
	if err == nil {
		for _, entry := range strings.Fields(res["sindex/"+ns]) {
			if strings.Contains(entry, "indexname="+indexName) {
				return s.waitIndexPopulated(client, cfg, ns)
			}
		}
	}

	task, ierr := client.CreateIndex(nil, ns, cfg.dataSet, indexName, binTable, aero.STRING)
	if ierr != nil {
		if ierr.Matches(types.INDEX_FOUND) {
			return s.waitIndexPopulated(client, cfg, ns)
		}
		return &MissingIndexError{Namespace: ns, Set: cfg.dataSet, Bin: binTable, Index: indexName, Cause: ierr}
	}

	deadline := time.Now().Add(indexReadyTimeout)
	for {
		done, derr := task.IsDone()
		if derr != nil {
			return fmt.Errorf("aerospike: index status: %w", derr)
		}
		if done {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("aerospike: timed out waiting for index %s", indexName)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// waitIndexPopulated polls the set's index_populating flag until the
// secondary index has finished building.
func (s *aerospikeStore) waitIndexPopulated(client *aero.Client, cfg config, ns string) error {
	deadline := time.Now().Add(indexReadyTimeout)
	for {
		nodes := client.GetNodes()
		if len(nodes) == 0 {
			return fmt.Errorf("aerospike: no cluster nodes available")
		}
		res, err := nodes[0].RequestInfo(&aero.InfoPolicy{}, "sets/"+ns)
		if err != nil {
			return fmt.Errorf("aerospike: index status: %w", err)
		}
		for _, entry := range strings.Split(res["sets/"+ns], ";") {
			if strings.Contains(entry, "set="+cfg.dataSet) {
				if strings.Contains(entry, "index_populating=false") {
					return nil
				}
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("aerospike: timed out waiting for index %s to finish building", indexName)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// resolve returns the namespace and table for a call, applying store defaults
// and validating the namespace.
func (s *aerospikeStore) resolve(client *aero.Client, opts store.Options, cfg config, db, table string) (string, string, error) {
	if db == "" {
		db = opts.Database
	}
	if table == "" {
		table = opts.Table
	}
	if table == "" {
		table = defaultTable
	}
	if err := s.checkNamespace(client, db); err != nil {
		return "", "", err
	}
	if cfg.createIndex {
		if err := s.ensureIndexOnce(client, cfg, db); err != nil {
			return "", "", err
		}
	}
	return db, table, nil
}

// ensureIndexOnce ensures the table index exists in a namespace, once per
// namespace per store instance.
func (s *aerospikeStore) ensureIndexOnce(client *aero.Client, cfg config, ns string) error {
	s.mu.RLock()
	done := s.namespaces["idx:"+ns]
	s.mu.RUnlock()
	if done {
		return nil
	}
	if err := s.ensureIndex(client, cfg, ns); err != nil {
		return err
	}
	s.mu.Lock()
	s.namespaces["idx:"+ns] = true
	s.mu.Unlock()
	return nil
}

// resolveTTL maps a duration to an Aerospike record expiration: negative or
// zero means never expire, positive durations round up to whole seconds with a
// one-second minimum.
func resolveTTL(d time.Duration) uint32 {
	if d <= 0 {
		return aero.TTLDontExpire
	}
	if d >= time.Duration(math.MaxUint32)*time.Second {
		return aero.TTLDontExpire
	}
	return uint32((d + time.Second - 1) / time.Second)
}

// writeTTL resolves WriteTTL > WriteExpiry > Record.Expiry, defaulting to
// never expire so the namespace default-ttl cannot apply.
func writeTTL(rec *store.Record, wopts *store.WriteOptions) uint32 {
	switch {
	case wopts.TTL != 0:
		return resolveTTL(wopts.TTL)
	case !wopts.Expiry.IsZero():
		// An expiry already in the past means the record should expire
		// immediately (1 s TTL), not never expire.
		if d := time.Until(wopts.Expiry); d <= 0 {
			return 1
		} else {
			return resolveTTL(d)
		}
	case rec.Expiry != 0:
		return resolveTTL(rec.Expiry)
	default:
		return aero.TTLDontExpire
	}
}

// validateSizes enforces the key, table, metadata and total-value caps.
func (s *aerospikeStore) validateSizes(c config, table string, rec *store.Record) error {
	if len(rec.Key) > c.maxKeySize {
		return fmt.Errorf("%w: %d > %d bytes", ErrKeyTooLarge, len(rec.Key), c.maxKeySize)
	}
	if len(table) > c.maxTableSize {
		return fmt.Errorf("%w: %d > %d bytes", ErrTableTooLarge, len(table), c.maxTableSize)
	}
	if rec.Metadata != nil {
		b, err := json.Marshal(rec.Metadata)
		if err != nil {
			return fmt.Errorf("aerospike: serialise metadata: %w", err)
		}
		if len(b) > c.maxMetadata {
			return fmt.Errorf("%w: %d > %d bytes", ErrMetadataTooLarge, len(b), c.maxMetadata)
		}
	}
	if len(rec.Value) > c.maxValueSize {
		return fmt.Errorf("%w: %d > %d bytes", ErrValueTooLarge, len(rec.Value), c.maxValueSize)
	}
	if len(rec.Value) > c.inlineThresh && !c.chunking {
		return fmt.Errorf("%w: %d > %d bytes and chunking is disabled", ErrValueTooLarge, len(rec.Value), c.inlineThresh)
	}
	return nil
}

func writePolicy(ttl uint32) *aero.WritePolicy {
	p := aero.NewWritePolicy(0, ttl)
	p.RecordExistsAction = aero.REPLACE
	return p
}

// Write persists a record, chunking values above the inline threshold.
func (s *aerospikeStore) Write(rec *store.Record, opts ...store.WriteOption) error {
	client, so, cfg, err := s.getState()
	if err != nil {
		return err
	}
	wopts := store.WriteOptions{}
	for _, o := range opts {
		o(&wopts)
	}
	ns, table, err := s.resolve(client, so, cfg, wopts.Database, wopts.Table)
	if err != nil {
		return err
	}
	if err := s.validateSizes(cfg, table, rec); err != nil {
		return err
	}

	ttl := writeTTL(rec, &wopts)
	encoded := encodeKey(table, rec.Key)
	head, _ := aero.NewKey(ns, cfg.dataSet, encoded)

	if len(rec.Value) <= cfg.inlineThresh {
		oldVersion, oldCount := s.chunkRef(client, head)
		bins := []*aero.Bin{
			aero.NewBin(binTable, table),
			aero.NewBin(binKey, rec.Key),
			aero.NewBin(binValue, rec.Value),
		}
		if rec.Metadata != nil {
			bins = append(bins, aero.NewBin(binMeta, rec.Metadata))
		}
		if err := client.PutBins(writePolicy(ttl), head, bins...); err != nil {
			return err
		}
		if oldVersion != "" && oldCount > 0 {
			s.deleteChunks(client, cfg, ns, encoded, oldVersion, oldCount)
		}
		return nil
	}
	return s.writeChunked(client, cfg, ns, table, encoded, head, rec, ttl)
}

// chunkRef snapshots the previous head's chunk version and count so the old
// version's chunks can be deleted after a commit. Bookkeeping only — read
// failures simply skip cleanup.
func (s *aerospikeStore) chunkRef(client *aero.Client, head *aero.Key) (version string, count int) {
	old, err := client.Get(nil, head, binChunkVersion, binChunkCount)
	if err == nil && old != nil {
		version, _ = old.Bins[binChunkVersion].(string)
		count = toInt(old.Bins[binChunkCount])
	}
	return version, count
}

// writeChunked writes all chunks of a new version, commits the head, then
// best-effort deletes the previous version's chunks.
func (s *aerospikeStore) writeChunked(client *aero.Client, c config, ns, table, encoded string, head *aero.Key, rec *store.Record, ttl uint32) error {
	var vb [16]byte
	if _, err := crand.Read(vb[:]); err != nil {
		return err
	}
	version := hex.EncodeToString(vb[:])
	nchunks := (len(rec.Value) + c.chunkSize - 1) / c.chunkSize

	// Snapshot the previous head's version so its chunks can be cleaned up
	// after the commit. This read is bookkeeping only.
	oldVersion, oldCount := s.chunkRef(client, head)

	chunkTTL := ttl
	if ttl != aero.TTLDontExpire {
		if ttl > math.MaxUint32-chunkTTLGrace {
			chunkTTL = aero.TTLDontExpire
		} else {
			chunkTTL = ttl + chunkTTLGrace
		}
	}
	records := make([]aero.BatchRecordIfc, 0, nchunks)
	bwp := aero.NewBatchWritePolicy()
	bwp.RecordExistsAction = aero.REPLACE
	bwp.Expiration = chunkTTL
	for i := 0; i < nchunks; i++ {
		end := (i + 1) * c.chunkSize
		if end > len(rec.Value) {
			end = len(rec.Value)
		}
		ck, _ := aero.NewKey(ns, c.chunkSet, chunkKey(encoded, version, i))
		records = append(records, aero.NewBatchWrite(bwp, ck,
			aero.PutOp(aero.NewBin(binValue, rec.Value[i*c.chunkSize:end])),
			aero.PutOp(aero.NewBin(binChunkHead, encoded)),
			aero.PutOp(aero.NewBin(binChunkVer, version)),
		))
	}
	if err := client.BatchOperate(nil, records); err != nil {
		return fmt.Errorf("aerospike: write chunks: %w", err)
	}
	for _, r := range records {
		if r.BatchRec().Err != nil {
			return fmt.Errorf("aerospike: write chunk: %w", r.BatchRec().Err)
		}
	}

	bins := []*aero.Bin{
		aero.NewBin(binTable, table),
		aero.NewBin(binKey, rec.Key),
		aero.NewBin(binChunkCount, nchunks),
		aero.NewBin(binChunkVersion, version),
		aero.NewBin(binLayoutVersion, layoutVersion),
	}
	if rec.Metadata != nil {
		bins = append(bins, aero.NewBin(binMeta, rec.Metadata))
	}
	if err := client.PutBins(writePolicy(ttl), head, bins...); err != nil {
		return err
	}

	if oldVersion != "" && oldCount > 0 {
		s.deleteChunks(client, c, ns, encoded, oldVersion, oldCount)
	}
	return nil
}

func toInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case uint64:
		return int(n)
	default:
		return 0
	}
}

// deleteChunks removes a version's chunk records best-effort.
func (s *aerospikeStore) deleteChunks(client *aero.Client, cfg config, ns, encoded, version string, count int) {
	keys := make([]*aero.Key, count)
	for i := 0; i < count; i++ {
		keys[i], _ = aero.NewKey(ns, cfg.chunkSet, chunkKey(encoded, version, i))
	}
	_, _ = client.BatchDelete(nil, nil, keys)
}

// Read returns the record for an exact key, or matching records for
// prefix/suffix reads.
func (s *aerospikeStore) Read(key string, opts ...store.ReadOption) ([]*store.Record, error) {
	client, so, cfg, err := s.getState()
	if err != nil {
		return nil, err
	}
	ropts := store.ReadOptions{}
	for _, o := range opts {
		o(&ropts)
	}
	ns, table, err := s.resolve(client, so, cfg, ropts.Database, ropts.Table)
	if err != nil {
		return nil, err
	}

	if !ropts.Prefix && !ropts.Suffix {
		rec, err := s.readHead(client, cfg, ns, table, key)
		if err != nil {
			return nil, err
		}
		return []*store.Record{rec}, nil
	}

	prefix, suffix := "", ""
	if ropts.Prefix {
		prefix = key
	}
	if ropts.Suffix {
		suffix = key
	}
	keys, err := s.listKeys(client, cfg, ns, table, prefix, suffix)
	if err != nil {
		return nil, err
	}
	keys = enforceLimits(keys, ropts.Limit, ropts.Offset)

	heads := make([]*aero.Key, len(keys))
	for i, k := range keys {
		heads[i], _ = aero.NewKey(ns, cfg.dataSet, encodeKey(table, k))
	}
	recs, err := client.BatchGet(nil, heads)
	if err != nil {
		return nil, err
	}
	out := make([]*store.Record, 0, len(recs))
	for _, r := range recs {
		if r == nil {
			continue
		}
		rec, err := s.recordFromHead(client, cfg, ns, r)
		if err != nil {
			if errors.Is(err, errChunkMissing) {
				// A writer may have committed a new version between the key
				// query and this chunk fetch; re-read the head with bounded
				// retries as for exact-key reads. If the head is gone the
				// record disappeared mid-selection and is simply omitted.
				rec, err = s.readHeadRecord(client, cfg, ns, r.Key)
				if errors.Is(err, store.ErrNotFound) {
					continue
				}
			}
			if err != nil {
				return nil, err
			}
		}
		out = append(out, rec)
	}
	return out, nil
}

// errChunkMissing is an internal sentinel: a head referenced a chunk that is
// absent, which may mean a writer committed a new version mid-read.
var errChunkMissing = errors.New("chunk missing")

// readHead resolves the record for (table, key); see readHeadRecord.
func (s *aerospikeStore) readHead(client *aero.Client, cfg config, ns, table, key string) (*store.Record, error) {
	head, _ := aero.NewKey(ns, cfg.dataSet, encodeKey(table, key))
	return s.readHeadRecord(client, cfg, ns, head)
}

// readHeadRecord fetches a head record and resolves chunked values. When a
// chunk is missing it re-reads the head and retries against the head's
// current state — bounded by readerRetries — so value, metadata and expiry
// always come from the same committed head. Only if the head is unchanged
// and chunks are still missing does it return corruption.
func (s *aerospikeStore) readHeadRecord(client *aero.Client, cfg config, ns string, head *aero.Key) (*store.Record, error) {
	var lastVersion string
	var lastCount int
	for attempt := 0; ; attempt++ {
		rec, err := client.Get(nil, head)
		if err != nil {
			if e, ok := err.(aero.Error); ok && e.Matches(types.KEY_NOT_FOUND_ERROR) {
				return nil, store.ErrNotFound
			}
			return nil, err
		}
		if rec == nil {
			return nil, store.ErrNotFound
		}
		out, rerr := s.recordFromHead(client, cfg, ns, rec)
		if rerr == nil {
			return out, nil
		}
		if !errors.Is(rerr, errChunkMissing) {
			return nil, rerr
		}
		v, _ := rec.Bins[binChunkVersion].(string)
		c := toInt(rec.Bins[binChunkCount])
		if attempt == cfg.readerRetries || (v == lastVersion && c == lastCount) {
			return nil, fmt.Errorf("%w: version %s, %d chunks", ErrCorruption, v, c)
		}
		lastVersion, lastCount = v, c
	}
}

// recordFromHead converts an Aerospike head record into a store.Record,
// fetching chunks when the head references them.
func (s *aerospikeStore) recordFromHead(client *aero.Client, cfg config, ns string, rec *aero.Record) (*store.Record, error) {
	out := &store.Record{}
	if k, ok := rec.Bins[binKey].(string); ok {
		out.Key = k
	}
	switch m := rec.Bins[binMeta].(type) {
	case map[string]interface{}:
		out.Metadata = m
	case map[interface{}]interface{}:
		out.Metadata = make(map[string]interface{}, len(m))
		for k, v := range m {
			if ks, ok := k.(string); ok {
				out.Metadata[ks] = v
			}
		}
	}
	if rec.Expiration != aero.TTLDontExpire && rec.Expiration != 0 {
		out.Expiry = time.Duration(rec.Expiration) * time.Second
	}

	lv := toInt(rec.Bins[binLayoutVersion])
	count := toInt(rec.Bins[binChunkCount])
	version, _ := rec.Bins[binChunkVersion].(string)

	if count == 0 {
		if v, ok := rec.Bins[binValue].([]byte); ok {
			out.Value = v
		}
		return out, nil
	}
	if lv != layoutVersion {
		return nil, fmt.Errorf("%w: %d", ErrUnknownLayout, lv)
	}

	value, err := s.fetchChunks(client, cfg, ns, rec.Key, version, count)
	if err != nil {
		return nil, err
	}
	out.Value = value
	return out, nil
}

// fetchChunks batch-fetches one version's chunks in a single attempt,
// returning errChunkMissing when any chunk is absent.
func (s *aerospikeStore) fetchChunks(client *aero.Client, cfg config, ns string, head *aero.Key, version string, count int) ([]byte, error) {
	if s.beforeChunkFetch != nil {
		s.beforeChunkFetch()
	}
	encoded := head.Value().GetObject().(string)
	keys := make([]*aero.Key, count)
	for i := 0; i < count; i++ {
		keys[i], _ = aero.NewKey(ns, cfg.chunkSet, chunkKey(encoded, version, i))
	}
	recs, err := client.BatchGet(nil, keys, binValue)
	if err != nil {
		return nil, err
	}
	var buf []byte
	for _, r := range recs {
		if r == nil || r.Bins == nil {
			return nil, errChunkMissing
		}
		v, ok := r.Bins[binValue].([]byte)
		if !ok {
			return nil, errChunkMissing
		}
		buf = append(buf, v...)
	}
	return buf, nil
}

// Delete removes the record's head, then its chunks best-effort.
func (s *aerospikeStore) Delete(key string, opts ...store.DeleteOption) error {
	client, so, cfg, err := s.getState()
	if err != nil {
		return err
	}
	dopts := store.DeleteOptions{}
	for _, o := range opts {
		o(&dopts)
	}
	ns, table, err := s.resolve(client, so, cfg, dopts.Database, dopts.Table)
	if err != nil {
		return err
	}

	head, _ := aero.NewKey(ns, cfg.dataSet, encodeKey(table, key))
	// Read the chunk version and delete the head in one atomic operation, so
	// cleanup targets the version that was committed at delete time — not a
	// stale snapshot a concurrent writer may have replaced.
	old, err := client.Operate(nil, head,
		aero.GetBinOp(binChunkVersion),
		aero.GetBinOp(binChunkCount),
		aero.DeleteOp(),
	)
	if err != nil {
		if e, ok := err.(aero.Error); ok && e.Matches(types.KEY_NOT_FOUND_ERROR) {
			return nil
		}
		return err
	}
	if old != nil {
		if v, ok := old.Bins[binChunkVersion].(string); ok {
			s.deleteChunks(client, cfg, ns, encodeKey(table, key), v, toInt(old.Bins[binChunkCount]))
		}
	}
	return nil
}

// listKeys runs the index query on t for one table with anchored, escaped
// prefix/suffix filter expressions on k, returning sorted keys.
func (s *aerospikeStore) listKeys(client *aero.Client, cfg config, ns, table, prefix, suffix string) ([]string, error) {
	stmt := aero.NewStatement(ns, cfg.dataSet, binKey)
	stmt.Filter = aero.NewEqualFilter(binTable, table)

	policy := aero.NewQueryPolicy()
	if expr := keyFilterExpr(prefix, suffix); expr != nil {
		policy.FilterExpression = expr
	}

	rs, err := client.Query(policy, stmt)
	if err != nil {
		return nil, s.maybeIndexError(cfg, ns, err)
	}
	defer rs.Close()

	keys := make([]string, 0, 64)
	for res := range rs.Results() {
		if res.Err != nil {
			return nil, s.maybeIndexError(cfg, ns, res.Err)
		}
		if res.Record == nil {
			continue
		}
		k, ok := res.Record.Bins[binKey].(string)
		if !ok {
			continue
		}
		keys = append(keys, k)
		if len(keys) > cfg.maxListKeys {
			return nil, fmt.Errorf("%w: more than %d keys in table %q", ErrTooManyListKeys, cfg.maxListKeys, table)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *aerospikeStore) maybeIndexError(cfg config, ns string, err error) error {
	if e, ok := err.(aero.Error); ok && e.Matches(types.INDEX_NOTFOUND) {
		return &MissingIndexError{Namespace: ns, Set: cfg.dataSet, Bin: binTable, Index: indexName, Cause: err}
	}
	return err
}

// keyFilterExpr builds anchored regex filter expressions on bin k. All
// metacharacters in the prefix/suffix arguments are escaped. Prefix and suffix
// are ANDed as independent conditions so keys whose prefix and suffix overlap
// still match.
func keyFilterExpr(prefix, suffix string) *aero.Expression {
	flags := aero.ExpRegexFlagEXTENDED | aero.ExpRegexFlagNOSUB
	kbin := func() *aero.Expression { return aero.ExpStringBin(binKey) }
	var exprs []*aero.Expression
	if prefix != "" {
		exprs = append(exprs, aero.ExpRegexCompare("^"+regexp.QuoteMeta(prefix), flags, kbin()))
	}
	if suffix != "" {
		exprs = append(exprs, aero.ExpRegexCompare(regexp.QuoteMeta(suffix)+"$", flags, kbin()))
	}
	switch len(exprs) {
	case 0:
		return nil
	case 1:
		return exprs[0]
	default:
		return aero.ExpAnd(exprs...)
	}
}

// List returns sorted keys matching prefix/suffix with offset/limit paging.
func (s *aerospikeStore) List(opts ...store.ListOption) ([]string, error) {
	client, so, cfg, err := s.getState()
	if err != nil {
		return nil, err
	}
	lopts := store.ListOptions{}
	for _, o := range opts {
		o(&lopts)
	}
	ns, table, err := s.resolve(client, so, cfg, lopts.Database, lopts.Table)
	if err != nil {
		return nil, err
	}
	keys, err := s.listKeys(client, cfg, ns, table, lopts.Prefix, lopts.Suffix)
	if err != nil {
		return nil, err
	}
	return enforceLimits(keys, lopts.Limit, lopts.Offset), nil
}

// Sweep deletes chunk records in a namespace whose version no head references.
// It is explicit maintenance and never runs automatically. Options apply only
// to this call — they never change the store's defaults.
func (s *aerospikeStore) Sweep(opts ...store.Option) (int, error) {
	client, err := s.getClient()
	if err != nil {
		return 0, err
	}
	s.mu.RLock()
	so := s.opts
	s.mu.RUnlock()
	for _, o := range opts {
		o(&so)
	}
	cfg := loadConfig(so)
	ns := so.Database

	// Only chunks older than the sweep grace are candidates. Age is measured
	// by the server from the chunk's last write, so chunks belonging to an
	// in-flight (not yet committed) write are never reclaimed.
	policy := aero.NewScanPolicy()
	if cfg.sweepGrace > 0 {
		policy.FilterExpression = aero.ExpGreater(
			aero.ExpSinceUpdate(),
			aero.ExpIntVal(cfg.sweepGrace.Milliseconds()),
		)
	}
	rs, err := client.ScanAll(policy, ns, cfg.chunkSet, binChunkHead, binChunkVer)
	if err != nil {
		return 0, err
	}
	defer rs.Close()

	deleted := 0
	for res := range rs.Results() {
		if res.Err != nil {
			return deleted, res.Err
		}
		r := res.Record
		if r == nil {
			continue
		}
		encoded, _ := r.Bins[binChunkHead].(string)
		version, _ := r.Bins[binChunkVer].(string)
		if encoded == "" || version == "" {
			continue
		}
		hk, _ := aero.NewKey(ns, cfg.dataSet, encoded)
		head, err := client.Get(nil, hk, binChunkVersion)
		if err != nil {
			if e, ok := err.(aero.Error); ok && e.Matches(types.KEY_NOT_FOUND_ERROR) {
				head = nil
			} else {
				return deleted, err
			}
		}
		hv, _ := headVersion(head)
		if head == nil || hv != version {
			if _, err := client.Delete(nil, r.Key); err == nil {
				deleted++
			}
		}
	}
	return deleted, nil
}

func headVersion(rec *aero.Record) (string, bool) {
	if rec == nil {
		return "", false
	}
	v, _ := rec.Bins[binChunkVersion].(string)
	return v, v != ""
}

// enforceLimits applies offset then limit without panicking.
func enforceLimits[V any](recs []V, limit, offset uint) []V {
	l := uint(len(recs))
	from := offset
	if from > l {
		from = l
	}
	to := l
	if limit > 0 {
		end := offset + limit
		if end < offset || end > l { // saturating add; overflow or overhang clamps to l
			end = l
		}
		to = end
	}
	return recs[from:to]
}
