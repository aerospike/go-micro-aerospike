package aerospike

import (
	"context"
	"crypto/tls"
	"time"

	aero "github.com/aerospike/aerospike-client-go/v8"
	"go-micro.dev/v6/store"
)

// Private context keys carrying this store's configuration in Options.Context,
// following the nats-js-kv convention.
type (
	dataSetKey       struct{}
	chunkSetKey      struct{}
	maxKeySizeKey    struct{}
	maxTableSizeKey  struct{}
	maxMetadataKey   struct{}
	inlineThreshKey  struct{}
	chunkSizeKey     struct{}
	maxValueSizeKey  struct{}
	chunkingKey      struct{}
	maxListKeysKey   struct{}
	createIndexKey   struct{}
	authKey          struct{}
	tlsKey           struct{}
	clientPolicyKey  struct{}
	readerRetriesKey struct{}
	sweepGraceKey    struct{}
)

type authConfig struct {
	user     string
	password string
}

// config is the resolved store configuration extracted from Options.
type config struct {
	dataSet      string
	chunkSet     string
	maxKeySize   int
	maxTableSize int
	maxMetadata  int
	inlineThresh int
	chunkSize    int
	maxValueSize int
	chunking     bool
	maxListKeys  int
	createIndex  bool
	auth         *authConfig
	tlsConfig    *tls.Config
	// tlsSet records that WithTLS was given (even WithTLS(nil)), so an
	// explicit nil overrides a base policy's TlsConfig while an absent
	// option preserves it.
	tlsSet        bool
	clientPolicy  *aero.ClientPolicy
	readerRetries int
	// sweepGrace is the minimum chunk age Sweep will consider deleting, so
	// chunks written by an in-flight, not-yet-committed write are never
	// reclaimed.
	sweepGrace time.Duration
}

func defaultConfig() config {
	return config{
		dataSet:       "micro",
		chunkSet:      "micro_chunks",
		maxKeySize:    1024,
		maxTableSize:  255,
		maxMetadata:   64 * 1024,
		inlineThresh:  512 * 1024,
		chunkSize:     512 * 1024,
		maxValueSize:  16 * 1024 * 1024,
		chunking:      true,
		maxListKeys:   100_000,
		createIndex:   true,
		readerRetries: 2,
		sweepGrace:    60 * time.Second,
	}
}

// loadConfig resolves the effective config from the given options. It does
// not mutate the store; callers assign the result where appropriate.
func loadConfig(so store.Options) config {
	c := defaultConfig()
	ctx := so.Context
	if ctx == nil {
		return c
	}
	get := func(k any, dst func(v any)) {
		if v := ctx.Value(k); v != nil {
			dst(v)
		}
	}
	get(dataSetKey{}, func(v any) { c.dataSet = v.(string) })
	get(chunkSetKey{}, func(v any) { c.chunkSet = v.(string) })
	get(maxKeySizeKey{}, func(v any) { c.maxKeySize = v.(int) })
	get(maxTableSizeKey{}, func(v any) { c.maxTableSize = v.(int) })
	get(maxMetadataKey{}, func(v any) { c.maxMetadata = v.(int) })
	get(inlineThreshKey{}, func(v any) { c.inlineThresh = v.(int) })
	get(chunkSizeKey{}, func(v any) { c.chunkSize = v.(int) })
	get(maxValueSizeKey{}, func(v any) { c.maxValueSize = v.(int) })
	get(chunkingKey{}, func(v any) { c.chunking = v.(bool) })
	get(maxListKeysKey{}, func(v any) { c.maxListKeys = v.(int) })
	get(createIndexKey{}, func(v any) { c.createIndex = v.(bool) })
	get(authKey{}, func(v any) { a := v.(authConfig); c.auth = &a })
	get(tlsKey{}, func(v any) { c.tlsSet = true; c.tlsConfig = v.(*tls.Config) })
	get(clientPolicyKey{}, func(v any) { c.clientPolicy = v.(*aero.ClientPolicy) })
	get(readerRetriesKey{}, func(v any) { c.readerRetries = v.(int) })
	get(sweepGraceKey{}, func(v any) { c.sweepGrace = v.(time.Duration) })
	return c
}

func setOption(k, v any) store.Option {
	return func(o *store.Options) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, k, v)
	}
}

// WithDataSet sets the Aerospike set holding head records. Default "micro".
func WithDataSet(name string) store.Option { return setOption(dataSetKey{}, name) }

// WithChunkSet sets the Aerospike set holding chunk records. Default "micro_chunks".
func WithChunkSet(name string) store.Option { return setOption(chunkSetKey{}, name) }

// WithMaxKeySize caps record key length in bytes. Default 1 KiB.
func WithMaxKeySize(n int) store.Option { return setOption(maxKeySizeKey{}, n) }

// WithMaxTableSize caps table name length in bytes. Default 255.
func WithMaxTableSize(n int) store.Option { return setOption(maxTableSizeKey{}, n) }

// WithMaxMetadataSize caps serialised record metadata in bytes. Default 64 KiB.
func WithMaxMetadataSize(n int) store.Option { return setOption(maxMetadataKey{}, n) }

// WithInlineThreshold sets the value size above which values are chunked (or
// rejected when chunking is disabled). Default 512 KiB.
func WithInlineThreshold(n int) store.Option { return setOption(inlineThreshKey{}, n) }

// WithChunkSize sets the payload size of each chunk record. Default 512 KiB.
func WithChunkSize(n int) store.Option { return setOption(chunkSizeKey{}, n) }

// WithMaxValueSize caps total value size in bytes. Default 16 MiB.
func WithMaxValueSize(n int) store.Option { return setOption(maxValueSizeKey{}, n) }

// WithChunking enables or disables transparent chunking of large values.
// When disabled, values above the inline threshold fail with ErrValueTooLarge.
// Default enabled.
func WithChunking(enabled bool) store.Option { return setOption(chunkingKey{}, enabled) }

// WithMaxListKeys bounds the number of keys a List or prefix/suffix Read will
// collect before returning ErrTooManyListKeys. Default 100,000.
func WithMaxListKeys(n int) store.Option { return setOption(maxListKeysKey{}, n) }

// WithCreateIndex controls whether Init ensures the secondary index on the
// table bin exists. Default true; set false when operators pre-create it.
func WithCreateIndex(enabled bool) store.Option { return setOption(createIndexKey{}, enabled) }

// WithAuth enables authentication with the given credentials. The password is
// never logged.
func WithAuth(user, password string) store.Option {
	return setOption(authKey{}, authConfig{user: user, password: password})
}

// WithTLS enables TLS for every connection using the given configuration.
// There is no plaintext fallback.
func WithTLS(cfg *tls.Config) store.Option { return setOption(tlsKey{}, cfg) }

// WithClientPolicy is an escape hatch supplying a caller-built client policy.
// It is used as the base policy; auth and TLS options still apply on top.
func WithClientPolicy(p *aero.ClientPolicy) store.Option {
	return setOption(clientPolicyKey{}, p)
}

// WithReaderRetries bounds how many times a chunked read re-reads the head and
// retries on missing chunks. Default 2.
func WithReaderRetries(n int) store.Option { return setOption(readerRetriesKey{}, n) }

// WithSweepGrace sets the minimum age a chunk record must reach before Sweep
// will consider deleting it. The age is measured from the chunk's last write
// by the server, so chunks belonging to an in-flight, not-yet-committed write
// are never reclaimed. Default 60 s — comfortably above the writer's bounded
// chunk-batch-plus-commit window (~2 s under default client timeouts).
func WithSweepGrace(d time.Duration) store.Option { return setOption(sweepGraceKey{}, d) }
