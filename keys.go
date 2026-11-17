package aerospike

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
)

// Bin names on head records.
const (
	binTable         = "t"
	binKey           = "k"
	binValue         = "v"
	binMeta          = "m"
	binChunkCount    = "chunk_count"
	binChunkVersion  = "chunk_version"
	binLayoutVersion = "layout_version"
)

// Additional bins on chunk records: ek and cv let Sweep map a chunk back to its
// head without relying on sendKey, and ow records the owning data set so a
// Sweep of a shared chunk set never touches another data set's chunks.
const (
	binChunkHead  = "ek"
	binChunkVer   = "cv"
	binChunkOwner = "ow"
)

const layoutVersion = 1

// indexName derives the secondary-index name for a data set. Sindex names are
// unique per namespace while each index binds one set, so a fixed name can
// index at most one data set per namespace; a set-derived name stays unique
// and deterministic. Hashed because set names (63 bytes) plus a readable
// prefix could exceed the 64-byte index-name limit.
func indexName(set string) string {
	sum := sha256.Sum256([]byte(set))
	return "gmt_" + hex.EncodeToString(sum[:8])
}

// defaultTable is used when no table is configured.
const defaultTable = "micro"

// encodeKey produces the unambiguous length-prefixed user key for (table, key):
// `len(table) ":" table ":" key`.
func encodeKey(table, key string) string {
	return fmt.Sprintf("%d:%s:%s", len(table), table, key)
}

// chunkKey encodes a chunk record's user key as `encodedKey|version|index`.
func chunkKey(encoded string, version string, index int) string {
	return encoded + "|" + version + "|" + strconv.Itoa(index)
}
