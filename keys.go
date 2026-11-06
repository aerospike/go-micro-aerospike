package aerospike

import (
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
// head without relying on sendKey.
const (
	binChunkHead = "ek"
	binChunkVer  = "cv"
)

const layoutVersion = 1

// indexName is the secondary index on bin t of the data set.
const indexName = "go_micro_table"

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
