package aerospike

import "errors"

var (
	// ErrKeyTooLarge is returned when a record key exceeds the configured key cap.
	ErrKeyTooLarge = errors.New("key exceeds the configured maximum size")
	// ErrTableTooLarge is returned when a table name exceeds the configured table cap.
	ErrTableTooLarge = errors.New("table name exceeds the configured maximum size")
	// ErrMetadataTooLarge is returned when serialised metadata exceeds the configured metadata cap.
	ErrMetadataTooLarge = errors.New("metadata exceeds the configured maximum size")
	// ErrValueTooLarge is returned when a value exceeds the total value cap, or the
	// inline threshold while chunking is disabled.
	ErrValueTooLarge = errors.New("value exceeds the configured maximum size")
	// ErrTooManyListKeys is returned when a listing matches more than MaxListKeys keys.
	ErrTooManyListKeys = errors.New("list matched more keys than MaxListKeys")
	// ErrCorruption is returned when a chunked record is missing a chunk of the
	// version its head references, after bounded retries.
	ErrCorruption = errors.New("record is corrupt: head references missing chunks")
	// ErrUnknownLayout is returned when a head record carries a layout version this
	// store does not understand.
	ErrUnknownLayout = errors.New("record was written with an unknown layout version")
	// ErrClosed is returned for calls made after Close.
	ErrClosed = errors.New("store is closed")
)

// MissingIndexError reports that the secondary index on the table bin is not
// usable and explains how to create it.
type MissingIndexError struct {
	Namespace string
	Set       string
	Bin       string
	Index     string
	Cause     error
}

func (e *MissingIndexError) Error() string {
	return "aerospike: secondary index " + e.Index + " on bin " + e.Bin +
		" of " + e.Namespace + "/" + e.Set + " is not usable; create it with " +
		"asinfo -v 'sindex-create:ns=" + e.Namespace + ";set=" + e.Set +
		";indexname=" + e.Index + ";indextype=default;indexdata=" + e.Bin + ",string' " +
		"or let the store create it (the default)"
}

func (e *MissingIndexError) Unwrap() error { return e.Cause }
