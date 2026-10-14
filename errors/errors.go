// Package errors defines the error variables that may be returned
// during bbolt operations.
package errors

import "errors"

// These errors can be returned when opening or calling methods on a DB.
var (
	// ErrDatabaseNotOpen is returned when a DB instance is accessed before it
	// is opened or after it is closed.
	ErrDatabaseNotOpen = errors.New("database not open")

	// ErrInvalid is returned when both meta pages on a database are invalid.
	// This typically occurs when a file is not a bolt database.
	ErrInvalid = errors.New("invalid database")

	// ErrInvalidMapping is returned when the database file fails to get mapped.
	ErrInvalidMapping = errors.New("database isn't correctly mapped")

	// ErrVersionMismatch is returned when the data file was created with a
	// different version of Bolt.
	ErrVersionMismatch = errors.New("version mismatch")

	// ErrChecksum is returned when a checksum mismatch occurs on either of the two meta pages.
	ErrChecksum = errors.New("checksum error")

	// ErrTimeout is returned when a database cannot obtain an exclusive lock
	// on the data file after the timeout passed to Open().
	ErrTimeout = errors.New("timeout")
)

// These errors can occur when beginning or committing a Tx.
var (
	// ErrTxNotWritable is returned when performing a write operation on a
	// read-only transaction.
	ErrTxNotWritable = errors.New("tx not writable")

	// ErrTxClosed is returned when committing or rolling back a transaction
	// that has already been committed or rolled back.
	ErrTxClosed = errors.New("tx closed")

	// ErrDatabaseReadOnly is returned when a mutating transaction is started on a
	// read-only database.
	ErrDatabaseReadOnly = errors.New("database is in read-only mode")

	// ErrFreePagesNotLoaded is returned when a readonly transaction without
	// preloading the free pages is trying to access the free pages.
	ErrFreePagesNotLoaded = errors.New("free pages are not pre-loaded")
)

// These errors can occur when putting or deleting a value or a bucket.
var (
	// ErrBucketNotFound is returned when trying to access a bucket that has
	// not been created yet.
	ErrBucketNotFound = errors.New("bucket not found")

	// ErrBucketExists is returned when creating a bucket that already exists.
	ErrBucketExists = errors.New("bucket already exists")

	// ErrBucketNameRequired is returned when creating a bucket with a blank name.
	ErrBucketNameRequired = errors.New("bucket name required")

	// ErrKeyRequired is returned when inserting a zero-length key.
	ErrKeyRequired = errors.New("key required")

	// ErrKeyTooLarge is returned when inserting a key that is larger than MaxKeySize.
	ErrKeyTooLarge = errors.New("key too large")

	// ErrValueTooLarge is returned when inserting a value that is larger than MaxValueSize.
	ErrValueTooLarge = errors.New("value too large")

	// ErrMaxSizeReached is returned when the configured maximum size of the data file is reached.
	ErrMaxSizeReached = errors.New("database reached maximum size")

	// ErrIncompatibleValue is returned when trying to create or delete a bucket
	// on an existing non-bucket key or when trying to create or delete a
	// non-bucket key on an existing bucket key.
	ErrIncompatibleValue = errors.New("incompatible value")

	// ErrSameBuckets is returned when trying to move a sub-bucket between
	// source and target buckets, while source and target buckets are the same.
	ErrSameBuckets = errors.New("the source and target are the same bucket")

	// ErrDifferentDB is returned when trying to move a sub-bucket between
	// source and target buckets, while source and target buckets are in different database files.
	ErrDifferentDB = errors.New("the source and target buckets are in different database files")
)

// These errors can occur when accessing retained historical revisions
// through DB.ViewAt.
var (
	// ErrHistoryDisabled is returned from ViewAt when the database was
	// opened without a positive Options.HistoryLimit.
	ErrHistoryDisabled = errors.New("history revision retention is disabled")

	// ErrRevisionInvalid is returned from ViewAt when the given revision id
	// is not a positive transaction id.
	ErrRevisionInvalid = errors.New("invalid revision id")

	// ErrRevisionExpired is returned from ViewAt when the given revision id
	// is older than the oldest retained revision.
	ErrRevisionExpired = errors.New("revision expired")

	// ErrRevisionNotFound is returned from ViewAt when the given revision id
	// is inside the retained range but does not correspond to a successfully
	// committed revision (e.g. it was skipped by a rolled back transaction).
	ErrRevisionNotFound = errors.New("revision not found")

	// ErrRevisionFuture is returned from ViewAt when the given revision id
	// is greater than the latest committed revision.
	ErrRevisionFuture = errors.New("revision is in the future")

	// ErrHistoryCorrupt is returned when the persisted retained-revision index
	// fails its integrity validation while opening a database.
	ErrHistoryCorrupt = errors.New("history index is corrupt")
)
