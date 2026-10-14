package common

import (
	"fmt"
	"hash/fnv"
	"io"
	"unsafe"

	"go.etcd.io/bbolt/errors"
)

type Meta struct {
	magic    uint32
	version  uint32
	pageSize uint32
	flags    uint32
	root     InBucket
	freelist Pgid
	pgid     Pgid
	txid     Txid
	checksum uint64

	// history points to the head page of the retained-revision index.
	// It is appended after checksum so Sum64() keeps hashing exactly the
	// same byte prefix as legacy databases and old files stay readable.
	// A zero value means history revision retention is disabled.
	history Pgid
}

// Validate checks the marker bytes and version of the meta page to ensure it matches this binary.
func (m *Meta) Validate() error {
	if m.magic != Magic {
		return errors.ErrInvalid
	} else if m.version != Version {
		return errors.ErrVersionMismatch
	} else if m.checksum != m.Sum64() {
		return errors.ErrChecksum
	}
	return nil
}

// Copy copies one meta object to another.
func (m *Meta) Copy(dest *Meta) {
	*dest = *m
}

// Write writes the meta onto a page.
func (m *Meta) Write(p *Page) {
	if m.root.root >= m.pgid {
		panic(fmt.Sprintf("root bucket pgid (%d) above high water mark (%d)", m.root.root, m.pgid))
	} else if m.freelist >= m.pgid && m.freelist != PgidNoFreelist {
		// TODO: reject pgidNoFreeList if !NoFreelistSync
		panic(fmt.Sprintf("freelist pgid (%d) above high water mark (%d)", m.freelist, m.pgid))
	} else if m.history >= m.pgid && m.history != 0 {
		panic(fmt.Sprintf("history index pgid (%d) above high water mark (%d)", m.history, m.pgid))
	}

	// Page id is either going to be 0 or 1 which we can determine by the transaction ID.
	p.id = Pgid(m.txid % 2)
	p.SetFlags(MetaPageFlag)

	// Calculate the checksum.
	m.checksum = m.Sum64()

	m.Copy(p.Meta())
}

// Sum64 generates the checksum for the meta.
//
// The checksum always covers every field up to (but excluding) the checksum
// field, which keeps the algorithm identical to legacy databases. When the
// history index pointer is present it is checksummed as well, so a torn meta
// page write cannot make validation follow a garbage history pointer; legacy
// meta pages always carry a zero pointer there and validate unchanged.
func (m *Meta) Sum64() uint64 {
	var h = fnv.New64a()
	_, _ = h.Write((*[unsafe.Offsetof(Meta{}.checksum)]byte)(unsafe.Pointer(m))[:])
	if m.history != 0 {
		_, _ = h.Write((*[unsafe.Sizeof(Pgid(0))]byte)(unsafe.Pointer(&m.history))[:])
	}
	return h.Sum64()
}

func (m *Meta) Magic() uint32 {
	return m.magic
}

func (m *Meta) SetMagic(v uint32) {
	m.magic = v
}

func (m *Meta) Version() uint32 {
	return m.version
}

func (m *Meta) SetVersion(v uint32) {
	m.version = v
}

func (m *Meta) PageSize() uint32 {
	return m.pageSize
}

func (m *Meta) SetPageSize(v uint32) {
	m.pageSize = v
}

func (m *Meta) Flags() uint32 {
	return m.flags
}

func (m *Meta) SetFlags(v uint32) {
	m.flags = v
}

func (m *Meta) SetRootBucket(b InBucket) {
	m.root = b
}

func (m *Meta) RootBucket() *InBucket {
	return &m.root
}

func (m *Meta) Freelist() Pgid {
	return m.freelist
}

func (m *Meta) SetFreelist(v Pgid) {
	m.freelist = v
}

func (m *Meta) IsFreelistPersisted() bool {
	return m.freelist != PgidNoFreelist
}

func (m *Meta) Pgid() Pgid {
	return m.pgid
}

func (m *Meta) SetPgid(id Pgid) {
	m.pgid = id
}

func (m *Meta) Txid() Txid {
	return m.txid
}

func (m *Meta) SetTxid(id Txid) {
	m.txid = id
}

func (m *Meta) IncTxid() {
	m.txid += 1
}

func (m *Meta) DecTxid() {
	m.txid -= 1
}

func (m *Meta) Checksum() uint64 {
	return m.checksum
}

func (m *Meta) SetChecksum(v uint64) {
	m.checksum = v
}

// History returns the pgid of the retained-revision index page, or 0 if
// history retention is disabled.
func (m *Meta) History() Pgid {
	return m.history
}

// SetHistory sets the pgid of the retained-revision index page.
func (m *Meta) SetHistory(id Pgid) {
	m.history = id
}

func (m *Meta) Print(w io.Writer) {
	fmt.Fprintf(w, "Version:    %d\n", m.version)
	fmt.Fprintf(w, "Page Size:  %d bytes\n", m.pageSize)
	fmt.Fprintf(w, "Flags:      %08x\n", m.flags)
	fmt.Fprintf(w, "Root:       <pgid=%d>\n", m.root.root)
	fmt.Fprintf(w, "Freelist:   <pgid=%d>\n", m.freelist)
	fmt.Fprintf(w, "HWM:        <pgid=%d>\n", m.pgid)
	fmt.Fprintf(w, "History:    <pgid=%d>\n", m.history)
	fmt.Fprintf(w, "Txn ID:     %d\n", m.txid)
	fmt.Fprintf(w, "Checksum:   %016x\n", m.checksum)
	fmt.Fprintf(w, "\n")
}
