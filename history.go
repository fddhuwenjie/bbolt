package bbolt

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"unsafe"

	berrors "go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/common"
)

// 历史修订（时间旅行视图）的磁盘布局：
//
// meta.history 指向一个或多个连续的“历史索引页”（HistoryPageFlag）。
// 索引页页体：
//
//	[ 16B 标准页头 ]
//	[ 4B magic ][ 4B version ][ 4B limit ][ 4B count ]
//	[ 8B checksum ]
//	[ count * 40B 定长修订记录 ]
//
// 每条修订记录：
//
//	[ 8B txid ][ 8B 根桶根页 ][ 8B 根桶序号 ][ 8B 页日志首页 ][ 4B 日志页数组长度 ][ 4B 保留 ]
//
// 页日志页同样使用 HistoryPageFlag，页体：
//
//	[ 16B 标准页头 ]
//	[ 4B magic ][ 4B count ][ 8B checksum ]
//	[ count * 8B 被该次提交替换掉的旧数据页首页 pgid ]
//
// 这些旧页在对应修订离开保留窗口之前不会进入空闲页列表，因此旧修订的
// 整棵 B 树在 mmap 中始终保持字节级不变。索引页与页日志页自身也采用
// 写时复制：每次提交都分配新页，旧页按与旧 freelist 页相同的生命周期
// 延迟释放，从而保证崩溃回退到上一个 meta 时引用仍然有效。
const (
	histIndexMagic  uint32 = 0x48495354 // "HIST"
	histBlobMagic   uint32 = 0x48424c42 // "HBLB"
	histDiskVersion uint32 = 1

	histIndexHeaderSize = 24
	histDiskEntrySize   = 40
	histBlobHeaderSize  = 16
)

// histEntry 是一个仍在保留窗口内的成功提交修订。
type histEntry struct {
	txid     common.Txid   // 修订 ID（即提交时的 Tx.ID()）
	root     common.Pgid   // 当时根桶的根页
	sequence uint64        // 当时根桶的序号
	blobPgid common.Pgid   // 该修订页日志首页，0 表示无
	freed    []common.Pgid // 该次提交替换掉、仍需为旧视图保留的数据页
}

// history 维护保留修订窗口的内存状态与持久化编解码。
type history struct {
	enabled bool // Options.HistoryLimit > 0
	limit   int  // 保留窗口大小（含当前修订）

	// entries 始终按 txid 升序；mu 保护 indexPgid 与 entries。
	// 写事务本身已由 rwlock 串行化，这里的锁仅用于与 ViewAt 的并发只读访问协同。
	mu        sync.RWMutex
	indexPgid common.Pgid // 当前已持久化的索引页（首次提交前为 0）
	entries   []histEntry
}

// loadHistory 在 mmap 建立之后、freelist 加载之前读取历史索引。
// limit 为 Options.HistoryLimit；limit == 0 表示关闭保留。
func (db *DB) loadHistory(limit int) error {
	h := &history{limit: limit, enabled: limit > 0}

	if hp := db.meta().History(); hp != 0 {
		h.indexPgid = hp
		entries, err := h.decodeIndex(db)
		if err != nil {
			return err
		}
		h.entries = entries
	}

	if h.enabled {
		// 在已有的库上首次开启、或全新数据库：以当前 meta 作为锚点修订。
		// 更早的修订不可恢复（其页可能已被复用），只能从当前一致状态开始保留。
		if len(h.entries) == 0 {
			m := db.meta()
			h.entries = []histEntry{{
				txid:     m.Txid(),
				root:     m.RootBucket().RootPage(),
				sequence: m.RootBucket().InSequence(),
			}}
		}
		db.hist = h
	} else if h.indexPgid != 0 {
		// 库中存在历史索引但本次打开关闭了保留：保留解码结果，
		// 供打开过程中的一次性清理使用；清理后 db.hist 会被置空。
		db.hist = h
	}

	return nil
}

// purgeHistoryOnOpen 在以可写方式打开且关闭了历史保留时，把旧修订仍
// 占用的页（旧数据页、页日志页、历史索引页）全部交还给 freelist，
// 并清除 meta.history 指针。
func (db *DB) purgeHistoryOnOpen() error {
	h := db.hist
	if h == nil || h.enabled || h.indexPgid == 0 {
		return nil
	}

	err := db.Update(func(tx *Tx) error {
		tid := tx.meta.Txid()
		blobs := make(map[common.Pgid]struct{})
		for i := range h.entries {
			e := &h.entries[i]
			for _, id := range e.freed {
				tx.db.freelist.Free(tid, tx.db.page(id))
			}
			if e.blobPgid != 0 {
				if _, ok := blobs[e.blobPgid]; !ok {
					blobs[e.blobPgid] = struct{}{}
					tx.db.freelist.Free(tid, tx.db.page(e.blobPgid))
				}
			}
		}
		tx.db.freelist.Free(tid, tx.db.page(h.indexPgid))
		tx.meta.SetHistory(0)
		return nil
	})
	if err != nil {
		return fmt.Errorf("purge history index failed: %w", err)
	}
	db.hist = nil
	return nil
}

// freeDataPage 替换写路径上对 freelist.Free 的直接调用：启用历史保留
// 时，被替换的旧页先登记到当前写事务的暂存区，直到对应修订离开窗口；
// 未启用时行为与直接调用 freelist.Free 完全一致。
func (db *DB) freeDataPage(tid common.Txid, p *common.Page) {
	if db.rwtx != nil && db.hist != nil && db.hist.enabled {
		db.rwtx.histFrees = append(db.rwtx.histFrees, p.Id())
		return
	}
	db.freelist.Free(tid, p)
}

// prepareCommit 在根桶 spill 完成、freelist 序列化之前推进修订窗口：
// 淘汰超出窗口的旧修订并释放其专属页，为新修订写页日志，写时复制
// 生成新的历史索引并更新 meta.history。失败时调用方负责 rollback，
// 通过 freelist.Rollback(T) 撤销本函数对 freelist 的全部改动。
func (h *history) prepareCommit(tx *Tx) error {
	h.mu.RLock()
	oldEntries := h.entries
	oldIndex := h.indexPgid
	h.mu.RUnlock()

	tid := tx.meta.Txid()
	root := tx.meta.RootBucket()

	entries := make([]histEntry, 0, len(oldEntries)+1)
	entries = append(entries, oldEntries...)
	entries = append(entries, histEntry{
		txid:     tid,
		root:     root.RootPage(),
		sequence: root.InSequence(),
		freed:    tx.histFrees,
	})

	// 窗口只保留最后 limit 个修订（含本次提交）。
	var dropped []histEntry
	if len(entries) > h.limit {
		dropped = entries[:len(entries)-h.limit]
		entries = entries[len(entries)-h.limit:]
	}

	// 被淘汰修订的页日志中的旧页不再被任何保留修订引用，用当前 tid
	// 挂到 freelist 的 pending 上：复用仍受只读事务水位约束，而本事务
	// 的 freelist 序列化会把它们写入磁盘，崩溃重开后同样视为空闲。
	for i := range dropped {
		e := &dropped[i]
		for _, id := range e.freed {
			tx.db.freelist.Free(tid, tx.db.page(id))
		}
		if e.blobPgid != 0 {
			tx.db.freelist.Free(tid, tx.db.page(e.blobPgid))
		}
	}

	// 新修订的页日志（仅记录被替换页的首页 pgid；页块长度在释放时
	// 从页头 overflow 字段读回）。
	last := &entries[len(entries)-1]
	if len(tx.histFrees) > 0 {
		blobPgid, err := h.writeBlob(tx, tx.histFrees)
		if err != nil {
			return err
		}
		last.blobPgid = blobPgid
	}

	// 旧索引页按 freelist 页相同的方式做写时复制释放。
	if oldIndex != 0 {
		tx.db.freelist.Free(tid, tx.db.page(oldIndex))
	}

	indexPgid, err := h.writeIndex(tx, entries)
	if err != nil {
		return err
	}
	tx.meta.SetHistory(indexPgid)

	// 仅在提交成功后（commitDone）才替换内存窗口；失败回滚保持旧状态。
	tx.histPrepared = entries
	return nil
}

// commitDone 在 meta 页成功写入后切换内存中的修订窗口。
func (h *history) commitDone(tx *Tx) {
	h.mu.Lock()
	h.entries = tx.histPrepared
	h.indexPgid = tx.meta.History()
	h.mu.Unlock()
	tx.histPrepared = nil
}

// writeIndex 分配并填充新的历史索引页块。
func (h *history) writeIndex(tx *Tx, entries []histEntry) (common.Pgid, error) {
	payload := histIndexHeaderSize + histDiskEntrySize*len(entries)
	total := int(common.PageHeaderSize) + payload
	nPages := (total + tx.db.pageSize - 1) / tx.db.pageSize

	p, err := tx.allocate(nPages)
	if err != nil {
		return 0, err
	}
	p.SetFlags(common.HistoryPageFlag)
	p.SetCount(0)

	buf := common.UnsafeByteSlice(unsafe.Pointer(p), 0, 0, nPages*tx.db.pageSize)
	b := buf[int(common.PageHeaderSize):]

	binary.LittleEndian.PutUint32(b[0:4], histIndexMagic)
	binary.LittleEndian.PutUint32(b[4:8], histDiskVersion)
	binary.LittleEndian.PutUint32(b[8:12], uint32(h.limit))
	binary.LittleEndian.PutUint32(b[12:16], uint32(len(entries)))

	eb := b[histIndexHeaderSize:]
	for i := range entries {
		e := eb[i*histDiskEntrySize : (i+1)*histDiskEntrySize]
		binary.LittleEndian.PutUint64(e[0:8], uint64(entries[i].txid))
		binary.LittleEndian.PutUint64(e[8:16], uint64(entries[i].root))
		binary.LittleEndian.PutUint64(e[16:24], entries[i].sequence)
		binary.LittleEndian.PutUint64(e[24:32], uint64(entries[i].blobPgid))
		binary.LittleEndian.PutUint32(e[32:36], uint32(len(entries[i].freed)))
		binary.LittleEndian.PutUint32(e[36:40], 0)
	}

	// 校验和覆盖定长头部的前 16 字节与全部修订记录（不含校验和自身）。
	csum := histChecksum(b[:16], b[histIndexHeaderSize:histIndexHeaderSize+histDiskEntrySize*len(entries)])
	binary.LittleEndian.PutUint64(b[16:24], csum)

	return p.Id(), nil
}

// writeBlob 分配并填充某修订的页日志页块。
func (h *history) writeBlob(tx *Tx, ids []common.Pgid) (common.Pgid, error) {
	payload := histBlobHeaderSize + 8*len(ids)
	total := int(common.PageHeaderSize) + payload
	nPages := (total + tx.db.pageSize - 1) / tx.db.pageSize

	p, err := tx.allocate(nPages)
	if err != nil {
		return 0, err
	}
	p.SetFlags(common.HistoryPageFlag)
	p.SetCount(0)

	buf := common.UnsafeByteSlice(unsafe.Pointer(p), 0, 0, nPages*tx.db.pageSize)
	b := buf[int(common.PageHeaderSize):]

	binary.LittleEndian.PutUint32(b[0:4], histBlobMagic)
	binary.LittleEndian.PutUint32(b[4:8], uint32(len(ids)))
	db := b[histBlobHeaderSize : histBlobHeaderSize+8*len(ids)]
	for i, id := range ids {
		binary.LittleEndian.PutUint64(db[i*8:(i+1)*8], uint64(id))
	}
	csum := histChecksum(b[:8], db)
	binary.LittleEndian.PutUint64(b[8:16], csum)

	return p.Id(), nil
}

// decodeIndex 从 mmap 读取历史索引及其全部页日志。
func (h *history) decodeIndex(db *DB) ([]histEntry, error) {
	block, err := readHistoryBlock(db, h.indexPgid)
	if err != nil {
		return nil, err
	}
	b := block[int(common.PageHeaderSize):]

	if len(b) < histIndexHeaderSize {
		return nil, histCorrupt("index page too small")
	}
	if magic := binary.LittleEndian.Uint32(b[0:4]); magic != histIndexMagic {
		return nil, histCorrupt(fmt.Sprintf("bad index magic %x", magic))
	}
	if ver := binary.LittleEndian.Uint32(b[4:8]); ver != histDiskVersion {
		return nil, histCorrupt(fmt.Sprintf("unsupported index version %d", ver))
	}
	count := int(binary.LittleEndian.Uint32(b[12:16]))
	csum := binary.LittleEndian.Uint64(b[16:24])

	if count < 0 || histIndexHeaderSize+histDiskEntrySize*count > len(b) {
		return nil, histCorrupt("index entry count out of bounds")
	}
	entriesBytes := b[histIndexHeaderSize : histIndexHeaderSize+histDiskEntrySize*count]
	if csum != histChecksum(b[:16], entriesBytes) {
		return nil, histCorrupt("index checksum mismatch")
	}

	entries := make([]histEntry, 0, count)
	for i := 0; i < count; i++ {
		e := entriesBytes[i*histDiskEntrySize : (i+1)*histDiskEntrySize]
		entry := histEntry{
			txid:     common.Txid(binary.LittleEndian.Uint64(e[0:8])),
			root:     common.Pgid(binary.LittleEndian.Uint64(e[8:16])),
			sequence: binary.LittleEndian.Uint64(e[16:24]),
			blobPgid: common.Pgid(binary.LittleEndian.Uint64(e[24:32])),
		}
		blobCount := int(binary.LittleEndian.Uint32(e[32:36]))
		if entry.blobPgid != 0 {
			ids, err := h.decodeBlob(db, entry.blobPgid, blobCount)
			if err != nil {
				return nil, err
			}
			entry.freed = ids
		} else if blobCount != 0 {
			return nil, histCorrupt(fmt.Sprintf("revision %d claims %d journal pages without a journal page", entry.txid, blobCount))
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].txid < entries[j].txid })
	return entries, nil
}

// decodeBlob 从 mmap 读取某个修订的页日志。
func (h *history) decodeBlob(db *DB, pgid common.Pgid, expected int) ([]common.Pgid, error) {
	block, err := readHistoryBlock(db, pgid)
	if err != nil {
		return nil, err
	}
	b := block[int(common.PageHeaderSize):]

	if len(b) < histBlobHeaderSize {
		return nil, histCorrupt("journal page too small")
	}
	if magic := binary.LittleEndian.Uint32(b[0:4]); magic != histBlobMagic {
		return nil, histCorrupt(fmt.Sprintf("bad journal magic %x at page %d", magic, pgid))
	}
	count := int(binary.LittleEndian.Uint32(b[4:8]))
	if expected != count {
		return nil, histCorrupt(fmt.Sprintf("journal length mismatch at page %d: %d != %d", pgid, count, expected))
	}
	if histBlobHeaderSize+8*count > len(b) {
		return nil, histCorrupt("journal count out of bounds")
	}
	idsBytes := b[histBlobHeaderSize : histBlobHeaderSize+8*count]
	csum := binary.LittleEndian.Uint64(b[8:16])
	if csum != histChecksum(b[:8], idsBytes) {
		return nil, histCorrupt(fmt.Sprintf("journal checksum mismatch at page %d", pgid))
	}

	ids := make([]common.Pgid, 0, count)
	for i := 0; i < count; i++ {
		ids = append(ids, common.Pgid(binary.LittleEndian.Uint64(idsBytes[i*8:(i+1)*8])))
	}
	return ids, nil
}

// readHistoryBlock 返回历史页块（含连续 overflow 页）的 Go 内存副本。
func readHistoryBlock(db *DB, pgid common.Pgid) ([]byte, error) {
	if pgid < 2 || int64(pgid)*int64(db.pageSize) >= int64(db.datasz) {
		return nil, histCorrupt(fmt.Sprintf("history page id %d out of mapped bounds", pgid))
	}
	first := db.page(pgid)
	if !first.IsHistoryPage() {
		return nil, histCorrupt(fmt.Sprintf("page %d is not a history page (flags %x)", pgid, first.Flags()))
	}
	nPages := int(first.Overflow()) + 1
	end := int64(pgid)*int64(db.pageSize) + int64(nPages)*int64(db.pageSize)
	if end > int64(db.datasz) {
		return nil, histCorrupt(fmt.Sprintf("history page block %d (+%d pages) exceeds mapped size", pgid, nPages))
	}
	src := common.UnsafeByteSlice(unsafe.Pointer(first), 0, 0, nPages*db.pageSize)
	out := make([]byte, len(src))
	copy(out, src)
	return out, nil
}

func histChecksum(parts ...[]byte) uint64 {
	h := fnv.New64a()
	for _, p := range parts {
		_, _ = h.Write(p)
	}
	return h.Sum64()
}

func histCorrupt(msg string) error {
	return fmt.Errorf("%w: %s", berrors.ErrHistoryCorrupt, msg)
}

// resolve 在保留窗口内查找指定修订；查找发生时调用方应已持有 metalock。
// currentRoot/currentSeq/currentTxid 为当前最新 meta 的内容，用于消除
// “meta 已写入但内存窗口刚切换”这一极短窗口：恰好等于当前已提交修订
// 时直接用最新 meta 合成视图。
func (h *history) resolve(id common.Txid, current common.Txid, currentRoot common.Pgid, currentSeq uint64) (histEntry, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.entries) == 0 {
		return histEntry{}, berrors.ErrHistoryDisabled
	}
	last := h.entries[len(h.entries)-1]

	if id > last.txid {
		// 消除“meta 已写入而内存窗口刚切换”的短暂窗口：最新已提交修订
		// 尚未进入 entries 时直接以当前 meta 合成只读视图。
		if id == current {
			return histEntry{txid: id, root: currentRoot, sequence: currentSeq}, nil
		}
		return histEntry{}, berrors.ErrRevisionFuture
	}

	idx := sort.Search(len(h.entries), func(i int) bool { return h.entries[i].txid >= id })
	if idx < len(h.entries) && h.entries[idx].txid == id {
		return h.entries[idx], nil
	}
	if id < h.entries[0].txid {
		return histEntry{}, berrors.ErrRevisionExpired
	}
	return histEntry{}, berrors.ErrRevisionNotFound
}

// protectedPgids 返回当前所有不应被当作空闲页回收的历史层页：
// 历史索引、各修订页日志，以及仍被保留修订引用的旧数据页。
// 供 NoFreelistSync 打开时的全库扫描与一致性检查使用。
func (h *history) protectedPgids(db *DB) map[common.Pgid]struct{} {
	h.mu.RLock()
	defer h.mu.RUnlock()

	set := make(map[common.Pgid]struct{})
	if h.indexPgid != 0 {
		markHistoryBlock(set, db, h.indexPgid)
	}
	for i := range h.entries {
		e := &h.entries[i]
		if e.blobPgid != 0 {
			markHistoryBlock(set, db, e.blobPgid)
		}
		for _, id := range e.freed {
			set[id] = struct{}{}
		}
	}
	return set
}

// accountPages 把历史层页标记为可达，供 Tx.Check 使用。
func (h *history) accountPages(db *DB, mark func(common.Pgid, *common.Page)) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.indexPgid != 0 {
		accountHistoryBlock(db, h.indexPgid, mark)
	}
	for i := range h.entries {
		e := &h.entries[i]
		if e.blobPgid != 0 {
			accountHistoryBlock(db, e.blobPgid, mark)
		}
		for _, id := range e.freed {
			p := db.page(id)
			mark(id, p)
		}
	}
}

func markHistoryBlock(set map[common.Pgid]struct{}, db *DB, start common.Pgid) {
	p := db.page(start)
	for i := 0; i <= int(p.Overflow()); i++ {
		set[start+common.Pgid(i)] = struct{}{}
	}
}

func accountHistoryBlock(db *DB, start common.Pgid, mark func(common.Pgid, *common.Page)) {
	p := db.page(start)
	for i := 0; i <= int(p.Overflow()); i++ {
		id := start + common.Pgid(i)
		mark(id, db.page(id))
	}
}

// ViewAt 在指定的历史修订上执行只读托管事务。id 必须是保留窗口内某次
// 成功提交事务的 Tx.ID()：
//   - 未启用 Options.HistoryLimit 时返回 ErrHistoryDisabled；
//   - 非正数返回 ErrRevisionInvalid；
//   - 早于最老保留修订返回 ErrRevisionExpired；
//   - 落在保留范围内但没有对应成功提交（回滚造成的 ID 空洞）返回
//     ErrRevisionNotFound；
//   - 大于最新提交修订返回 ErrRevisionFuture。
//
// 出错时回调不会执行。回调内对（含嵌套）桶与游标的读操作与该修订
// 提交完成时的一致状态完全相同；即使其他 goroutine 持续提交导致窗口
// 前移，本次历史读取在结束前也保持稳定。回调中的 Tx 是只读事务，
// Tx.ID() 返回 id，且不应在回调返回后继续使用。
func (db *DB) ViewAt(id int, fn func(*Tx) error) error {
	t, err := db.beginHistoryTx(id)
	if err != nil {
		return err
	}

	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	t.managed = true
	err = fn(t)
	t.managed = false
	if err != nil {
		_ = t.Rollback()
		return err
	}
	return t.Rollback()
}

// beginHistoryTx 与 beginTx 类似，但把只读事务的根桶替换为目标修订的
// 根，并按目标修订 ID 在 freelist 上登记只读水位，保证窗口前移时旧页
// 不会被复用覆盖。
func (db *DB) beginHistoryTx(id int) (*Tx, error) {
	if db.hist == nil || !db.hist.enabled {
		return nil, berrors.ErrHistoryDisabled
	}
	if id <= 0 {
		return nil, berrors.ErrRevisionInvalid
	}

	db.metalock.Lock()
	db.mmaplock.RLock()

	if !db.opened {
		db.mmaplock.RUnlock()
		db.metalock.Unlock()
		return nil, berrors.ErrDatabaseNotOpen
	}
	if db.data == nil {
		db.mmaplock.RUnlock()
		db.metalock.Unlock()
		return nil, berrors.ErrInvalidMapping
	}

	m := db.meta()
	entry, err := db.hist.resolve(common.Txid(id), m.Txid(), m.RootBucket().RootPage(), m.RootBucket().InSequence())
	if err != nil {
		db.mmaplock.RUnlock()
		db.metalock.Unlock()
		return nil, err
	}

	t := &Tx{}
	t.init(db)

	// 使用历史修订作为事务身份：Tx.ID() 返回该修订，removeTx 按该
	// 修订 ID 解除只读水位登记。
	t.meta.SetTxid(entry.txid)
	t.meta.SetRootBucket(common.NewInBucket(entry.root, entry.sequence))
	*t.root.InBucket = *t.meta.RootBucket()

	if db.freelist != nil {
		db.freelist.AddReadonlyTXID(entry.txid)
	}

	db.metalock.Unlock()

	if db.stats != nil {
		db.statlock.Lock()
		db.stats.TxN++
		db.stats.OpenTxN++
		db.statlock.Unlock()
	}

	return t, nil
}
