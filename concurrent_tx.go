package bbolt

import (
	"bytes"
	"encoding/hex"
	"errors"
	"strings"

	berrors "go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/common"
)

// ErrConcurrentTransactionConflict 在并发写事务提交时返回，表示本事务开始
// 之后已有其他事务提交了相互冲突的修改（同一个键被改写、桶被删除或重建、
// 桶内序列号被推进等）。收到该错误的事务不会写入任何数据，调用方应当在最新
// 已提交状态上重新执行事务逻辑后再次提交。
var ErrConcurrentTransactionConflict = errors.New("concurrent transaction conflict")

// errConcurrentMoveBucketUnsupported 表示并发写事务暂不支持 MoveBucket 操作。
var errConcurrentMoveBucketUnsupported = errors.New("MoveBucket is not supported within a concurrent transaction")

// BeginConcurrent 开始一个可选的并发写事务。
//
// 与 Begin(true) 不同，多个 BeginConcurrent 事务可以同时存在并各自执行写操作。
// 每个事务基于开始时刻的数据库快照工作，在 Commit 之前互不阻塞，也不会产生
// 任何磁盘写入：
//
//   - 修改不同键、不同桶的事务可以按任意顺序提交，双方结果都会保留；
//   - 修改同一个键，或者桶结构相互冲突（同名建桶、删桶与写桶并发等）时，
//     后提交的一方会收到 ErrConcurrentTransactionConflict，已提交数据不会被
//     静默覆盖；
//   - 任意一方回滚都不会影响其他事务；
//   - 提交成功后新开启的只读事务可以看到已提交结果，而提交前开启的只读事务
//     仍然只看到自己的快照。
//
// 并发写事务可以与普通串行写事务（Begin(true)/Update）交错使用。
//
// 并发写事务自身不会阻塞数据库的 mmap 扩容；但若同时存在长期打开的普通只读
// 事务，写事务提交时仍可能因等待扩容而阻塞（与 Begin(true) 的既有行为相同），
// 这种场景建议设置 Options.InitialMmapSize 预留空间。
//
// 事务必须通过 Commit 或 Rollback 显式结束，这一点与普通事务相同。
func (db *DB) BeginConcurrent() (*Tx, error) {
	if db.readOnly {
		return nil, berrors.ErrDatabaseReadOnly
	}

	db.concurrentTxns.Add(1)

	// 完全按只读事务的方式建立事务：在 freelist 中登记只读 txid 以保护
	// 快照页不被回收，并且不修改任何 DB 级别的共享写状态。
	t, err := db.beginTx()
	if err != nil {
		db.concurrentTxns.Done()
		return nil, err
	}

	// 仅在内存中把事务提升为可写：所有写入都进入该事务私有的 bucket/node
	// 缓存，不会分配页面、不会触碰 freelist，直到提交时才串行化重放。
	t.writable = true
	if t.root.buckets == nil {
		t.root.buckets = make(map[string]*Bucket)
	}
	if t.root.nodes == nil {
		t.root.nodes = make(map[common.Pgid]*node)
	}
	t.concurrent = newConcurrentTxnState()

	// 提前释放 mmap 读锁。并发写事务可能长期同时存在，若像普通只读事务那样
	// 一直持有读锁，任一事务提交时的数据扩容（重新 mmap）都会被其他未关闭
	// 的并发事务阻塞而死锁。快照页内容由 freelist 中的只读 txid 登记保护，
	// 不会被复用或覆盖；该事务对 mmap 的所有读取都会先复制到堆上，因此也
	// 不依赖映射地址保持稳定。
	db.mmaplock.RUnlock()

	return t, nil
}

// ccOpKind 标识并发写事务记录的逻辑操作类型。
type ccOpKind int

const (
	ccOpPut ccOpKind = iota + 1
	ccOpDelete
	ccOpCreateBucket
	ccOpCreateBucketIfNotExists
	ccOpDeleteBucket
	ccOpSequence
)

// ccOp 是对一次逻辑写操作的记录。提交时先基于最新已提交状态逐条校验，再按
// 记录顺序重放到一个普通串行写事务中。
type ccOp struct {
	kind ccOpKind

	// path 是父桶（sequence 操作时为桶自身）的完整路径编码，"" 表示根桶。
	path     string
	pathSegs [][]byte

	// key 是叶子键或桶名（已复制，独立于 mmap/调用方切片）。
	key []byte

	// put/delete：本事务第一次修改该键之前观察到的状态。
	obsPresent bool
	observed   []byte

	// put：最终要写入的值。
	value []byte

	// create 类操作是否在本事务中实际新建了桶。
	local bool
	// deleteBucket 删除的是否为本事务自己创建的桶（无需快照身份校验）。
	localBucket bool
	// deleteBucket/create 是否构成“先删后建”的一对操作。
	replaced bool
	replaces bool

	// deleteBucket：删除前桶的身份。非内联桶比较根页 ID；内联桶比较其在
	// 父桶叶子中的原始字节。
	snapRoot   common.Pgid
	snapInline []byte

	// sequence：开始推进前观察到的序列号与最终序列号。
	snapSeq  uint64
	finalSeq uint64
}

// concurrentTxnState 保存单个并发写事务的操作日志与辅助索引。
type concurrentTxnState struct {
	// all 按调用顺序保存所有操作（键级写操作为去重后保留首次位置的指针）。
	all []*ccOp

	// writes 以“桶路径 + 键”为索引，用于同一键多次修改时就地合并。
	writes map[string]*ccOp
	// seqs 以桶路径为索引，合并同桶的多次序列号推进。
	seqs map[string]*ccOp
	// localBuckets 记录本事务中新建桶的完整路径。
	localBuckets map[string]bool
	// deletedTargets 记录本事务中删除桶操作，键为目标桶完整路径。
	deletedTargets map[string]*ccOp
}

func newConcurrentTxnState() *concurrentTxnState {
	return &concurrentTxnState{
		writes:         make(map[string]*ccOp),
		seqs:           make(map[string]*ccOp),
		localBuckets:   make(map[string]bool),
		deletedTargets: make(map[string]*ccOp),
	}
}

// ccEncodePath 把桶路径编码为可作为 map 键的字符串，根桶编码为空串。
// 每个路径段先做十六进制编码再拼接，因此可以容纳任意字节。
func ccEncodePath(segments [][]byte) string {
	if len(segments) == 0 {
		return ""
	}
	var sb strings.Builder
	for i, seg := range segments {
		if i > 0 {
			sb.WriteByte('/')
		}
		sb.WriteString(hex.EncodeToString(seg))
	}
	return sb.String()
}

// ccItemKey 返回“某桶内某个键”的全局索引键。
func ccItemKey(parentSegs [][]byte, key []byte) string {
	return ccEncodePath(parentSegs) + "\x00" + hex.EncodeToString(key)
}

// ccChildPath 返回父桶下某个子桶的完整路径编码。
func ccChildPath(parentSegs [][]byte, name []byte) string {
	segs := make([][]byte, 0, len(parentSegs)+1)
	segs = append(segs, parentSegs...)
	segs = append(segs, name)
	return ccEncodePath(segs)
}

func cloneSegments(segments [][]byte) [][]byte {
	if len(segments) == 0 {
		return nil
	}
	out := make([][]byte, len(segments))
	for i, seg := range segments {
		out[i] = cloneBytes(seg)
	}
	return out
}

// recordKeyValue 记录一次 Put 或 Delete。
//
// existed/isBucket/observed 表示本次调用生效前该键在本事务视图中的状态；
// 同一键的多次修改只保留第一次观察到的快照状态和最后一次的最终意图。
func (st *concurrentTxnState) recordKeyValue(b *Bucket, key, observed []byte, kind ccOpKind, existed, isBucket bool, finalValue []byte) {
	itemKey := ccItemKey(b.ccPath, key)
	if op, ok := st.writes[itemKey]; ok {
		op.kind = kind
		if kind == ccOpPut {
			op.value = cloneBytes(finalValue)
		}
		return
	}

	op := &ccOp{
		kind:       kind,
		path:       ccEncodePath(b.ccPath),
		pathSegs:   cloneSegments(b.ccPath),
		key:        cloneBytes(key),
		obsPresent: existed && !isBucket,
	}
	if op.obsPresent {
		op.observed = cloneBytes(observed)
	}
	if kind == ccOpPut {
		op.value = cloneBytes(finalValue)
	}

	st.writes[itemKey] = op
	st.all = append(st.all, op)
}

// recordCreateBucket 记录一次建桶操作。
func (st *concurrentTxnState) recordCreateBucket(b *Bucket, name []byte, ifNotExists bool) {
	kind := ccOpCreateBucket
	if ifNotExists {
		kind = ccOpCreateBucketIfNotExists
	}
	op := &ccOp{
		kind:     kind,
		path:     ccEncodePath(b.ccPath),
		pathSegs: cloneSegments(b.ccPath),
		key:      cloneBytes(name),
		local:    true,
	}

	// 与“先删后建”配对：两个操作都需要放宽为“目标不存在或仍是桶”。
	target := ccChildPath(b.ccPath, name)
	if dop, ok := st.deletedTargets[target]; ok {
		dop.replaced = true
		op.replaces = true
	}

	st.localBuckets[target] = true
	st.all = append(st.all, op)
}

// recordDeleteBucket 记录一次删桶操作，并捕获桶删除前的身份。
func (st *concurrentTxnState) recordDeleteBucket(b *Bucket, name []byte, child *Bucket, inlineValue []byte) {
	target := ccChildPath(b.ccPath, name)
	op := &ccOp{
		kind:        ccOpDeleteBucket,
		path:        ccEncodePath(b.ccPath),
		pathSegs:    cloneSegments(b.ccPath),
		key:         cloneBytes(name),
		localBucket: st.localBuckets[target],
		snapRoot:    child.RootPage(),
	}
	if !op.localBucket && op.snapRoot == 0 {
		// 删除前是内联桶：保存其在父桶叶子中的原始字节，提交时逐字节比较。
		op.snapInline = cloneBytes(inlineValue)
	}

	delete(st.localBuckets, target)
	st.deletedTargets[target] = op
	st.all = append(st.all, op)
}

// recordSetSequence 记录一次显式设置序列号的操作。
func (st *concurrentTxnState) recordSetSequence(b *Bucket, v uint64) {
	path := ccEncodePath(b.ccPath)
	if op, ok := st.seqs[path]; ok {
		op.finalSeq = v
		return
	}
	op := &ccOp{
		kind:     ccOpSequence,
		path:     path,
		pathSegs: cloneSegments(b.ccPath),
		snapSeq:  b.Sequence(),
		finalSeq: v,
	}
	st.seqs[path] = op
	st.all = append(st.all, op)
}

// recordNextSequence 记录一次推进序列号的操作。
func (st *concurrentTxnState) recordNextSequence(b *Bucket) {
	path := ccEncodePath(b.ccPath)
	if op, ok := st.seqs[path]; ok {
		op.finalSeq++
		return
	}
	seq := b.Sequence()
	op := &ccOp{
		kind:     ccOpSequence,
		path:     path,
		pathSegs: cloneSegments(b.ccPath),
		snapSeq:  seq,
		finalSeq: seq + 1,
	}
	st.seqs[path] = op
	st.all = append(st.all, op)
}

// commitConcurrent 实现并发写事务的提交流程：
//  1. 以只读事务的关闭路径摘除私有快照（释放 mmap 读锁并注销只读 txid）；
//  2. 通过普通 Begin(true) 与所有写者串行，在最新已提交状态上校验冲突；
//  3. 无冲突时按原顺序把逻辑操作重放到串行事务并正常落盘；
//  4. 仅在重放事务提交成功后执行一次提交回调。
func (tx *Tx) commitConcurrent() error {
	st := tx.concurrent
	db := tx.db
	handlers := tx.commitHandlers

	// 摘除私有快照：注销 freelist 中的只读 txid 并清空事务引用。mmap 读锁
	// 已在 BeginConcurrent 时提前释放，这里不能再走普通只读事务的 close。
	tx.discardConcurrent()

	// 在最新已提交状态上开启普通串行写事务；这一步天然与 Begin(true)/Update
	// 以及其他并发事务的重放互斥。
	rt, err := db.Begin(true)
	if err != nil {
		db.concurrentTxns.Done()
		return err
	}

	if err = st.validate(rt); err != nil {
		_ = rt.Rollback()
		db.concurrentTxns.Done()
		return err
	}
	if err = st.replay(rt); err != nil {
		_ = rt.Rollback()
		db.concurrentTxns.Done()
		return err
	}
	if err = rt.Commit(); err != nil {
		db.concurrentTxns.Done()
		return err
	}
	db.concurrentTxns.Done()

	// 数据已持久化，提交回调恰好执行一次；回滚或冲突路径不会执行回调。
	for _, fn := range handlers {
		fn()
	}
	return nil
}

// rollbackConcurrent 放弃并发写事务的全部修改，仅摘除其私有快照登记。
func (tx *Tx) rollbackConcurrent() {
	db := tx.db
	tx.discardConcurrent()
	db.concurrentTxns.Done()
}

// discardConcurrent 注销并发事务所持的只读快照并清空事务引用。它不触碰
// mmap 读锁（读锁在 BeginConcurrent 成功后已立即释放），也不触碰写锁与
// freelist 的写状态。
func (tx *Tx) discardConcurrent() {
	db := tx.db

	db.metalock.Lock()
	if db.freelist != nil {
		db.freelist.RemoveReadonlyTXID(tx.meta.Txid())
	}
	db.metalock.Unlock()

	if db.stats != nil {
		db.statlock.Lock()
		db.stats.OpenTxN--
		db.stats.TxStats.add(&tx.stats)
		db.statlock.Unlock()
	}

	tx.db = nil
	tx.meta = nil
	tx.root = Bucket{tx: tx}
	tx.pages = nil
	tx.concurrent = nil
}

// validate 基于最新已提交状态检查所有操作是否仍可应用。任一前提不再成立都
// 返回 ErrConcurrentTransactionConflict。
func (st *concurrentTxnState) validate(rt *Tx) error {
	for _, op := range st.all {
		if err := op.validate(rt, st); err != nil {
			return err
		}
	}
	return nil
}

// replay 按记录顺序把全部操作重放到串行写事务中。
func (st *concurrentTxnState) replay(rt *Tx) error {
	for _, op := range st.all {
		if err := op.replay(rt); err != nil {
			return err
		}
	}
	return nil
}

// isLocalBucket 判断某条桶路径是否由本事务自己创建。
func (st *concurrentTxnState) isLocalBucket(path string) bool {
	return st.localBuckets[path]
}

// ccOpenPath 按路径打开桶，根桶路径返回根桶；任一层级不存在则返回 nil。
func ccOpenPath(tx *Tx, segs [][]byte) *Bucket {
	if len(segs) == 0 {
		return &tx.root
	}
	b := tx.Bucket(segs[0])
	if b == nil {
		return nil
	}
	for _, seg := range segs[1:] {
		b = b.Bucket(seg)
		if b == nil {
			return nil
		}
	}
	return b
}

// ccProbe 判断键在桶中的当前状态：是否存在、是否为桶、以及普通值内容。
func ccProbe(b *Bucket, key []byte) (present, isBucket bool, value []byte) {
	k, v, flags := b.Cursor().seek(key)
	if !bytes.Equal(k, key) {
		return false, false, nil
	}
	if flags&common.BucketLeafFlag != 0 {
		return true, true, nil
	}
	return true, false, v
}

// ccRawBucketValue 返回桶在父桶叶子中保存的原始内容（桶头 + 内联页）。
func ccRawBucketValue(parent *Bucket, name []byte) []byte {
	k, v, flags := parent.Cursor().seek(name)
	if !bytes.Equal(k, name) || flags&common.BucketLeafFlag == 0 {
		return nil
	}
	return v
}

func (op *ccOp) validate(rt *Tx, st *concurrentTxnState) error {
	switch op.kind {
	case ccOpPut, ccOpDelete:
		// 桶由本事务自己创建时，键在快照中必然不存在，无需对照已提交状态。
		if st.isLocalBucket(op.path) {
			return nil
		}
		parent := ccOpenPath(rt, op.pathSegs)
		if parent == nil {
			return ErrConcurrentTransactionConflict
		}
		present, isBucket, v := ccProbe(parent, op.key)
		if isBucket || present != op.obsPresent {
			return ErrConcurrentTransactionConflict
		}
		if present && !bytes.Equal(v, op.observed) {
			return ErrConcurrentTransactionConflict
		}

	case ccOpCreateBucket, ccOpCreateBucketIfNotExists:
		parent := ccOpenPath(rt, op.pathSegs)
		if parent == nil {
			return ErrConcurrentTransactionConflict
		}
		present, isBucket, _ := ccProbe(parent, op.key)
		switch {
		case op.replaces:
			// “先删后建”：目标当前不存在或仍是桶才允许。
			if present && !isBucket {
				return ErrConcurrentTransactionConflict
			}
		case op.local:
			// 本事务中实际新建的桶：快照中不存在，现在也必须不存在。
			if present {
				return ErrConcurrentTransactionConflict
			}
		default:
			// CreateBucketIfNotExists 复用了快照中已存在的桶：该桶现在仍
			// 必须以桶的形式存在（被删除或被普通键占用都属于冲突）。
			if !present || !isBucket {
				return ErrConcurrentTransactionConflict
			}
		}

	case ccOpDeleteBucket:
		if op.localBucket {
			return nil
		}
		parent := ccOpenPath(rt, op.pathSegs)
		if parent == nil {
			return ErrConcurrentTransactionConflict
		}
		present, isBucket, _ := ccProbe(parent, op.key)
		if !present || !isBucket {
			return ErrConcurrentTransactionConflict
		}
		if op.replaced {
			return nil
		}
		child := parent.Bucket(op.key)
		if child == nil {
			return ErrConcurrentTransactionConflict
		}
		if op.snapRoot != 0 {
			// 非内联桶只要在快照之后发生过任意 COW 重写（增删改键、推进
			// 序列号、桶结构变化等），根页 ID 必然改变。
			if child.RootPage() != op.snapRoot {
				return ErrConcurrentTransactionConflict
			}
		} else {
			// 内联桶：桶头+内联页逐字节比较；一旦溢出为多页桶也立即冲突。
			if child.RootPage() != 0 {
				return ErrConcurrentTransactionConflict
			}
			if !bytes.Equal(ccRawBucketValue(parent, op.key), op.snapInline) {
				return ErrConcurrentTransactionConflict
			}
		}

	case ccOpSequence:
		if st.isLocalBucket(op.path) {
			return nil
		}
		b := ccOpenPath(rt, op.pathSegs)
		if b == nil || b.Sequence() != op.snapSeq {
			return ErrConcurrentTransactionConflict
		}
	}

	return nil
}

func (op *ccOp) replay(rt *Tx) error {
	switch op.kind {
	case ccOpPut:
		parent := ccOpenPath(rt, op.pathSegs)
		if parent == nil {
			return ErrConcurrentTransactionConflict
		}
		return parent.Put(op.key, op.value)

	case ccOpDelete:
		parent := ccOpenPath(rt, op.pathSegs)
		if parent == nil {
			return ErrConcurrentTransactionConflict
		}
		return parent.Delete(op.key)

	case ccOpCreateBucket:
		parent := ccOpenPath(rt, op.pathSegs)
		if parent == nil {
			return ErrConcurrentTransactionConflict
		}
		_, err := parent.CreateBucket(op.key)
		return err

	case ccOpCreateBucketIfNotExists:
		parent := ccOpenPath(rt, op.pathSegs)
		if parent == nil {
			return ErrConcurrentTransactionConflict
		}
		_, err := parent.CreateBucketIfNotExists(op.key)
		return err

	case ccOpDeleteBucket:
		parent := ccOpenPath(rt, op.pathSegs)
		if parent == nil {
			return ErrConcurrentTransactionConflict
		}
		if op.replaced {
			// 与后续建桶配对时，目标可能已被本事务早先的操作删除。
			if present, isBucket, _ := ccProbe(parent, op.key); !present {
				return nil
			} else if !isBucket {
				return ErrConcurrentTransactionConflict
			}
		}
		return parent.DeleteBucket(op.key)

	case ccOpSequence:
		b := ccOpenPath(rt, op.pathSegs)
		if b == nil {
			return ErrConcurrentTransactionConflict
		}
		return b.SetSequence(op.finalSeq)
	}

	return nil
}
