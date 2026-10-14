package bbolt_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
)

// histOptions 构造开启历史修订保留的测试选项，并沿用测试环境指定的
// freelist 类型（array/hashmap），从而在 Makefile 的两套测试下都被覆盖。
func histOptions(limit int) *bolt.Options {
	freelistType := bolt.FreelistArrayType
	if os.Getenv(btesting.TestFreelistType) == string(bolt.FreelistMapType) {
		freelistType = bolt.FreelistMapType
	}
	return &bolt.Options{
		FreelistType:    freelistType,
		HistoryLimit:    limit,
		InitialMmapSize: 1 << 20,
	}
}

func mustOpenHist(t *testing.T, path string, limit int, modify func(*bolt.Options)) *bolt.DB {
	t.Helper()
	opts := histOptions(limit)
	if modify != nil {
		modify(opts)
	}
	db, err := bolt.Open(path, 0600, opts)
	require.NoError(t, err)
	return db
}

// putRev 在根桶 bucket 写入一对键值，返回该成功提交的修订 ID。
func putRev(t *testing.T, db *bolt.DB, bucket, key, value string) int {
	t.Helper()
	id := 0
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		if err := b.Put([]byte(key), []byte(value)); err != nil {
			return err
		}
		id = tx.ID()
		return nil
	}))
	require.NotZero(t, id)
	return id
}

func viewAtGet(t *testing.T, db *bolt.DB, rev int, bucket, key string) (string, bool) {
	t.Helper()
	var val string
	var ok bool
	require.NoError(t, db.ViewAt(rev, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return nil
		}
		v := b.Get([]byte(key))
		if v != nil {
			val = string(v)
			ok = true
		}
		return nil
	}))
	return val, ok
}

func dumpBucketTree(b *bolt.Bucket, prefix string, out map[string]string) {
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if v == nil {
			if child := b.Bucket(k); child != nil {
				dumpBucketTree(child, prefix+string(k)+"/", out)
			}
		} else {
			out[prefix+string(k)] = string(v)
		}
	}
}

func viewAtDump(t *testing.T, db *bolt.DB, rev int, roots ...string) map[string]string {
	t.Helper()
	out := make(map[string]string)
	require.NoError(t, db.ViewAt(rev, func(tx *bolt.Tx) error {
		for _, name := range roots {
			if b := tx.Bucket([]byte(name)); b != nil {
				dumpBucketTree(b, name+"/", out)
			}
		}
		return nil
	}))
	return out
}

func viewDump(t *testing.T, db *bolt.DB, roots ...string) map[string]string {
	t.Helper()
	out := make(map[string]string)
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		for _, name := range roots {
			if b := tx.Bucket([]byte(name)); b != nil {
				dumpBucketTree(b, name+"/", out)
			}
		}
		return nil
	}))
	return out
}

func mustCheckDB(t *testing.T, db *bolt.DB) {
	t.Helper()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		var errs []error
		for err := range tx.Check() {
			errs = append(errs, err)
		}
		if len(errs) > 0 {
			return fmt.Errorf("consistency check failed: %v", errs)
		}
		return nil
	}))
}

// 未启用 HistoryLimit 时，ViewAt 返回 ErrHistoryDisabled 且不执行回调。
func TestHistory_Disabled(t *testing.T) {
	db := btesting.MustCreateDB(t) // 默认选项，HistoryLimit=0

	called := false
	err := db.ViewAt(1, func(tx *bolt.Tx) error {
		called = true
		return nil
	})
	require.ErrorIs(t, err, bolt.ErrHistoryDisabled)
	require.False(t, called)

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		_, e := tx.CreateBucket([]byte("b"))
		return e
	}))
}

// 负数 HistoryLimit 在 Open 阶段直接报错。
func TestHistory_NegativeLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db, err := bolt.Open(path, 0600, &bolt.Options{HistoryLimit: -1})
	require.Error(t, err)
	if db != nil {
		_ = db.Close()
	}
}

// 基础修订读取：键值、嵌套桶、游标遍历、空提交与 Tx.ID() 一致性。
func TestHistory_BasicRevisions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db := mustOpenHist(t, path, 8, nil)
	defer func() { _ = db.Close() }()

	type snapshot = map[string]string
	snaps := make(map[int]snapshot)

	// r1：顶层桶 app 写入 k1。
	r1 := putRev(t, db, "app", "k1", "v1")
	snaps[r1] = snapshot{"app/k1": "v1"}

	// r2：创建嵌套桶 sec1。
	r2 := 0
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		app, err := tx.Bucket([]byte("app")).CreateBucket([]byte("sec1"))
		if err != nil {
			return err
		}
		if err := app.Put([]byte("a"), []byte("1")); err != nil {
			return err
		}
		k1v := append([]byte(nil), tx.Bucket([]byte("app")).Get([]byte("k1"))...)
		r2 = tx.ID()
		snaps[r2] = snapshot{"app/k1": string(k1v), "app/sec1/a": "1"}
		return nil
	}))

	// r3：根键更新 + 嵌套桶新增键。
	r3 := 0
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		app := tx.Bucket([]byte("app"))
		if err := app.Put([]byte("k1"), []byte("v2")); err != nil {
			return err
		}
		if err := app.Bucket([]byte("sec1")).Put([]byte("b"), []byte("2")); err != nil {
			return err
		}
		r3 = tx.ID()
		snaps[r3] = snapshot{"app/k1": "v2", "app/sec1/a": "1", "app/sec1/b": "2"}
		return nil
	}))

	// r4：空提交同样推进修订，视图与 r3 一致。
	r4 := 0
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		r4 = tx.ID()
		return nil
	}))
	snaps[r4] = snaps[r3]

	// r5：删除嵌套桶。
	r5 := 0
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket([]byte("app")).DeleteBucket([]byte("sec1")); err != nil {
			return err
		}
		r5 = tx.ID()
		snaps[r5] = snapshot{"app/k1": "v2"}
		return nil
	}))

	// r6：再次更新。
	r6 := putRev(t, db, "app", "k1", "v3")
	snaps[r6] = snapshot{"app/k1": "v3"}

	for rev, want := range snaps {
		got := viewAtDump(t, db, rev, "app")
		require.Equal(t, want, got, "revision %d snapshot mismatch", rev)
	}

	// 游标遍历旧修订：r3 上 sec1 的两个键都要按序可见。
	require.NoError(t, db.ViewAt(r3, func(tx *bolt.Tx) error {
		sec := tx.Bucket([]byte("app")).Bucket([]byte("sec1"))
		require.NotNil(t, sec)
		c := sec.Cursor()
		var keys []string
		for k, v := c.First(); k != nil; k, v = c.Next() {
			require.NotNil(t, v)
			keys = append(keys, string(k))
		}
		require.Equal(t, []string{"a", "b"}, keys)
		return nil
	}))

	// r5 上嵌套桶已不存在。
	require.NoError(t, db.ViewAt(r5, func(tx *bolt.Tx) error {
		require.Nil(t, tx.Bucket([]byte("app")).Bucket([]byte("sec1")))
		return nil
	}))

	// 当前视图与最新修订一致。
	require.Equal(t, snaps[r6], viewDump(t, db, "app"))
	mustCheckDB(t, db)
}

// 窗口淘汰：超出保留范围的旧修订返回 ErrRevisionExpired；
// 无效 / 未来 ID 分别报错，且回调不执行。
func TestHistory_WindowEviction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db := mustOpenHist(t, path, 2, nil)
	defer func() { _ = db.Close() }()

	var revs []int
	for i := 0; i < 6; i++ {
		revs = append(revs, putRev(t, db, "b", "k", fmt.Sprintf("v%d", i)))
	}

	for _, rev := range revs[:len(revs)-2] {
		called := false
		err := db.ViewAt(rev, func(tx *bolt.Tx) error {
			called = true
			return nil
		})
		require.ErrorIs(t, err, bolt.ErrRevisionExpired, "rev %d", rev)
		require.False(t, called)
	}
	for _, rev := range revs[len(revs)-2:] {
		_, ok := viewAtGet(t, db, rev, "b", "k")
		require.True(t, ok)
	}

	latest := revs[len(revs)-1]
	expectNoCallback := func(id int, target error) {
		called := false
		err := db.ViewAt(id, func(tx *bolt.Tx) error {
			called = true
			return nil
		})
		require.ErrorIs(t, err, target)
		require.False(t, called)
	}
	expectNoCallback(0, bolt.ErrRevisionInvalid)
	expectNoCallback(-7, bolt.ErrRevisionInvalid)
	expectNoCallback(latest+1, bolt.ErrRevisionFuture)
	expectNoCallback(1<<30, bolt.ErrRevisionFuture)

	// 保留范围内的内容仍然精确。
	v, ok := viewAtGet(t, db, latest, "b", "k")
	require.True(t, ok)
	require.Equal(t, "v5", v)
}

// 回滚或失败写入不推进修订窗口。
func TestHistory_RollbackDoesNotAdvance(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db := mustOpenHist(t, path, 8, nil)
	defer func() { _ = db.Close() }()

	r1 := putRev(t, db, "b", "k", "v1")

	// Update 回调返回错误：事务回滚，ID 不前进。
	failedID := 0
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("b"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte("k"), []byte("v2-bad")))
		failedID = tx.ID()
		return fmt.Errorf("boom")
	})
	require.ErrorContains(t, err, "boom")
	require.Equal(t, r1+1, failedID)

	// 失败时刻的 ID 还不是成功修订。
	require.ErrorIs(t, db.ViewAt(failedID, func(tx *bolt.Tx) error { return nil }), bolt.ErrRevisionFuture)
	var cur int
	require.NoError(t, db.View(func(tx *bolt.Tx) error { cur = tx.ID(); return nil }))
	require.Equal(t, r1, cur)

	// 手动 Begin + Rollback 同样不推进。
	tx, err := db.Begin(true)
	require.NoError(t, err)
	require.NoError(t, tx.Bucket([]byte("b")).Put([]byte("k2"), []byte("x")))
	require.NoError(t, tx.Rollback())
	require.NoError(t, db.View(func(tx *bolt.Tx) error { cur = tx.ID(); return nil }))
	require.Equal(t, r1, cur)

	// 下一次成功提交复用失败事务的 ID；该 ID 上只能读到成功后的内容。
	r2 := putRev(t, db, "b", "k", "v2-good")
	require.Equal(t, failedID, r2)
	v, ok := viewAtGet(t, db, r2, "b", "k")
	require.True(t, ok)
	require.Equal(t, "v2-good", v)
	_, hasK2 := viewAtGet(t, db, r2, "b", "k2")
	require.False(t, hasK2)

	v, ok = viewAtGet(t, db, r1, "b", "k")
	require.True(t, ok)
	require.Equal(t, "v1", v)
	mustCheckDB(t, db)
}

// 关闭重开后，保留范围内的修订（含嵌套桶）仍可读取；窗口继续正常滑动。
func TestHistory_PersistAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")

	db := mustOpenHist(t, path, 3, nil)
	var revs []int
	for i := 0; i < 6; i++ {
		id := 0
		require.NoError(t, db.Update(func(tx *bolt.Tx) error {
			app, err := tx.CreateBucketIfNotExists([]byte("app"))
			if err != nil {
				return err
			}
			if i >= 2 {
				sec, err := app.CreateBucketIfNotExists([]byte("sec"))
				if err != nil {
					return err
				}
				if err := sec.Put([]byte("k"), []byte(strconv.Itoa(i))); err != nil {
					return err
				}
			}
			return app.Put([]byte("k1"), []byte(fmt.Sprintf("v%d", i)))
		}))
		require.NoError(t, db.View(func(tx *bolt.Tx) error { id = tx.ID(); return nil }))
		revs = append(revs, id)
	}
	mustCheckDB(t, db)
	require.NoError(t, db.Close())

	// 重开：只剩最近 3 个修订。
	db = mustOpenHist(t, path, 3, nil)
	require.ErrorIs(t, db.ViewAt(revs[2], func(tx *bolt.Tx) error { return nil }), bolt.ErrRevisionExpired)
	for _, rev := range revs[3:] {
		var k1, sec string
		require.NoError(t, db.ViewAt(rev, func(tx *bolt.Tx) error {
			app := tx.Bucket([]byte("app"))
			k1 = string(app.Get([]byte("k1")))
			if b := app.Bucket([]byte("sec")); b != nil {
				sec = string(b.Get([]byte("k")))
			}
			return nil
		}))
		idx := rev - revs[0]
		require.Equal(t, fmt.Sprintf("v%d", idx), k1, "rev %d", rev)
		require.Equal(t, strconv.Itoa(idx), sec, "rev %d", rev)
	}

	// 重开后继续提交，窗口继续前移且历史仍然可读。
	r7 := putRev(t, db, "app", "k1", "v7")
	require.ErrorIs(t, db.ViewAt(revs[3], func(tx *bolt.Tx) error { return nil }), bolt.ErrRevisionExpired)
	v, _ := viewAtGet(t, db, r7, "app", "k1")
	require.Equal(t, "v7", v)
	mustCheckDB(t, db)
	require.NoError(t, db.Close())

	// 再次重开，最近三个修订（revs[5], r6?, r7）仍在。
	db = mustOpenHist(t, path, 3, nil)
	v, _ = viewAtGet(t, db, r7, "app", "k1")
	require.Equal(t, "v7", v)
	mustCheckDB(t, db)
}

// 大窗口重开后所有修订均可读。
func TestHistory_ReopenRetainsAll(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db := mustOpenHist(t, path, 20, nil)

	var revs []int
	for i := 0; i < 7; i++ {
		revs = append(revs, putRev(t, db, "b", "k", fmt.Sprintf("v%d", i)))
	}
	require.NoError(t, db.Close())

	db = mustOpenHist(t, path, 20, nil)
	defer func() { _ = db.Close() }()
	for i, rev := range revs {
		v, ok := viewAtGet(t, db, rev, "b", "k")
		require.True(t, ok, "rev %d", rev)
		require.Equal(t, fmt.Sprintf("v%d", i), v)
	}
}

// 存储按变化页与有限窗口增长，而不是每次小更新复制整库。
func TestHistory_IncrementalStorage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	modify := func(o *bolt.Options) {
		o.PageSize = 4096
		o.InitialMmapSize = 1 << 20
	}

	// N=1：预热两轮后，后续单键更新不应再抬高高水位（页在相邻修订间循环复用）。
	db := mustOpenHist(t, path, 1, modify)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("seed"))
		if err != nil {
			return err
		}
		for i := 0; i < 200; i++ {
			if err := b.Put([]byte(fmt.Sprintf("key-%04d", i)), make([]byte, 256)); err != nil {
				return err
			}
		}
		return nil
	}))
	putRev(t, db, "seed", "zz:warm", "w1")
	putRev(t, db, "seed", "zz:warm", "w2")

	dbSize := func() int64 {
		var s int64
		require.NoError(t, db.View(func(tx *bolt.Tx) error { s = tx.Size(); return nil }))
		return s
	}
	stable := dbSize()
	for i := 0; i < 6; i++ {
		putRev(t, db, "seed", "zz:mut", fmt.Sprintf("m%d", i))
		require.Equal(t, stable, dbSize(), "high water mark moved at mutation %d with limit=1", i)
	}
	mustCheckDB(t, db)
	require.NoError(t, db.Close())

	// N=3：30 次小更新后，增长应被限制在少量变化页（若干数据页 +
	// 每页一个索引页/页日志页）之内，而非整库复制。
	path2 := filepath.Join(t.TempDir(), "db2")
	db = mustOpenHist(t, path2, 3, modify)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("seed"))
		if err != nil {
			return err
		}
		for i := 0; i < 200; i++ {
			if err := b.Put([]byte(fmt.Sprintf("key-%04d", i)), make([]byte, 256)); err != nil {
				return err
			}
		}
		return nil
	}))
	base := dbSize()
	for i := 0; i < 30; i++ {
		putRev(t, db, "seed", fmt.Sprintf("key-%04d", i%200), fmt.Sprintf("u%0250d", i))
	}
	grown := dbSize()
	slackPages := int64(32) // 3 个修订的数据页 + 索引/日志/freelist 页
	require.LessOrEqual(t, grown-base, slackPages*4096,
		"db grew by %d bytes (base %d), expected incremental page growth", grown-base, base)
	mustCheckDB(t, db)
}

// 已经开始的历史读取，在后续提交和窗口前移过程中保持稳定。
func TestHistory_StableReadAcrossAdvancingWindow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db := mustOpenHist(t, path, 2, nil)
	defer func() { _ = db.Close() }()

	r1 := putRev(t, db, "b", "k", "v1")
	putRev(t, db, "b", "k", "v2")

	started := make(chan struct{})
	proceed := make(chan struct{})
	viewDone := make(chan error, 1)

	go func() {
		viewDone <- db.ViewAt(r1, func(tx *bolt.Tx) error {
			before := make(map[string]string)
			dumpBucketTree(tx.Bucket([]byte("b")), "b/", before)
			close(started)
			<-proceed // 持有历史读事务，等待外部把窗口推过 r1
			after := make(map[string]string)
			dumpBucketTree(tx.Bucket([]byte("b")), "b/", after)
			require.Equal(t, before, after)
			return nil
		})
	}()

	<-started
	for i := 0; i < 8; i++ {
		putRev(t, db, "b", "k", fmt.Sprintf("later-%d", i))
	}
	close(proceed)
	require.NoError(t, <-viewDone)

	// r1 已经离开窗口，但读取期间看到的始终是 v1。
	require.ErrorIs(t, db.ViewAt(r1, func(tx *bolt.Tx) error { return nil }), bolt.ErrRevisionExpired)
}

// NoFreelistSync 场景下历史修订同样可重开读取。
func TestHistory_NoFreelistSync(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	opts := histOptions(4)
	opts.NoFreelistSync = true
	db, err := bolt.Open(path, 0600, opts)
	require.NoError(t, err)

	var revs []int
	for i := 0; i < 5; i++ {
		revs = append(revs, putRev(t, db, "b", "k", fmt.Sprintf("v%d", i)))
	}
	mustCheckDB(t, db)
	require.NoError(t, db.Close())

	db = mustOpenHist(t, path, 4, func(o *bolt.Options) { o.NoFreelistSync = true })
	for i, rev := range revs[1:] {
		v, ok := viewAtGet(t, db, rev, "b", "k")
		require.True(t, ok)
		require.Equal(t, fmt.Sprintf("v%d", i+1), v)
	}
	putRev(t, db, "b", "k", "v5")
	mustCheckDB(t, db)
	require.NoError(t, db.Close())

	// 切换回同步 freelist 打开（Open 会自动补一次 freelist 落盘）。
	db = mustOpenHist(t, path, 4, nil)
	v, _ := viewAtGet(t, db, revs[len(revs)-1], "b", "k")
	require.Equal(t, "v4", v)
	putRev(t, db, "b", "k", "v6")
	mustCheckDB(t, db)
	require.NoError(t, db.Close())
}

// 关闭历史保留后，旧页被回收且当前数据一致；再次开启时从当前修订重新起窗。
func TestHistory_TurnOff(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db := mustOpenHist(t, path, 3, nil)
	var revs []int
	for i := 0; i < 5; i++ {
		revs = append(revs, putRev(t, db, "b", "k", fmt.Sprintf("v%d", i)))
	}
	mustCheckDB(t, db)
	require.NoError(t, db.Close())

	// 以 HistoryLimit=0 重开：一次性清理历史索引与保留页。
	db = mustOpenHist(t, path, 0, nil)
	require.ErrorIs(t, db.ViewAt(revs[4], func(tx *bolt.Tx) error { return nil }), bolt.ErrHistoryDisabled)
	v, _ := viewGet(t, db, "b", "k")
	require.Equal(t, "v4", v)
	mustCheckDB(t, db)
	require.NoError(t, db.Close())

	// 再次以 0 打开不应有残留问题。
	db = mustOpenHist(t, path, 0, nil)
	mustCheckDB(t, db)
	require.NoError(t, db.Close())

	// 重新开启：只有当前修订可读，更早的修订视为过期。
	db = mustOpenHist(t, path, 3, nil)
	defer func() { _ = db.Close() }()
	var current int
	require.NoError(t, db.View(func(tx *bolt.Tx) error { current = tx.ID(); return nil }))
	require.ErrorIs(t, db.ViewAt(revs[4], func(tx *bolt.Tx) error { return nil }), bolt.ErrRevisionExpired)
	var val string
	require.NoError(t, db.ViewAt(current, func(tx *bolt.Tx) error {
		val = string(tx.Bucket([]byte("b")).Get([]byte("k")))
		return nil
	}))
	require.Equal(t, "v4", val)
	r := putRev(t, db, "b", "k", "vnew")
	v, _ = viewAtGet(t, db, r, "b", "k")
	require.Equal(t, "vnew", v)
	mustCheckDB(t, db)
}

func viewGet(t *testing.T, db *bolt.DB, bucket, key string) (string, bool) {
	t.Helper()
	var val string
	var ok bool
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket([]byte(bucket)).Get([]byte(key)); v != nil {
			val = string(v)
			ok = true
		}
		return nil
	}))
	return val, ok
}

// 小页 + 大窗口：历史索引跨多页，且删除整个桶会产生跨页页日志。
func TestHistory_SmallPagesLargeWindow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db := mustOpenHist(t, path, 60, func(o *bolt.Options) {
		o.PageSize = 512
		o.InitialMmapSize = 1 << 20
	})
	defer func() { _ = db.Close() }()

	var revs []int
	for i := 0; i < 65; i++ {
		revs = append(revs, putRev(t, db, "b", fmt.Sprintf("k%03d", i%10), fmt.Sprintf("v%03d", i)))
	}
	// 60 个修订仍在窗口内，索引至少跨越多页（每页放不下 60 条 40 字节记录）。
	for i, rev := range revs[5:] {
		wantN := i + 5 + 1 // revs[i] 时已写入 i+1 个不同键
		if wantN > 10 {
			wantN = 10
		}
		var n int
		require.NoError(t, db.ViewAt(rev, func(tx *bolt.Tx) error {
			c := tx.Bucket([]byte("b")).Cursor()
			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				n++
			}
			return nil
		}))
		require.Equal(t, wantN, n, "revision %d (index %d)", rev, i+5)
	}
	require.ErrorIs(t, db.ViewAt(revs[4], func(tx *bolt.Tx) error { return nil }), bolt.ErrRevisionExpired)
	mustCheckDB(t, db)

	// 一次删除一个较大的桶（许多页一次性进入页日志，可能跨多页），
	// 删除前后的修订都必须可读。
	before := 0
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("big"))
		if err != nil {
			return err
		}
		for i := 0; i < 120; i++ {
			if err := b.Put([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%04d-pad-pad", i))); err != nil {
				return err
			}
		}
		before = tx.ID()
		return nil
	}))
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte("big"))
	}))
	var afterID int
	require.NoError(t, db.View(func(tx *bolt.Tx) error { afterID = tx.ID(); return nil }))

	var count int
	require.NoError(t, db.ViewAt(before, func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("big")).Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			count++
		}
		return nil
	}))
	require.Equal(t, 120, count)
	require.NoError(t, db.ViewAt(afterID, func(tx *bolt.Tx) error {
		require.Nil(t, tx.Bucket([]byte("big")))
		return nil
	}))
	require.NoError(t, db.Close())

	// 重开后多页索引与多页页日志仍能解码。
	db = mustOpenHist(t, path, 60, func(o *bolt.Options) {
		o.PageSize = 512
		o.InitialMmapSize = 1 << 20
	})
	count = 0
	require.NoError(t, db.ViewAt(before, func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("big")).Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			count++
		}
		return nil
	}))
	require.Equal(t, 120, count)
	mustCheckDB(t, db)
}

// 并发：持续提交的写者与多个 ViewAt / View 读者在 race 下运行，
// 读到的内容必须与修订一一对应，不允许脏读。
func TestHistory_Concurrent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db := mustOpenHist(t, path, 8, nil)
	defer func() { _ = db.Close() }()

	putRev(t, db, "b", "k", "v0")

	var mu sync.Mutex
	valueByRev := map[int]string{}
	var orderedRevs []int

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 写者。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= 150; i++ {
			v := fmt.Sprintf("v%d", i)
			rev := 0
			_ = db.Update(func(tx *bolt.Tx) error {
				b, _ := tx.CreateBucketIfNotExists([]byte("b"))
				_ = b.Put([]byte("k"), []byte(v))
				rev = tx.ID()
				return nil
			})
			mu.Lock()
			valueByRev[rev] = v
			orderedRevs = append(orderedRevs, rev)
			mu.Unlock()
		}
		close(stop)
	}()

	// 历史读者：始终选择最近窗口内的修订；修订恰好被挤出窗口（过期）
	// 是允许的竞争结果，但只要读取成功，值必须精确匹配该修订。
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			x := seed
			for {
				select {
				case <-stop:
					return
				default:
				}
				mu.Lock()
				var id int
				var want string
				if len(orderedRevs) > 0 {
					start := 0
					if len(orderedRevs) > 4 {
						start = len(orderedRevs) - 4
					}
					x = x*6364136223846793005 + 1442695040888963407
					id = orderedRevs[start+int(x>>33)%(len(orderedRevs)-start)]
					want = valueByRev[id]
				}
				mu.Unlock()
				if id == 0 {
					continue
				}
				err := db.ViewAt(id, func(tx *bolt.Tx) error {
					got := tx.Bucket([]byte("b")).Get([]byte("k"))
					if !bytes.Equal(got, []byte(want)) {
						return fmt.Errorf("rev %d: got %q want %q", id, got, want)
					}
					return nil
				})
				if err != nil && err != bolt.ErrRevisionExpired && err != bolt.ErrRevisionNotFound {
					require.NoError(t, err) // 在 goroutine 中失败即终止
				}
			}
		}(uint64(0x9E3779B97F4A7C15) + uint64(r)*11400714819323198485)
	}

	// 普通最新视图读者。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = db.View(func(tx *bolt.Tx) error {
				if b := tx.Bucket([]byte("b")); b != nil {
					_ = b.Get([]byte("k"))
				}
				return nil
			})
		}
	}()

	wg.Wait()
	mustCheckDB(t, db)
}

// 全新数据库在第一次成功提交之前，锚点修订（当前 meta 的 txid）即可读。
func TestHistory_AnchorRevision(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db := mustOpenHist(t, path, 3, nil)
	defer func() { _ = db.Close() }()

	var anchor int
	require.NoError(t, db.View(func(tx *bolt.Tx) error { anchor = tx.ID(); return nil }))

	called := false
	require.NoError(t, db.ViewAt(anchor, func(tx *bolt.Tx) error {
		called = true
		require.Equal(t, anchor, tx.ID())
		require.Nil(t, tx.Bucket([]byte("nope")))
		return nil
	}))
	require.True(t, called)
	require.ErrorIs(t, db.ViewAt(anchor+1, func(tx *bolt.Tx) error { return nil }), bolt.ErrRevisionFuture)
}

// 以只读方式打开带历史索引的数据库时，ViewAt 正常工作，且禁止写事务。
func TestHistory_ReadOnlyOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db := mustOpenHist(t, path, 4, nil)
	var revs []int
	for i := 0; i < 5; i++ {
		revs = append(revs, putRev(t, db, "b", "k", fmt.Sprintf("v%d", i)))
	}
	require.NoError(t, db.Close())

	opts := histOptions(4)
	opts.ReadOnly = true
	ro, err := bolt.Open(path, 0600, opts)
	require.NoError(t, err)
	defer func() { _ = ro.Close() }()

	for i, rev := range revs[1:] {
		var val string
		require.NoError(t, ro.ViewAt(rev, func(tx *bolt.Tx) error {
			val = string(tx.Bucket([]byte("b")).Get([]byte("k")))
			return nil
		}))
		require.Equal(t, fmt.Sprintf("v%d", i+1), val)
	}
	require.ErrorIs(t, ro.ViewAt(revs[0], func(tx *bolt.Tx) error { return nil }), bolt.ErrRevisionExpired)

	_, err = ro.Begin(true)
	require.ErrorIs(t, err, bolt.ErrDatabaseReadOnly)
	require.NoError(t, ro.Close())

	// 顺序切换：关闭只读连接后写入新修订，再以只读重开应能读到。
	rw := mustOpenHist(t, path, 4, nil)
	latest := putRev(t, rw, "b", "k", "v5")
	mustCheckDB(t, rw)
	require.NoError(t, rw.Close())

	opts.ReadOnly = true
	ro, err = bolt.Open(path, 0600, opts)
	require.NoError(t, err)
	var got string
	require.NoError(t, ro.ViewAt(latest, func(tx *bolt.Tx) error {
		got = string(tx.Bucket([]byte("b")).Get([]byte("k")))
		return nil
	}))
	require.Equal(t, "v5", got)
}
