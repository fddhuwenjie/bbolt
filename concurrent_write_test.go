package bbolt_test

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/btesting"
)

// ccCheck 读取整库并执行一致性检查。
func ccCheck(t *testing.T, db *btesting.DB) {
	t.Helper()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		for err := range tx.Check() {
			require.NoError(t, err)
		}
		return nil
	}))
}

// ccReopen 关闭数据库后重新打开，返回同一个测试包装句柄。
func ccReopen(t *testing.T, db *btesting.DB) *btesting.DB {
	t.Helper()
	db.MustClose()
	db.MustReopen()
	return db
}

// ccSeed 预先创建桶并写入初始数据。
func ccSeed(t *testing.T, db *btesting.DB, bucket string, kvs map[string]string) {
	t.Helper()
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		for k, v := range kvs {
			if err := b.Put([]byte(k), []byte(v)); err != nil {
				return err
			}
		}
		return nil
	}))
}

func ccGet(t *testing.T, db *btesting.DB, bucket, key string) string {
	t.Helper()
	var out string
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		require.NotNil(t, b)
		v := b.Get([]byte(key))
		if v != nil {
			out = string(v)
		}
		return nil
	}))
	return out
}

// TestCC_DifferentKeysSameBucket：修改不同键时，无论谁先提交，双方结果都保留。
func TestCC_DifferentKeysSameBucket(t *testing.T) {
	for _, firstA := range []bool{true, false} {
		name := "A first"
		if !firstA {
			name = "B first"
		}
		t.Run(name, func(t *testing.T) {
			db := btesting.MustCreateDB(t)
			ccSeed(t, db, "bk", map[string]string{"old": "v0"})

			ta, err := db.BeginConcurrent()
			require.NoError(t, err)
			tb, err := db.BeginConcurrent()
			require.NoError(t, err)

			require.NoError(t, ta.Bucket([]byte("bk")).Put([]byte("a"), []byte("va")))
			require.NoError(t, tb.Bucket([]byte("bk")).Put([]byte("b"), []byte("vb")))

			commit := func(tx *bolt.Tx) error { return tx.Commit() }
			if firstA {
				require.NoError(t, commit(ta))
				require.NoError(t, commit(tb))
			} else {
				require.NoError(t, commit(tb))
				require.NoError(t, commit(ta))
			}

			require.Equal(t, "v0", ccGet(t, db, "bk", "old"))
			require.Equal(t, "va", ccGet(t, db, "bk", "a"))
			require.Equal(t, "vb", ccGet(t, db, "bk", "b"))
			ccCheck(t, db)

			db = ccReopen(t, db)
			defer db.Close()
			require.Equal(t, "va", ccGet(t, db, "bk", "a"))
			require.Equal(t, "vb", ccGet(t, db, "bk", "b"))
		})
	}
}

// TestCC_SameKeyConflict：修改同一个键时，后提交的一方必须明确冲突失败。
func TestCC_SameKeyConflict(t *testing.T) {
	for _, firstA := range []bool{true, false} {
		name := "A first"
		if !firstA {
			name = "B first"
		}
		t.Run(name, func(t *testing.T) {
			db := btesting.MustCreateDB(t)
			ccSeed(t, db, "bk", map[string]string{"k": "v0"})

			ta, err := db.BeginConcurrent()
			require.NoError(t, err)
			tb, err := db.BeginConcurrent()
			require.NoError(t, err)

			require.NoError(t, ta.Bucket([]byte("bk")).Put([]byte("k"), []byte("va")))
			require.NoError(t, tb.Bucket([]byte("bk")).Put([]byte("k"), []byte("vb")))

			if firstA {
				require.NoError(t, ta.Commit())
				err = tb.Commit()
			} else {
				require.NoError(t, tb.Commit())
				err = ta.Commit()
			}
			require.ErrorIs(t, err, bolt.ErrConcurrentTransactionConflict)

			// 先提交一方的数据保留，失败方不得覆盖。
			require.Equal(t, map[string]string{"k": pick(firstA, "va", "vb")},
				map[string]string{"k": ccGet(t, db, "bk", "k")})
			ccCheck(t, db)
		})
	}
}

func pick(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}

// TestCC_DeleteSameKeyConflict：一方更新、另一方删除同一个键，后提交方必须失败。
func TestCC_DeleteSameKeyConflict(t *testing.T) {
	db := btesting.MustCreateDB(t)
	ccSeed(t, db, "bk", map[string]string{"k": "v0", "k2": "v20", "keep": "x"})

	t.Run("put commits first", func(t *testing.T) {
		ta, err := db.BeginConcurrent()
		require.NoError(t, err)
		tb, err := db.BeginConcurrent()
		require.NoError(t, err)
		require.NoError(t, ta.Bucket([]byte("bk")).Put([]byte("k"), []byte("v1")))
		require.NoError(t, tb.Bucket([]byte("bk")).Delete([]byte("k")))
		require.NoError(t, ta.Commit())
		require.ErrorIs(t, tb.Commit(), bolt.ErrConcurrentTransactionConflict)
		require.Equal(t, "v1", ccGet(t, db, "bk", "k"))
	})

	t.Run("delete commits first", func(t *testing.T) {
		ta, err := db.BeginConcurrent()
		require.NoError(t, err)
		tb, err := db.BeginConcurrent()
		require.NoError(t, err)
		require.NoError(t, ta.Bucket([]byte("bk")).Put([]byte("k2"), []byte("v2")))
		require.NoError(t, tb.Bucket([]byte("bk")).Delete([]byte("k2")))
		require.NoError(t, tb.Commit())
		require.ErrorIs(t, ta.Commit(), bolt.ErrConcurrentTransactionConflict)
		require.Equal(t, "", ccGet(t, db, "bk", "k2"))
	})
	ccCheck(t, db)
}

// TestCC_DifferentBuckets：双方各自创建并写入不同桶，任意提交顺序都保留。
func TestCC_DifferentBuckets(t *testing.T) {
	for _, firstA := range []bool{true, false} {
		t.Run(pick(firstA, "A first", "B first"), func(t *testing.T) {
			db := btesting.MustCreateDB(t)

			ta, err := db.BeginConcurrent()
			require.NoError(t, err)
			tb, err := db.BeginConcurrent()
			require.NoError(t, err)

			ba, err := ta.CreateBucket([]byte("ba"))
			require.NoError(t, err)
			require.NoError(t, ba.Put([]byte("x"), []byte("1")))
			bb, err := tb.CreateBucket([]byte("bb"))
			require.NoError(t, err)
			require.NoError(t, bb.Put([]byte("y"), []byte("2")))

			if firstA {
				require.NoError(t, ta.Commit())
				require.NoError(t, tb.Commit())
			} else {
				require.NoError(t, tb.Commit())
				require.NoError(t, ta.Commit())
			}

			require.Equal(t, "1", ccGet(t, db, "ba", "x"))
			require.Equal(t, "2", ccGet(t, db, "bb", "y"))
			ccCheck(t, db)
		})
	}
}

// TestCC_CreateSameBucketConflict：双方创建同名桶，后提交方明确失败。
func TestCC_CreateSameBucketConflict(t *testing.T) {
	db := btesting.MustCreateDB(t)

	ta, err := db.BeginConcurrent()
	require.NoError(t, err)
	tb, err := db.BeginConcurrent()
	require.NoError(t, err)

	ba, err := ta.CreateBucket([]byte("dup"))
	require.NoError(t, err)
	require.NoError(t, ba.Put([]byte("k"), []byte("a")))
	bb, err := tb.CreateBucket([]byte("dup"))
	require.NoError(t, err)
	require.NoError(t, bb.Put([]byte("k"), []byte("b")))

	require.NoError(t, ta.Commit())
	require.ErrorIs(t, tb.Commit(), bolt.ErrConcurrentTransactionConflict)
	require.Equal(t, "a", ccGet(t, db, "dup", "k"))
	ccCheck(t, db)
}

// TestCC_DeleteVsWriteBucketConflict：删除桶与写桶并发时，后提交方必须失败，
// 不能静默吞掉对方已提交的数据。内联桶与多页桶两种形态都要覆盖。
func TestCC_DeleteVsWriteBucketConflict(t *testing.T) {
	cases := []struct {
		name      string
		seedCount int // 0 表示保持内联桶
	}{
		{name: "inline bucket", seedCount: 0},
		{name: "spilled bucket", seedCount: 2000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := btesting.MustCreateDB(t)
			if tc.seedCount > 0 {
				require.NoError(t, db.Update(func(tx *bolt.Tx) error {
					b, err := tx.CreateBucket([]byte("victim"))
					if err != nil {
						return err
					}
					for i := 0; i < tc.seedCount; i++ {
						if err := b.Put([]byte(fmt.Sprintf("seed-%06d", i)), make([]byte, 64)); err != nil {
							return err
						}
					}
					return nil
				}))
			} else {
				ccSeed(t, db, "victim", map[string]string{"seed": "s"})
			}

			t.Run("writer first", func(t *testing.T) {
				tw, err := db.BeginConcurrent()
				require.NoError(t, err)
				td, err := db.BeginConcurrent()
				require.NoError(t, err)
				require.NoError(t, tw.Bucket([]byte("victim")).Put([]byte("new"), []byte("v")))
				require.NoError(t, td.DeleteBucket([]byte("victim")))
				require.NoError(t, tw.Commit())
				require.ErrorIs(t, td.Commit(), bolt.ErrConcurrentTransactionConflict)
				require.Equal(t, "v", ccGet(t, db, "victim", "new"))
			})

			t.Run("deleter first", func(t *testing.T) {
				tw, err := db.BeginConcurrent()
				require.NoError(t, err)
				td, err := db.BeginConcurrent()
				require.NoError(t, err)
				require.NoError(t, tw.Bucket([]byte("victim")).Put([]byte("new2"), []byte("v")))
				require.NoError(t, td.DeleteBucket([]byte("victim")))
				require.NoError(t, td.Commit())
				require.ErrorIs(t, tw.Commit(), bolt.ErrConcurrentTransactionConflict)
				require.NoError(t, db.View(func(tx *bolt.Tx) error {
					require.Nil(t, tx.Bucket([]byte("victim")))
					return nil
				}))
			})
			ccCheck(t, db)
		})
	}
}

// TestCC_DeleteDifferentBucket：删除与自己无关的桶可以与对方的写入并存。
func TestCC_DeleteDifferentBucket(t *testing.T) {
	db := btesting.MustCreateDB(t)
	ccSeed(t, db, "keep", map[string]string{"a": "1"})
	ccSeed(t, db, "gone", map[string]string{"b": "2"})

	ta, err := db.BeginConcurrent()
	require.NoError(t, err)
	tb, err := db.BeginConcurrent()
	require.NoError(t, err)

	require.NoError(t, ta.Bucket([]byte("keep")).Put([]byte("c"), []byte("3")))
	require.NoError(t, tb.DeleteBucket([]byte("gone")))

	require.NoError(t, ta.Commit())
	require.NoError(t, tb.Commit())

	require.Equal(t, "3", ccGet(t, db, "keep", "c"))
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		require.Nil(t, tx.Bucket([]byte("gone")))
		require.NotNil(t, tx.Bucket([]byte("keep")))
		return nil
	}))
	ccCheck(t, db)
}

// TestCC_RollbackIsolation：一方回滚不影响另一方提交，也不留下任何痕迹。
func TestCC_RollbackIsolation(t *testing.T) {
	db := btesting.MustCreateDB(t)
	ccSeed(t, db, "bk", map[string]string{"k": "v0"})

	ta, err := db.BeginConcurrent()
	require.NoError(t, err)
	tb, err := db.BeginConcurrent()
	require.NoError(t, err)

	require.NoError(t, ta.Bucket([]byte("bk")).Put([]byte("a"), []byte("va")))
	require.NoError(t, tb.Bucket([]byte("bk")).Put([]byte("b"), []byte("vb")))

	// A 先回滚，B 随后提交；即使 A 回滚前操作过相同桶也不影响 B。
	require.NoError(t, ta.Rollback())
	require.NoError(t, tb.Commit())

	require.Equal(t, "", ccGet(t, db, "bk", "a"))
	require.Equal(t, "vb", ccGet(t, db, "bk", "b"))
	require.Equal(t, "v0", ccGet(t, db, "bk", "k"))

	// 回滚后的事务不能再次提交或回滚。
	require.ErrorIs(t, ta.Commit(), berrors.ErrTxClosed)
	require.ErrorIs(t, ta.Rollback(), berrors.ErrTxClosed)
	ccCheck(t, db)
}

// TestCC_SnapshotRead：旧只读事务保持快照，新只读事务看到已提交结果。
//
// 与普通 Begin(true)/Update 相同，当存在长期打开的只读事务时，写者扩容 mmap
// 会等待其结束（见 DB.Begin 文档），因此这里通过 InitialMmapSize 预留空间。
func TestCC_SnapshotRead(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{InitialMmapSize: 1 << 20})
	ccSeed(t, db, "bk", map[string]string{"k": "old"})

	// 提交前开启的只读事务。
	old, err := db.Begin(false)
	require.NoError(t, err)

	ta, err := db.BeginConcurrent()
	require.NoError(t, err)
	require.NoError(t, ta.Bucket([]byte("bk")).Put([]byte("k"), []byte("new")))
	require.NoError(t, ta.Commit())

	// 旧读事务仍看到快照值。
	ob := old.Bucket([]byte("bk"))
	require.Equal(t, "old", string(ob.Get([]byte("k"))))

	// 新读事务看到已提交值。
	require.Equal(t, "new", ccGet(t, db, "bk", "k"))

	require.NoError(t, old.Rollback())
}

// TestCC_OnCommitCallbacks：回调只在成功提交后运行一次，回滚或冲突不运行。
func TestCC_OnCommitCallbacks(t *testing.T) {
	db := btesting.MustCreateDB(t)
	ccSeed(t, db, "bk", map[string]string{"k": "v0"})

	t.Run("success runs once", func(t *testing.T) {
		var n int32
		tx, err := db.BeginConcurrent()
		require.NoError(t, err)
		tx.OnCommit(func() { atomic.AddInt32(&n, 1) })
		tx.OnCommit(func() { atomic.AddInt32(&n, 2) })
		require.NoError(t, tx.Bucket([]byte("bk")).Put([]byte("c1"), []byte("x")))
		require.NoError(t, tx.Commit())
		require.Equal(t, int32(3), atomic.LoadInt32(&n))
		// 再次提交失败，回调不能重复执行。
		require.ErrorIs(t, tx.Commit(), berrors.ErrTxClosed)
		require.Equal(t, int32(3), atomic.LoadInt32(&n))
	})

	t.Run("rollback does not run", func(t *testing.T) {
		var n int32
		tx, err := db.BeginConcurrent()
		require.NoError(t, err)
		tx.OnCommit(func() { atomic.AddInt32(&n, 1) })
		require.NoError(t, tx.Bucket([]byte("bk")).Put([]byte("c2"), []byte("x")))
		require.NoError(t, tx.Rollback())
		require.Equal(t, int32(0), atomic.LoadInt32(&n))
	})

	t.Run("conflict does not run", func(t *testing.T) {
		var n int32
		tx, err := db.BeginConcurrent()
		require.NoError(t, err)
		tx.OnCommit(func() { atomic.AddInt32(&n, 1) })
		require.NoError(t, tx.Bucket([]byte("bk")).Put([]byte("k"), []byte("cc")))

		// 另一个事务先改掉同一个键。
		require.NoError(t, db.Update(func(t2 *bolt.Tx) error {
			return t2.Bucket([]byte("bk")).Put([]byte("k"), []byte("serial"))
		}))
		require.ErrorIs(t, tx.Commit(), bolt.ErrConcurrentTransactionConflict)
		require.Equal(t, int32(0), atomic.LoadInt32(&n))
		require.Equal(t, "serial", ccGet(t, db, "bk", "k"))
	})
}

// TestCC_InterleaveSerialWrites：普通串行 Update 必须能与并发事务交错使用。
func TestCC_InterleaveSerialWrites(t *testing.T) {
	db := btesting.MustCreateDB(t)
	ccSeed(t, db, "bk", map[string]string{"k": "v0"})

	t.Run("disjoint keys merge", func(t *testing.T) {
		tx, err := db.BeginConcurrent()
		require.NoError(t, err)
		require.NoError(t, tx.Bucket([]byte("bk")).Put([]byte("cc"), []byte("c")))

		// 并发事务打开期间进行普通串行写入（不同键、不同桶）。
		require.NoError(t, db.Update(func(u *bolt.Tx) error {
			if err := u.Bucket([]byte("bk")).Put([]byte("s"), []byte("sv")); err != nil {
				return err
			}
			_, err := u.CreateBucket([]byte("serial-bucket"))
			return err
		}))

		require.NoError(t, tx.Commit())
		require.Equal(t, "c", ccGet(t, db, "bk", "cc"))
		require.Equal(t, "sv", ccGet(t, db, "bk", "s"))
	})

	t.Run("serial touches same key", func(t *testing.T) {
		tx, err := db.BeginConcurrent()
		require.NoError(t, err)
		require.NoError(t, tx.Bucket([]byte("bk")).Put([]byte("shared"), []byte("from-cc")))

		require.NoError(t, db.Update(func(u *bolt.Tx) error {
			return u.Bucket([]byte("bk")).Put([]byte("shared"), []byte("from-serial"))
		}))
		require.ErrorIs(t, tx.Commit(), bolt.ErrConcurrentTransactionConflict)
		require.Equal(t, "from-serial", ccGet(t, db, "bk", "shared"))
	})

	t.Run("serial deletes bucket in use", func(t *testing.T) {
		ccSeed(t, db, "doomed", map[string]string{"x": "1"})
		tx, err := db.BeginConcurrent()
		require.NoError(t, err)
		require.NoError(t, tx.Bucket([]byte("doomed")).Put([]byte("y"), []byte("2")))

		require.NoError(t, db.Update(func(u *bolt.Tx) error {
			return u.DeleteBucket([]byte("doomed"))
		}))
		require.ErrorIs(t, tx.Commit(), bolt.ErrConcurrentTransactionConflict)
	})

	t.Run("serial after commit is visible", func(t *testing.T) {
		tx, err := db.BeginConcurrent()
		require.NoError(t, err)
		require.NoError(t, tx.Bucket([]byte("bk")).Put([]byte("z1"), []byte("a")))
		require.NoError(t, tx.Commit())

		require.NoError(t, db.Update(func(u *bolt.Tx) error {
			return u.Bucket([]byte("bk")).Put([]byte("z2"), []byte("b"))
		}))
		require.Equal(t, "a", ccGet(t, db, "bk", "z1"))
		require.Equal(t, "b", ccGet(t, db, "bk", "z2"))
	})
	ccCheck(t, db)
}

// TestCC_NestedBuckets：嵌套桶路径上的不同键合并、相同键冲突。
func TestCC_NestedBuckets(t *testing.T) {
	db := btesting.MustCreateDB(t)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		outer, err := tx.CreateBucket([]byte("outer"))
		if err != nil {
			return err
		}
		_, err = outer.CreateBucket([]byte("inner"))
		return err
	}))

	ta, err := db.BeginConcurrent()
	require.NoError(t, err)
	tb, err := db.BeginConcurrent()
	require.NoError(t, err)

	ia := ta.Bucket([]byte("outer")).Bucket([]byte("inner"))
	ib := tb.Bucket([]byte("outer")).Bucket([]byte("inner"))
	require.NotNil(t, ia)
	require.NotNil(t, ib)
	require.NoError(t, ia.Put([]byte("a"), []byte("1")))
	require.NoError(t, ib.Put([]byte("b"), []byte("2")))
	require.NoError(t, ta.Commit())
	require.NoError(t, tb.Commit())

	require.Equal(t, "1", ccGetNested(t, db, "a"))
	require.Equal(t, "2", ccGetNested(t, db, "b"))

	// 嵌套桶同键冲突。
	tc, err := db.BeginConcurrent()
	require.NoError(t, err)
	td, err := db.BeginConcurrent()
	require.NoError(t, err)
	require.NoError(t, tc.Bucket([]byte("outer")).Bucket([]byte("inner")).Put([]byte("c"), []byte("3")))
	require.NoError(t, td.Bucket([]byte("outer")).Bucket([]byte("inner")).Put([]byte("c"), []byte("4")))
	require.NoError(t, tc.Commit())
	require.ErrorIs(t, td.Commit(), bolt.ErrConcurrentTransactionConflict)
	ccCheck(t, db)
}

func ccGetNested(t *testing.T, db *btesting.DB, key string) string {
	t.Helper()
	var out string
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("outer")).Bucket([]byte("inner"))
		if v := b.Get([]byte(key)); v != nil {
			out = string(v)
		}
		return nil
	}))
	return out
}

// TestCC_Sequence：序列号推进冲突检测，不同桶互不影响。
func TestCC_Sequence(t *testing.T) {
	db := btesting.MustCreateDB(t)
	ccSeed(t, db, "s1", nil)
	ccSeed(t, db, "s2", nil)

	t.Run("same bucket conflicts", func(t *testing.T) {
		ta, err := db.BeginConcurrent()
		require.NoError(t, err)
		tb, err := db.BeginConcurrent()
		require.NoError(t, err)
		v1, err := ta.Bucket([]byte("s1")).NextSequence()
		require.NoError(t, err)
		v2, err := tb.Bucket([]byte("s1")).NextSequence()
		require.NoError(t, err)
		require.Equal(t, uint64(1), v1)
		require.Equal(t, uint64(1), v2)
		require.NoError(t, ta.Commit())
		require.ErrorIs(t, tb.Commit(), bolt.ErrConcurrentTransactionConflict)
	})

	t.Run("different buckets merge", func(t *testing.T) {
		ta, err := db.BeginConcurrent()
		require.NoError(t, err)
		tb, err := db.BeginConcurrent()
		require.NoError(t, err)
		_, err = ta.Bucket([]byte("s1")).NextSequence()
		require.NoError(t, err)
		_, err = tb.Bucket([]byte("s2")).NextSequence()
		require.NoError(t, err)
		require.NoError(t, ta.Commit())
		require.NoError(t, tb.Commit())
		var s1, s2 uint64
		require.NoError(t, db.View(func(tx *bolt.Tx) error {
			s1 = tx.Bucket([]byte("s1")).Sequence()
			s2 = tx.Bucket([]byte("s2")).Sequence()
			return nil
		}))
		require.Equal(t, uint64(2), s1)
		require.Equal(t, uint64(1), s2)
	})
	ccCheck(t, db)
}

// TestCC_NotSupportedOnReadOnly：只读数据库拒绝并发写事务。
func TestCC_NotSupportedOnReadOnly(t *testing.T) {
	db := btesting.MustCreateDB(t)
	ccSeed(t, db, "bk", map[string]string{"k": "v"})
	path := db.Path()
	require.NoError(t, db.Close())

	ro, err := bolt.Open(path, 0600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer ro.Close()
	_, err = ro.BeginConcurrent()
	require.ErrorIs(t, err, berrors.ErrDatabaseReadOnly)
}

// TestCC_MoveBucketRejected：并发事务中 MoveBucket 显式失败而不是静默丢失。
func TestCC_MoveBucketRejected(t *testing.T) {
	db := btesting.MustCreateDB(t)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucket([]byte("src")); err != nil {
			return err
		}
		_, err := tx.CreateBucket([]byte("dst"))
		return err
	}))

	tx, err := db.BeginConcurrent()
	require.NoError(t, err)
	defer tx.Rollback()
	src := tx.Bucket([]byte("src"))
	dst := tx.Bucket([]byte("dst"))
	require.Error(t, tx.MoveBucket([]byte("child"), src, dst))
}

// TestCC_EmptyCommit：无写操作的并发事务也可以正常提交并触发一次回调。
func TestCC_EmptyCommit(t *testing.T) {
	db := btesting.MustCreateDB(t)
	var n int32
	tx, err := db.BeginConcurrent()
	require.NoError(t, err)
	tx.OnCommit(func() { atomic.AddInt32(&n, 1) })
	require.NoError(t, tx.Commit())
	require.Equal(t, int32(1), atomic.LoadInt32(&n))
}

// TestCC_ReopenConsistency：提交后关闭重开，结果保持一致且数据库结构完好。
func TestCC_ReopenConsistency(t *testing.T) {
	db := btesting.MustCreateDB(t)
	ccSeed(t, db, "bk", map[string]string{"k0": "v0"})

	ta, err := db.BeginConcurrent()
	require.NoError(t, err)
	tb, err := db.BeginConcurrent()
	require.NoError(t, err)
	require.NoError(t, ta.Bucket([]byte("bk")).Put([]byte("ka"), []byte("va")))
	bb, err := tb.CreateBucket([]byte("extra"))
	require.NoError(t, err)
	require.NoError(t, bb.Put([]byte("kb"), []byte("vb")))
	require.NoError(t, ta.Commit())
	require.NoError(t, tb.Commit())

	db = ccReopen(t, db)
	defer db.Close()
	require.Equal(t, "v0", ccGet(t, db, "bk", "k0"))
	require.Equal(t, "va", ccGet(t, db, "bk", "ka"))
	require.Equal(t, "vb", ccGet(t, db, "extra", "kb"))
	ccCheck(t, db)
}

// TestCC_CreateOnEmptyDB：空库上的并发事务可以独立建桶、写值，提交后持久化。
func TestCC_CreateOnEmptyDB(t *testing.T) {
	db := btesting.MustCreateDB(t)

	tx, err := db.BeginConcurrent()
	require.NoError(t, err)
	b, err := tx.CreateBucket([]byte("solo"))
	require.NoError(t, err)
	require.NoError(t, b.Put([]byte("k"), []byte("v")))
	require.NoError(t, tx.Commit())

	require.Equal(t, "v", ccGet(t, db, "solo", "k"))
	ccCheck(t, db)
}

// TestCC_DeleteAndRecreateBucketInOneTx：同一事务内先删桶再建桶，旧数据清空、
// 新数据保留。
func TestCC_DeleteAndRecreateBucketInOneTx(t *testing.T) {
	db := btesting.MustCreateDB(t)
	ccSeed(t, db, "reset", map[string]string{"old1": "1", "old2": "2"})

	tx, err := db.BeginConcurrent()
	require.NoError(t, err)
	require.NoError(t, tx.DeleteBucket([]byte("reset")))
	b, err := tx.CreateBucket([]byte("reset"))
	require.NoError(t, err)
	require.NoError(t, b.Put([]byte("fresh"), []byte("yes")))
	require.NoError(t, tx.Commit())

	require.NoError(t, db.View(func(u *bolt.Tx) error {
		bk := u.Bucket([]byte("reset"))
		require.NotNil(t, bk)
		require.Nil(t, bk.Get([]byte("old1")))
		require.Equal(t, "yes", string(bk.Get([]byte("fresh"))))
		return nil
	}))
	ccCheck(t, db)
}

// TestCC_MergeWritesToSameKeyInOneTx：同一事务内对同一键的多次修改合并为最终
// 意图，校验依据是第一次修改前观察到的快照值。
func TestCC_MergeWritesToSameKeyInOneTx(t *testing.T) {
	db := btesting.MustCreateDB(t)
	ccSeed(t, db, "bk", map[string]string{"m": "v0", "u": "u0"})

	t.Run("put twice then delete", func(t *testing.T) {
		tx, err := db.BeginConcurrent()
		require.NoError(t, err)
		b := tx.Bucket([]byte("bk"))
		require.NoError(t, b.Put([]byte("m"), []byte("v1")))
		require.NoError(t, b.Put([]byte("m"), []byte("v2")))
		require.NoError(t, b.Delete([]byte("m")))
		require.NoError(t, tx.Commit())
		require.Equal(t, "", ccGet(t, db, "bk", "m"))
	})

	t.Run("delete then put survives concurrent commit on other keys", func(t *testing.T) {
		ta, err := db.BeginConcurrent()
		require.NoError(t, err)
		tb, err := db.BeginConcurrent()
		require.NoError(t, err)
		ba := ta.Bucket([]byte("bk"))
		require.NoError(t, ba.Delete([]byte("u")))
		require.NoError(t, ba.Put([]byte("u"), []byte("u-new")))
		require.NoError(t, tb.Bucket([]byte("bk")).Put([]byte("w"), []byte("wb")))
		require.NoError(t, tb.Commit())
		require.NoError(t, ta.Commit())
		require.Equal(t, "u-new", ccGet(t, db, "bk", "u"))
		require.Equal(t, "wb", ccGet(t, db, "bk", "w"))
	})
	ccCheck(t, db)
}

// TestCC_ConflictClosesTransaction：冲突返回后事务即已结束，不能再次提交或回滚，
// 且失败方的私有修改不会泄漏到数据库。
func TestCC_ConflictClosesTransaction(t *testing.T) {
	db := btesting.MustCreateDB(t)
	ccSeed(t, db, "bk", map[string]string{"z": "0"})

	ta, err := db.BeginConcurrent()
	require.NoError(t, err)
	tb, err := db.BeginConcurrent()
	require.NoError(t, err)
	require.NoError(t, ta.Bucket([]byte("bk")).Put([]byte("z"), []byte("a")))
	require.NoError(t, tb.Bucket([]byte("bk")).Put([]byte("z"), []byte("b")))
	require.NoError(t, ta.Commit())
	require.ErrorIs(t, tb.Commit(), bolt.ErrConcurrentTransactionConflict)

	// 冲突后事务已关闭。
	require.ErrorIs(t, tb.Commit(), berrors.ErrTxClosed)
	require.ErrorIs(t, tb.Rollback(), berrors.ErrTxClosed)
	require.Equal(t, "a", ccGet(t, db, "bk", "z"))
}

// TestCC_ConcurrentStress：多个并发写事务使用互不相交的键空间，同时与普通
// 串行写入和只读事务交错，所有结果都必须保留。配合 -race 检查数据竞争。
func TestCC_ConcurrentStress(t *testing.T) {
	db := btesting.MustCreateDB(t)
	ccSeed(t, db, "common", map[string]string{"init": "0"})

	const (
		workers  = 8
		rounds   = 4
		keysEach = 40
		serialN  = 30
	)

	var stopReaders atomic.Bool
	var g errgroup.Group

	// 持续的只读负载。
	for i := 0; i < 4; i++ {
		g.Go(func() error {
			for !stopReaders.Load() {
				if err := db.View(func(tx *bolt.Tx) error {
					b := tx.Bucket([]byte("common"))
					_ = b.Get([]byte("init"))
					return nil
				}); err != nil {
					return err
				}
			}
			return nil
		})
	}

	// 普通串行写负载，写自己的键空间。
	g.Go(func() error {
		for i := 0; i < serialN; i++ {
			i := i
			if err := db.Update(func(tx *bolt.Tx) error {
				return tx.Bucket([]byte("common")).Put(
					[]byte(fmt.Sprintf("serial-%04d", i)), []byte("s"))
			}); err != nil {
				return err
			}
		}
		return nil
	})

	// 并发写事务负载，每个 worker 使用独立的桶和独立的键前缀。
	var wg sync.WaitGroup
	var failCount int64
	for w := 0; w < workers; w++ {
		w := w
		wg.Add(1)
		g.Go(func() error {
			defer wg.Done()
			bucket := []byte(fmt.Sprintf("w%d", w))
			for r := 0; r < rounds; r++ {
				tx, err := db.BeginConcurrent()
				if err != nil {
					return err
				}
				b, err := tx.CreateBucketIfNotExists(bucket)
				if err != nil {
					return err
				}
				for k := 0; k < keysEach; k++ {
					key := []byte(fmt.Sprintf("r%d-k%04d", r, k))
					if err := b.Put(key, []byte(fmt.Sprintf("w%d-r%d", w, r))); err != nil {
						return err
					}
				}
				if err := tx.Commit(); err != nil {
					if errors.Is(err, bolt.ErrConcurrentTransactionConflict) {
						// 桶名/键空间互不相交，理论上不应发生；记录以便断言。
						atomic.AddInt64(&failCount, 1)
						continue
					}
					return err
				}
			}
			return nil
		})
	}
	wg.Wait()
	stopReaders.Store(true)
	require.NoError(t, g.Wait())
	require.Equal(t, int64(0), atomic.LoadInt64(&failCount))

	// 校验所有 worker 的全部键都已保留。
	for w := 0; w < workers; w++ {
		bucket := fmt.Sprintf("w%d", w)
		for r := 0; r < rounds; r++ {
			for k := 0; k < keysEach; k++ {
				require.Equal(t,
					fmt.Sprintf("w%d-r%d", w, r),
					ccGet(t, db, bucket, fmt.Sprintf("r%d-k%04d", r, k)))
			}
		}
	}
	for i := 0; i < serialN; i++ {
		require.Equal(t, "s", ccGet(t, db, "common", fmt.Sprintf("serial-%04d", i)))
	}
	ccCheck(t, db)

	// 重开后再次校验。
	db = ccReopen(t, db)
	defer db.Close()
	ccCheck(t, db)
}
