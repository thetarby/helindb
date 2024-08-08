package pfreelistv1

//func mkPoolTemp(t *testing.T) buffer.Pool {
//	dir, err := os.MkdirTemp("./", "")
//	if err != nil {
//		panic(err)
//	}
//
//	dbName := filepath.Join(dir, uuid.New().String())
//
//	dm, _, err := disk.NewDiskManager(dbName, false)
//	require.NoError(t, err)
//
//	pool := buffer.NewBufferPoolV2WithDM(true, 128, dm, wal.NoopLM)
//
//	t.Cleanup(func() {
//		if err := os.RemoveAll(dir); err != nil {
//			t.Fatal(err)
//		}
//	})
//
//	return pool
//}
//
//func TestBPFreeList(t *testing.T) {
//	// initialize a buffer pool
//	pool := mkPoolTemp(t)
//	for i := 0; i < 100; i++ {
//		p, err := pool.NewPage(transaction.TxnNoop())
//		pages.InitSlottedPage(p)
//		pool.Unpin(p.GetPageId(), true)
//
//		require.NoError(t, err)
//	}
//
//	freeList := freelistv1.NewFreeList(NewBufferPoolPager(pool, wal.NoopLM), true)
//
//	freeList.GetHeaderPageLsn()
//
//	assert.NoError(t, freeList.Add(transaction.TxnNoop(), 10))
//	assert.NoError(t, freeList.Add(transaction.TxnNoop(), 20))
//	assert.NoError(t, freeList.Add(transaction.TxnNoop(), 30))
//
//	popped, err := freeList.Pop(transaction.TxnNoop())
//	assert.NoError(t, err)
//	assert.EqualValues(t, 10, popped)
//
//	popped, err = freeList.Pop(transaction.TxnNoop())
//	assert.NoError(t, err)
//	assert.EqualValues(t, 20, popped)
//
//	popped, err = freeList.Pop(transaction.TxnNoop())
//	assert.NoError(t, err)
//	assert.EqualValues(t, 30, popped)
//}
