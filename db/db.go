package db

import (
	"fmt"
	"helin/btree/btree"
	"helin/btree/pbtree"
	"helin/buffer"
	"helin/catalog"
	"helin/common"
	"helin/concurrency"
	"helin/disk"
	"helin/disk/wal"
	"helin/locker"
	"helin/transaction"
	"log"
	"os"
	"time"
)

const (
	segmentSize        = uint64(16 * 1024 * 1024)
	checkpointInterval = time.Second * 10
)

type Store interface {
	Get(txn transaction.Transaction, key common.Key) any
	Set(txn transaction.Transaction, key common.Key, value any) (isInserted bool)
	Delete(txn transaction.Transaction, key common.Key) bool
	Count(txn transaction.Transaction) int
}

type DB struct {
	cm   concurrency.CheckpointManager
	pool buffer.Pool
	Ctl  catalog.Catalog
	Tm   concurrency.TxnManager
	dm   disk.IDiskManager
	lm   wal.LogManager
	l    *log.Logger

	checkPointDone    chan bool
	checkPointStopped chan bool

	// TODO: default parameters
	checkpointInterval time.Duration
}

func OpenDB(file string, poolSize int, fsync bool) (hdb *DB) {
	defer func() {
		if hdb != nil {
			hdb.StartCheckpointRoutine()
		}
	}()

	f, err := os.Create("info.log")
	if err != nil {
		panic(err)
	}

	l := log.New(f, ">> ", 0)

	dbFileName := file + ".helin"
	logDir := file + "_logs"

	dm, created, err := disk.NewDiskManager(dbFileName, fsync)
	common.PanicIfErr(err)

	lockManager := locker.NewLockManager()

	// TODO: get these as parameters
	lm, err := wal.OpenBWALLogManager(512*1024, segmentSize, logDir, wal.NewDefaultSerDe())
	common.PanicIfErr(err)

	if created {
		pool := buffer.NewBufferPoolV2WithDM(true, poolSize, dm, lm, lockManager)
		tm := concurrency.NewTxnManager(pool, lm, lockManager, segmentSize, logDir)
		cm := concurrency.NewCheckpointManager(pool, lm, tm)

		// NOTE: maybe use global serializers instead of initializing structs
		pager := btree.NewPager2(pbtree.NewBufferPoolBPager(pool, lm), &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
		catalogStore := btree.NewBtreeWithPager(transaction.TxnTODO(), catalog.BtreeDegree, pager)
		dm.SetCatalogPID(uint64(catalogStore.GetMetaPID()))
		if err := pool.FlushAll(); err != nil {
			panic(err)
		}

		return &DB{
			cm:                 cm,
			pool:               pool,
			Ctl:                catalog.NewPersistentCatalog(catalogStore, pool, lm),
			Tm:                 tm,
			dm:                 dm,
			lm:                 lm,
			l:                  l,
			checkPointDone:     make(chan bool),
			checkPointStopped:  make(chan bool),
			checkpointInterval: checkpointInterval,
		}
	}

	pool := buffer.NewBufferPoolV2WithDM(false, poolSize, dm, lm, lockManager)
	tm := concurrency.NewTxnManager(pool, lm, lockManager, segmentSize, logDir)
	cm := concurrency.NewCheckpointManager(pool, lm, tm)

	if err := recoverDB(dm, lm, pool, logDir); err != nil {
		panic(fmt.Errorf("failed to recover db: %w", err))
	}

	pager := btree.NewPager2(pbtree.NewBufferPoolBPager(pool, lm), &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
	catalogStore := btree.ConstructBtreeByMeta(transaction.TxnTODO(), btree.Pointer(dm.GetCatalogPID()), pager)

	return &DB{
		cm:                 cm,
		pool:               pool,
		Ctl:                catalog.NewPersistentCatalog(catalogStore, pool, lm),
		Tm:                 tm,
		dm:                 dm,
		lm:                 lm,
		l:                  l,
		checkPointStopped:  make(chan bool),
		checkPointDone:     make(chan bool),
		checkpointInterval: checkpointInterval,
	}
}

func OpenDB2(file string, poolSize int, fsync bool) (hdb *DB) {
	defer func() {
		if hdb != nil {
			hdb.StartCheckpointRoutine()
		}
	}()

	f, err := os.Create("info.log")
	if err != nil {
		panic(err)
	}

	l := log.New(f, ">> ", 0)

	dbFileName := file + ".helin"
	logDir := file + "_logs"

	dm, created, err := disk.NewDiskManager(dbFileName, fsync)
	common.PanicIfErr(err)

	lockManager := locker.NewLockManager()

	// TODO: get these as parameters

	lm := wal.NoopLM
	if created {
		pool := buffer.NewBufferPoolV2WithDM(true, poolSize, dm, lm, lockManager)
		tm := concurrency.NewTxnManager(pool, lm, lockManager, segmentSize, logDir)
		cm := concurrency.NewCheckpointManager(pool, lm, tm)

		// NOTE: maybe use global serializers instead of initializing structs
		pager := btree.NewPager2(pbtree.NewBufferPoolBPager(pool, lm), &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
		catalogStore := btree.NewBtreeWithPager(transaction.TxnTODO(), catalog.BtreeDegree, pager)
		dm.SetCatalogPID(uint64(catalogStore.GetMetaPID()))
		if err := pool.FlushAll(); err != nil {
			panic(err)
		}

		return &DB{
			cm:                 cm,
			pool:               pool,
			Ctl:                catalog.NewPersistentCatalog(catalogStore, pool, lm),
			Tm:                 tm,
			dm:                 dm,
			lm:                 lm,
			l:                  l,
			checkPointDone:     make(chan bool),
			checkPointStopped:  make(chan bool),
			checkpointInterval: checkpointInterval,
		}
	}

	pool := buffer.NewBufferPoolV2WithDM(false, poolSize, dm, lm, lockManager)
	tm := concurrency.NewTxnManager(pool, lm, lockManager, segmentSize, logDir)
	cm := concurrency.NewCheckpointManager(pool, lm, tm)

	if err := recoverDB(dm, lm, pool, logDir); err != nil {
		panic(fmt.Errorf("failed to recover db: %w", err))
	}

	pager := btree.NewPager2(pbtree.NewBufferPoolBPager(pool, lm), &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
	catalogStore := btree.ConstructBtreeByMeta(transaction.TxnTODO(), btree.Pointer(dm.GetCatalogPID()), pager)

	return &DB{
		cm:                 cm,
		pool:               pool,
		Ctl:                catalog.NewPersistentCatalog(catalogStore, pool, lm),
		Tm:                 tm,
		dm:                 dm,
		lm:                 lm,
		l:                  l,
		checkPointStopped:  make(chan bool),
		checkPointDone:     make(chan bool),
		checkpointInterval: checkpointInterval,
	}
}

// recoverDB tries to recover db from crash. If db is closed gracefully recovery will be fast since there will be an empty
// checkpoint at the end of the log file.
func recoverDB(dm *disk.Manager, lm wal.LogManager, pool buffer.Pool, logDir string) error {
	iter, err := wal.OpenBwalLogIterEnd(segmentSize, logDir, wal.NewDefaultSerDe())
	if err != nil {
		return err
	}

	r := concurrency.NewRecovery(iter, lm, dm, pool)
	return r.Recover()
}

func (d *DB) StartCheckpointRoutine() {
	// initiate checkpoint routine
	go func() {
		for {
			tick := time.After(d.checkpointInterval)
			select {
			case <-tick:
				if err := d.cm.TakeCheckpoint(); err != nil {
					log.Println(err.Error())
				}
				d.l.Println("checkpoint taken")
			case <-d.checkPointDone:
				d.l.Println("stopped checkpoint routine")
				d.checkPointStopped <- true
				return
			}
		}
	}()
}

func (d *DB) BeginTxn() transaction.Transaction {
	return d.Tm.Begin()
}

func (d *DB) Commit(txn transaction.Transaction) error {
	return d.Tm.Commit(txn)
}

func (d *DB) CommitByID(txnID transaction.TxnID) error {
	return d.Tm.CommitByID(txnID)
}

func (d *DB) Rollback(txn transaction.Transaction) {
	d.Tm.Abort(txn)
}

func (d *DB) CreateStore(txn transaction.Transaction, name string) error {
	_, err := d.Ctl.CreateStore(txn, name)
	return err
}

func (d *DB) ListStores() ([]string, error) {
	return d.Ctl.ListStores(), nil
}

func (d *DB) GetStore(txn transaction.Transaction, name string) Store {
	return d.Ctl.GetStore(txn, name)
}

func (d *DB) StoreGet(txnID transaction.TxnID, store string, key common.Key) any {
	txn := d.Tm.GetByID(txnID)
	s := d.GetStore(txn, store)
	return s.Get(txn, key)
}

func (d *DB) StoreSet(txnID transaction.TxnID, store string, key common.Key, value any) (isInserted bool) {
	txn := d.Tm.GetByID(txnID)
	s := d.GetStore(txn, store)
	return s.Set(txn, key, value)
}

func (d *DB) StoreDelete(txnID transaction.TxnID, store string, key common.Key) bool {
	txn := d.Tm.GetByID(txnID)
	s := d.GetStore(txn, store)
	return s.Delete(txn, key)
}

func (d *DB) Metrics() int {
	return d.pool.EmptyFrameSize()
}

func (d *DB) Close() error {
	// block new transactions and wait until all active transactions are done
	d.Tm.Close()

	// stop checkpoint routine and wait for it to stop
	close(d.checkPointDone)
	<-d.checkPointStopped

	// take one last checkpoint to flush all buffers
	if err := d.cm.TakeCheckpoint(); err != nil {
		return err
	}

	// flush wal
	if err := d.lm.Flush(); err != nil {
		return fmt.Errorf("failed to flush err: %w", err)
	}

	return nil
}
