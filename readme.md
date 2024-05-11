# What Is This?
I dunno, a database written in go maybe? It will be at least.

## To Run Tests
```shell
go test ./...
```

For verbose results;
```shell
go test ./... -v
```

# NOTES

* binary.Write calls the write method of underlying io.Writer object. If it is bytes.Buffer
then it always returns nil as the error hence no need to pass error or check it. 
Maybe check and panic to detect possible api changes in bytes.Buffer   

# TODOS:
- [x] Make pool size dynamic for each buffer pool instance.(Constant for the buffer 
pool but each buffer pool can have different size)

- [x] Concurrent index

- [x] Variable sized keys in B+ tree

- [x] Overflow pages in B+ tree

- [x] Eliminate duplicated code in var_key_nodes.go

- [x] Buffer pool free page implementation

- [x] WAL implementation

- [x] Clock replacer implementation for buffer pool

- [x] Use uint as page id instead of int

- [ ] Refactor free and alloc page logic to make it more compliant with recovery and WAL logic
  - implement free list as a separate construct from disk manager
  - integrate that construct directly to buffer pool and set pageLSN on freed pages
  - when recovering decide what to do based on pageLSN 

- [ ] If replacer had accounted for forced wal flushes and avoided them it would increase performance 

- [ ] More than one overflow pages in B+ tree

- [ ] B+ tree try insert with read lock traversal 

- [ ] Refactor B+ tree split logic so that nodes are split in the middle in terms of bytes with variable sized keys

- [ ] Write better random load tests for B+ tree index with concurrent inserts and deletes

- [ ] Define page_id type and use it everywhere instead of uint64

- [ ] What happens if crash happens in the middle of initializing slotted page

free page is not undoable once a page is freed its content is
not logged hence action cannot be undone. That means new page
is undoable but there cannot be another action after that.

meaning, to undo a new page, put page in free list and that is
it. There cannot be any other log record that is to be undone
regarding that page.

WHEN TO SET DIRTY?
Once a log record modifying a page is appended to log, that page is dirty.
If a log record modifying a page is appended and right after that a checkpoint started
and its log record is appended too, if aforementioned page is not considered dirty then
we have a correctness problem because that page might be flushed(buffer pool can flush a page even if it is pinned) 
before changing its state to dirty. That could lead to missing updates in crash recovery.

buffer pool holds global lock when doing io 

IMPORTANT NOTE: 
  if txn creates a page and frees it and then rolls back. undoing free is required   