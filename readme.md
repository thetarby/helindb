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

- [ ] More than one overflow pages in B+ tree

- [ ] B+ tree try insert with read lock traversal 

- [ ] Refactor B+ tree split logic so that nodes are split in the middle in terms of bytes with variable sized keys

- [ ] Write better random load tests for B+ tree index with concurrent inserts and deletes

- [ ] Buffer pool free page implementation

- [ ] WAL implementation 

- [ ] Clock replacer implementation for buffer pool