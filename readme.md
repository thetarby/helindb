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
* make pool size dynamic for each buffer pool instance.(Constant for the buffer 
pool but each buffer pool can have different size)