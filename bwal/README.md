### TODO
- [x] Implement truncate front 
- [x] Unit tests
- [ ] Check type conversions that has overflow risk
- [x] Test what happens when last segment is empty(with header)
- [x] Test what happens when last segment is an empty file(no header)
- [x] Test opening segment writer and reader when there is an uninitialized segment
- [ ] Test readers and writers properly close each file
- [x] Retry and panic eventually when flush or swap fails
- [x] ErrAtStart when seeking front truncated parts 
- [x] Make truncates fail-safe and retryable. Make sure it does not leave segments in an corrupted state in case of error. 
- [ ] Test truncate whole
- [ ] What to do when reader truncates seek position?
- [ ] When flush retries, logger assumes in case of errors nothing is written. That might not be true.

### NOTES
* How to handle errors? 
* LSN is the logical bytes offset of the log record
* Each segment's size is its logical_size + header_size

```shell
go test -v ./...
```