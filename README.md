# LettuceTest
A test of [lettuce](https://github.com/lettuce-io/lettuce-core) client for [redis](https://redis.io/).
The test is used for integrating redis as a type of block store in [Spark](https://github.com/apache/spark).

In this test, data is stored in an asynchronous steaming fashion and retrieved with synchronous operation.

An additional K-V codec for String key and ByteBuffer value is also defined.

The test result has shown that Java's `WritableByteChannelImpl` is buffered and not applicable for the type of
output streams that do not flush immediately in a write operation. Therefore, we have reimplemented a new `WritableByteChannel` with neither buffer nor additional data copy.

#### Result

The simple performance benchmark shows that different data store/load fashions 
have tremendous performance gap (up to 500x). That means the massive I/O operations on 
Redis should be carefully designed and optimized. Refer to the benchmark [result](/result.txt).