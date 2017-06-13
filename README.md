# LettuceTest
A test of [lettuce](https://github.com/lettuce-io/lettuce-core) client for [redis](https://redis.io/).
The test is used for integration redis as a type of block store in [Spark](https://github.com/apache/spark).

In this test, data is stored in an asynchronous steaming fashion and retrieved with synchronous operation.

An additional K-V codec for String key and ByteBuffer value is also defined.

The test result has shown that Java's `WritableByteChannelImpl` is buffered and not applicable for the type of
output streams that do not flush immediately in a write operation. Therefore, we have reimplemented a new `WritableByteChannel` with neither buffer nor additional data copy.
