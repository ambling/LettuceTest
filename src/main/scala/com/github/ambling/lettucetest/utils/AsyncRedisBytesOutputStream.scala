package com.github.ambling.lettucetest.utils

import java.io.{Closeable, OutputStream}
import java.nio.ByteBuffer
import java.nio.channels.spi.AbstractInterruptibleChannel
import java.nio.channels.{Channels, WritableByteChannel}

import com.lambdaworks.redis.RedisFuture
import com.lambdaworks.redis.api.StatefulRedisConnection
import com.lambdaworks.redis.api.async.RedisAsyncCommands

import scala.collection.mutable.ArrayBuffer

/**
 * An output stream that write (append) bytes to a string value with a specific key
 */
class AsyncRedisBytesOutputStream(
    val connection: StatefulRedisConnection[String, ByteBuffer],
    val key: String)
  extends OutputStream with Closeable {

  var writed: Long = 0
  var closed: Boolean = false
  val futures: ArrayBuffer[RedisFuture[java.lang.Long]] =
    new ArrayBuffer[RedisFuture[java.lang.Long]]

  // use asynchronous commend to write data
  val asyncCommand: RedisAsyncCommands[String, ByteBuffer] = connection.async()

  override def write(b: Int): Unit = {
    val buf = ByteBuffer.allocate(4)
    buf.putInt(b)
    buf.flip()
    buf.position(3)
    write(buf)
  }

  override def write(b: Array[Byte]): Unit = {
    write(ByteBuffer.wrap(b))
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    val buf = ByteBuffer.wrap(b, off, len)
    write(buf)
  }

  /**
   * convenient write content in a bytebuffer
   */
  def write(buf: ByteBuffer): Unit = {
    val future = asyncCommand.append(key, buf)
    futures.append(future)
  }

  override def flush(): Unit = {
    // TODO to be thread-safe
    for (future <- futures) writed = future.get()
    futures.clear()
  }

  override def close(): Unit = {
    closed = true
    flush()
  }

  def getChannel: WritableByteChannel = {
    new UnbufferedWritableByteChannel(this)
  }
}

/**
 * The WritableByteChannelImpl class defined in [[Channels]] is actually buffered and may lead to
 * error if each write operation is not flushed immediately.
 *
 * The buffer is actually unnecessary since the input is already a buffer itself.
 */
class UnbufferedWritableByteChannel(val out: AsyncRedisBytesOutputStream)
  extends AbstractInterruptibleChannel with WritableByteChannel {

  private val TRANSFER_SIZE = 8192
  private var open = true

  override def write(src: ByteBuffer): Int = {
    val len = src.remaining
    var totalWritten = 0
    this.synchronized {

      while (totalWritten < len) {
        val bytesToWrite = Math.min(len - totalWritten, TRANSFER_SIZE)
        try {
          begin()
          val buf = src.duplicate()
          buf.limit(buf.position() + bytesToWrite)
          out.write(buf)
          src.position(src.position() + bytesToWrite)
        } finally end(bytesToWrite > 0)
        totalWritten += bytesToWrite
      }
      return totalWritten
    }
  }

  override protected def implCloseChannel(): Unit = {
    out.close()
    open = false
  }
}
