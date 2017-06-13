package com.github.ambling.lettucetest

import java.io.{Closeable, OutputStream}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, WritableByteChannel}

import com.lambdaworks.redis.RedisFuture
import com.lambdaworks.redis.api.StatefulRedisConnection
import com.lambdaworks.redis.api.async.RedisAsyncCommands

import scala.collection.mutable.ArrayBuffer

/**
 * An output stream that write (append) bytes to a string value with a specific key
 */
class RedisBytesOutputStream(
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
    val future = asyncCommand.append(key, buf)
    futures += future
  }

  override def write(b: Array[Byte]): Unit = {
    val future = asyncCommand.append(key, ByteBuffer.wrap(b))
    futures += future
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    val buf = ByteBuffer.wrap(b, off, len)
    val future = asyncCommand.append(key, buf)
    futures += future
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
    Channels.newChannel(this)
  }
}
