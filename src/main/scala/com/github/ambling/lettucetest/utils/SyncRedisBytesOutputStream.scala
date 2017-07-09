package com.github.ambling.lettucetest.utils

import java.io.{Closeable, OutputStream}
import java.nio.ByteBuffer

import com.lambdaworks.redis.api.StatefulRedisConnection
import com.lambdaworks.redis.api.sync.RedisCommands

/**
 * Output stream using synchronous API
 */
class SyncRedisBytesOutputStream(
    val connection: StatefulRedisConnection[String, ByteBuffer],
    val key: String) extends OutputStream with Closeable {

  var writed: Long = 0
  var closed: Boolean = false
  val syncCommand: RedisCommands[String, ByteBuffer] = connection.sync

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

  def write(buf: ByteBuffer): Unit = {
    val future = syncCommand.append(key, buf)
  }

  override def flush(): Unit = {
  }

  override def close(): Unit = {
    closed = true
  }
}
