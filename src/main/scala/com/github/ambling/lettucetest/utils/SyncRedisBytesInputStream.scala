package com.github.ambling.lettucetest.utils

import java.io.InputStream
import java.nio.ByteBuffer

import com.lambdaworks.redis.api.StatefulRedisConnection

/**
 *
 * @author ambling
 */
class SyncRedisBytesInputStream(
    val connection: StatefulRedisConnection[String, ByteBuffer],
    val key: String)
  extends InputStream {

  private val syncCommand = connection.sync()
  private var pos = 0L
  private val size = syncCommand.strlen(key)

  override def read(): Int = {
    val buf = syncCommand.getrange(key, pos, pos)
    pos += buf.remaining()
    buf.get() & 0xff
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    val buf = syncCommand.getrange(key, pos, pos + len - 1)
    val hasRead = buf.remaining()
    buf.get(b, off, buf.remaining())
    pos += hasRead
    hasRead
  }

  override def skip(n: Long): Long = {
    val newpos = Math.min(size - 1, pos + n)
    val skipped = newpos - pos
    pos = newpos
    skipped
  }

  override def available(): Int = {
    Math.min(size - pos, Integer.MAX_VALUE).toInt
  }
}
