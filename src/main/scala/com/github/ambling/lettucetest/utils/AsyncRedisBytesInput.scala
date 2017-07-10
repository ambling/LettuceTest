package com.github.ambling.lettucetest.utils

import java.nio.ByteBuffer

import com.lambdaworks.redis.RedisFuture
import com.lambdaworks.redis.api.StatefulRedisConnection

import scala.collection.mutable.ArrayBuffer

/**
 * The aynchronous reading method dose not conform to the InputStream API,
 * as this method dose not return reading results immediately.
 */
class AsyncRedisBytesInput(
    val connection: StatefulRedisConnection[String, ByteBuffer],
    val key: String) {

  private val asyncCommand = connection.async()
  private val syncCommand = connection.sync()
  private var pos = 0L
  private val size = syncCommand.strlen(key)
  private val futures = new ArrayBuffer[RedisFuture[ByteBuffer]]

  def read(): Unit = {
    val future = asyncCommand.getrange(key, pos, pos)
    pos += 1
    futures.append(future)
  }

  def read(len: Int): Unit = {
    val reallen = Math.min(len, size - pos)
    val future = asyncCommand.getrange(key, pos, pos + reallen - 1)
    pos += reallen
    futures.append(future)
  }

  def skip(n: Long): Long = {
    val newpos = Math.min(size - 1, pos + n)
    val skipped = newpos - pos
    pos = newpos
    skipped
  }

  def get(): Array[ByteBuffer] = {
    val data = futures.map(_.get()).toArray
    futures.clear()
    data
  }

  def seek(p: Long): Unit = {
    pos =
      if (p < 0) size - p
      else p
  }

  def available(): Int = {
    Math.min(size - pos, Integer.MAX_VALUE).toInt
  }
}