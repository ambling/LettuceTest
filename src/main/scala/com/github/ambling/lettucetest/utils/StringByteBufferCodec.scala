package com.github.ambling.lettucetest.utils

import java.nio.ByteBuffer
import java.nio.charset.Charset

import com.lambdaworks.redis.codec.RedisCodec
import io.netty.buffer.Unpooled

/**
 * A codec for redis client.
 * Note: the key should only encoded with ASCII charset
 */
class StringByteBufferCodec extends RedisCodec[String, ByteBuffer] {

  val charset: Charset = Charset.forName("US-ASCII")

  override def decodeKey(bytes: ByteBuffer): String = {
    Unpooled.wrappedBuffer(bytes).toString(charset)
  }

  override def encodeKey(key: String): ByteBuffer = {
    ByteBuffer.wrap(key.getBytes)
  }

  override def encodeValue(value: ByteBuffer): ByteBuffer = {
    if (value == null) ByteBuffer.wrap(Array[Byte]())
    else value.duplicate()
  }

  override def decodeValue(bytes: ByteBuffer): ByteBuffer = {
    val data = new Array[Byte](bytes.remaining())
    bytes.get(data)
    ByteBuffer.wrap(data)
  }

}
