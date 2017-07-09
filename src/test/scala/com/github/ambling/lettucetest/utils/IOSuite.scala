package com.github.ambling.lettucetest.utils

import java.nio.ByteBuffer

import com.lambdaworks.redis.RedisClient
import com.lambdaworks.redis.api.StatefulRedisConnection
import com.lambdaworks.redis.api.sync.RedisCommands
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

/**
 * Test of input and output of data in redis
 */
@RunWith(classOf[JUnitRunner])
class IOSuite extends FunSuite with ShouldMatchers {

  val sockpath = "/tmp/redis.sock"
  val redisClient = RedisClient.create(s"redis-socket://$sockpath")

  val connection: StatefulRedisConnection[String, ByteBuffer] =
    redisClient.connect(new StringByteBufferCodec)
  val syncCommands: RedisCommands[String, ByteBuffer] = connection.sync

  test("string") {
    val key = "test_key"
    val data = "test_data".getBytes

    // incase the key is not clear
    syncCommands.del(key)

    // use single write
    var inputsteam = new RedisBytesOutputStream(connection, key)
    for (byte <- data) inputsteam.write(byte)
    inputsteam.flush
    inputsteam.writed should equal(data.length)
    var retrieved = syncCommands.get(key)
    retrieved.array().deep should equal (data.deep)

    syncCommands.del(key)
    // use all write
    inputsteam = new RedisBytesOutputStream(connection, key)
    inputsteam.write(data)
    inputsteam.flush
    inputsteam.writed should equal(data.length)
    retrieved = syncCommands.get(key)
    retrieved.array().deep should equal (data.deep)

    syncCommands.del(key)
    // use offset write
    inputsteam = new RedisBytesOutputStream(connection, key)
    inputsteam.write(data, 0, 4)
    inputsteam.flush
    inputsteam.writed should equal(4)
    retrieved = syncCommands.get(key)
    retrieved.array().deep should not equal data.deep
    val subdata = "test".getBytes
    retrieved.array().deep should equal (subdata.deep)

  }

  test("incremental array") {
    val alldata = ByteBuffer.allocate(4000000)
    for (i <- 0 until 1000000) alldata.putInt(i)
    alldata.flip()

    val key = "test"

    // 400
    var length = 400
    var data = alldata.duplicate()
    data.limit(length)
    data.remaining() should equal (length)
    syncCommands.del(key)
    var inputsteam = new RedisBytesOutputStream(connection, key)
    var channel = inputsteam.getChannel
    channel.write(data)
    channel.close()
    data.position(0)
    var retrieved = syncCommands.get(key)
    retrieved.remaining() should equal(length)
    data.equals(retrieved) should equal(true)

    // 4000
    length = 4000
    data = alldata.duplicate()
    data.limit(length)
    data.remaining() should equal (length)
    syncCommands.del(key)
    inputsteam = new RedisBytesOutputStream(connection, key)
    channel = inputsteam.getChannel
    channel.write(data)
    channel.close()
    data.position(0)
    retrieved = syncCommands.get(key)
    retrieved.remaining() should equal(length)
    data.equals(retrieved) should equal(true)

    length = 4000
    data = alldata.duplicate()
    data.limit(length)
    data.remaining() should equal (length)
    syncCommands.del(key)
    inputsteam = new RedisBytesOutputStream(connection, key)
    channel = inputsteam.getChannel
    channel.write(data)
    channel.close()
    data.position(0)
    retrieved = syncCommands.get(key)
    retrieved.remaining() should equal(length)
    data.equals(retrieved) should equal(true)

    length = 40000
    data = alldata.duplicate()
    data.limit(length)
    data.remaining() should equal (length)
    syncCommands.del(key)
    inputsteam = new RedisBytesOutputStream(connection, key)
    channel = inputsteam.getChannel
    channel.write(data)
    channel.close()
    data.position(0)
    retrieved = syncCommands.get(key)
    retrieved.remaining() should equal(length)
    data.equals(retrieved) should equal(true)

    length = 400000
    data = alldata.duplicate()
    data.limit(length)
    data.remaining() should equal (length)
    syncCommands.del(key)
    inputsteam = new RedisBytesOutputStream(connection, key)
    channel = inputsteam.getChannel
    channel.write(data)
    channel.close()
    data.position(0)
    retrieved = syncCommands.get(key)
    retrieved.remaining() should equal(length)
    data.equals(retrieved) should equal(true)

    length = 4000000
    data = alldata.duplicate()
    data.limit(length)
    data.remaining() should equal (length)
    syncCommands.del(key)
    inputsteam = new RedisBytesOutputStream(connection, key)
    channel = inputsteam.getChannel
    channel.write(data)
    channel.close()
    data.position(0)
    retrieved = syncCommands.get(key)
    retrieved.remaining() should equal(length)
    data.equals(retrieved) should equal(true)
  }
}
