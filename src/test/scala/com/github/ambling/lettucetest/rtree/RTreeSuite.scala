package com.github.ambling.lettucetest.rtree

import java.nio.ByteBuffer
import java.util.zip.GZIPInputStream

import com.github.ambling.lettucetest.utils.{Profiler, StringByteBufferCodec}
import com.github.davidmoten.rtree.geometry.{Geometries, Point}
import com.lambdaworks.redis.RedisClient
import com.lambdaworks.redis.api.StatefulRedisConnection
import com.lambdaworks.redis.api.sync.RedisCommands
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.io.Source

/**
 * Test of input and output of rtree in redis
 */
@RunWith(classOf[JUnitRunner])
class RTreeSuite extends FunSuite with ShouldMatchers with BeforeAndAfter {
  // start redis-server at localhost with default port
  val redisClient: RedisClient = RedisClient.create(s"redis://localhost")

  val connection: StatefulRedisConnection[String, ByteBuffer] =
    redisClient.connect(new StringByteBufferCodec)
  val syncCommands: RedisCommands[String, ByteBuffer] = connection.sync

  val entries1kPath = "/1000.txt"
  val greekPath = "/greek-earthquakes-1964-2000.txt.gz"

  def readFile(path: String, gz: Boolean): Array[Point] = {
    val input = getClass.getResource(path).openStream()
    val decompressed = if (gz) new GZIPInputStream(input) else input
    Source.fromInputStream(decompressed)
      .getLines()
      .map(line => line.split(" "))
      .filter(_.length > 1)
      .map(data => (data(0).toDouble, data(1).toDouble))
      .map(data => Geometries.point(data._1, data._2))
      .toArray
  }

  val entries1k: Array[Point] = readFile(entries1kPath, gz = false)
//  val greek: Array[Point] = readFile(greekPath, gz = true)

  val entries1kKey = "entries1k"
  val entries1kHashKey = "entries1k_hash"
  val greekKey = "greek"

  after {
    syncCommands.flushall()
  }

  test("store on string or hashmap") {

    val builder = new SpatialRTreeBuilder(100)
    val profiler = new Profiler("entries 1k")

    builder.storeChannel(connection, entries1kKey, entries1k)
    profiler.recordAndReset("channel store")

    val bufdata0 = syncCommands.get(entries1kKey).array()
    syncCommands.del(entries1kKey)

    profiler.reset()
    builder.storeSync(connection, entries1kKey, entries1k)
    profiler.recordAndReset("sync store")

    var bufdata1 = syncCommands.get(entries1kKey).array()
    bufdata0.toSeq should equal (bufdata1.toSeq)
    syncCommands.del(entries1kKey)

    profiler.reset()
    builder.storeBufferedSync(connection, entries1kKey, entries1k)
    profiler.recordAndReset("buffered sync store")

    bufdata1 = syncCommands.get(entries1kKey).array()
    bufdata0.toSeq should equal (bufdata1.toSeq)
    syncCommands.del(entries1kKey)

    profiler.reset()
    builder.storeAsync(connection, entries1kKey, entries1k)
    profiler.recordAndReset("async store")

    bufdata1 = syncCommands.get(entries1kKey).array()
    bufdata0.toSeq should equal (bufdata1.toSeq)
    syncCommands.del(entries1kKey)

    profiler.reset()
    builder.storeChannel(connection, entries1kKey, entries1k)
    profiler.recordAndReset("channel store")

    bufdata1 = syncCommands.get(entries1kKey).array()
    bufdata0.toSeq should equal (bufdata1.toSeq)

    // load

    profiler.reset()
    val dataWithPos1 = builder.loadChannel(connection, entries1kKey)
    profiler.recordAndReset("channel load")

    profiler.reset()
    var dataWithPos2 = builder.loadSyncBuffered(connection, entries1kKey)
    profiler.recordAndReset("sync buffered load")

    dataWithPos2.toSet should equal (dataWithPos1.toSet)

    profiler.reset()
    dataWithPos2 = builder.loadSync(connection, entries1kKey)
    profiler.recordAndReset("sync load")

    dataWithPos2.toSet should equal (dataWithPos1.toSet)

    profiler.reset()
    dataWithPos2 = builder.loadAsync(connection, entries1kKey)
    profiler.recordAndReset("async load")

    dataWithPos2.toSet should equal (dataWithPos1.toSet)

    profiler.reset()
    builder.loadChannel(connection, entries1kKey)
    profiler.recordAndReset("channel load")

    // hash store and load
    syncCommands.del(entries1kKey)

    profiler.reset()
    builder.storeHashSync(connection, entries1kKey, entries1k)
    profiler.recordAndReset("hash sync store")

    syncCommands.del(entries1kKey)

    profiler.reset()
    builder.storeHashAsync(connection, entries1kKey, entries1k)
    profiler.recordAndReset("hash async store")

    syncCommands.del(entries1kKey)

    profiler.reset()
    builder.storeHashSync(connection, entries1kKey, entries1k)
    profiler.recordAndReset("hash sync store")

    profiler.reset()
    val dataWithHash1 = builder.loadHashAsync(connection, entries1kKey)
    profiler.recordAndReset("hash async load")

    profiler.reset()
    val dataWithHash2 = builder.loadHashSync(connection, entries1kKey)
    profiler.recordAndReset("hash sync load")

    dataWithHash1.toSet should equal (dataWithHash2.toSet)

    profiler.reset()
    builder.loadHashAsync(connection, entries1kKey)
    profiler.recordAndReset("hash async load")

    println(profiler.printAll)
  }

  test("store data and range query") {
    val builder = new SpatialRTreeBuilder(100)
    val profiler = new Profiler("entries 1k")

    builder.storeBufferedSync(connection, entries1kKey, entries1k)
    profiler.recordAndReset("buffered sync store")

    builder.storeHashAsync(connection, entries1kHashKey, entries1k)
    profiler.recordAndReset("buffered sync store hash")

    profiler.reset()
    val entries1KPos = builder.loadAsync(connection, entries1kKey)
    profiler.recordAndReset("load")

    val entries1KHashPos = builder.loadHashAsync(connection, entries1kHashKey)
    profiler.recordAndReset("load hash")

    entries1KPos.length should equal (1000)
    entries1KHashPos.length should equal (1000)

    profiler.reset()
    val rtree = builder.buildRTree(entries1KPos)
    profiler.recordAndReset("construct in-mem rtree")

    val rtreeHash = builder.buildRTree(entries1KHashPos)
    profiler.recordAndReset("construct in-mem rtree hash")

    builder.build(connection, entries1kKey, rtree)
    profiler.recordAndReset("store rtree")

    builder.build(connection, entries1kHashKey, rtreeHash)
    profiler.recordAndReset("store rtree hash")

    entries1k.length should equal (1000)
    val sizebuf = syncCommands.hget(entries1kKey + "_" + SpatialRTree.name, "size")
    sizebuf.getInt() should equal (1000)
    val mbrbuf = syncCommands.hget(entries1kKey + "_" + SpatialRTree.name, "mbr")
    val x1 = mbrbuf.getDouble()
    val x2 = mbrbuf.getDouble()
    val y1 = mbrbuf.getDouble()
    val y2 = mbrbuf.getDouble()
    println(x1 + "," + x2 + "," + y1 + "," + y2)

    val dist = Array(5, 10, 50, 100, 500)
    val ranges = dist.map(d => Geometries.rectangle(500 - d, 500 - d, 500 + d, 500 + d))

    for (i <- List(0, 1, 2, 3, 4)) {

      profiler.reset()
      val re0 = RangeQuery(connection, entries1kKey, ranges(i), fetch = false)
      profiler.recordAndReset(s"range query without data on Redis $i")
      val re1 = RangeQuery(connection, entries1kKey, ranges(i))
      profiler.recordAndReset(s"range query on Redis with channel $i")
      val re2 = RangeQuery(connection, entries1kKey, ranges(i), useChannel = false)
      profiler.recordAndReset(s"range query on Redis with sync input $i")
      val re3 = RangeQuery(connection, entries1kKey, ranges(i), useChannel = false, optimize = true)
      profiler.recordAndReset(s"range query on Redis with buffered sync input $i")
      val re4 = RangeQuery(rtree, ranges(i))
      profiler.recordAndReset(s"range query on RTree $i")
      val re5 = RangeQuery(entries1k.toIterator, ranges(i))
      profiler.recordAndReset(s"range query on entries $i")
      RangeQuery(connection, entries1kKey, ranges(i), fetch = false)
      profiler.recordAndReset(s"range query without data on Redis $i")

      re1.toSet should equal (re2.toSet)
      re1.toSet should equal (re3.toSet)
      re1.toSet should equal (re4.toSet)
      re1.toSet should equal (re5.toSet)

      profiler.reset()
      val re6 = RangeQuery(connection, entries1kHashKey, ranges(i),
        useChannel = false, useHash = true)
      profiler.recordAndReset(s"range query on Redis with hash $i")
      val re7 = RangeQuery(connection, entries1kHashKey, ranges(i),
        useChannel = false, useHash = true, optimize = true)
      profiler.recordAndReset(s"range query on Redis with aync hash $i")
      RangeQuery(connection, entries1kHashKey, ranges(i), useChannel = false, useHash = true)
      profiler.recordAndReset(s"range query on Redis with hash $i")

      re1.toSet should equal (re6.toSet)
      re1.toSet should equal (re7.toSet)
    }

    println(profiler.printAll)
  }

}
