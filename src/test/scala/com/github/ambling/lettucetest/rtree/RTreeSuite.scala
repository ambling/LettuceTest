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

import scala.io.{Codec, Source}

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
  val greekKey = "greek"

  after {
    syncCommands.flushall()
  }

  test("store data and range query") {
    val builder = new SpatialRTreeBuilder
    val profiler = new Profiler("construction")

    builder.store(connection, entries1kKey, entries1k)
    profiler.recordAndReset("store entries 1k")
    val entries1KPos = builder.load(connection, entries1kKey)
    profiler.recordAndReset("load entries 1k")

    entries1KPos.length should equal (1000)

    profiler.reset()
    val rtree = builder.buildRTree(entries1KPos)
    profiler.recordAndReset("construct in-mem rtree on entries 1k")

    builder.build(connection, entries1kKey, rtree)
    profiler.recordAndReset("store rtree on entries 1k")

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

    profiler.reset()
    RangeQuery(connection, entries1kKey, ranges(0))
    profiler.recordAndReset("range query on Redis 0")
    RangeQuery(rtree, ranges(0))
    profiler.recordAndReset("range query on RTree 0")
    RangeQuery(entries1k.toIterator, ranges(0))
    profiler.recordAndReset("range query on entries 0")

    RangeQuery(connection, entries1kKey, ranges(1))
    profiler.recordAndReset("range query on Redis 1")
    RangeQuery(rtree, ranges(1))
    profiler.recordAndReset("range query on RTree 1")
    RangeQuery(entries1k.toIterator, ranges(1))
    profiler.recordAndReset("range query on entries 1")

    RangeQuery(connection, entries1kKey, ranges(2))
    profiler.recordAndReset("range query on Redis 2")
    RangeQuery(rtree, ranges(2))
    profiler.recordAndReset("range query on RTree 2")
    RangeQuery(entries1k.toIterator, ranges(2))
    profiler.recordAndReset("range query on entries 2")

    RangeQuery(connection, entries1kKey, ranges(3))
    profiler.recordAndReset("range query on Redis 3")
    RangeQuery(rtree, ranges(3))
    profiler.recordAndReset("range query on RTree 3")
    RangeQuery(entries1k.toIterator, ranges(3))
    profiler.recordAndReset("range query on entries 3")

    RangeQuery(connection, entries1kKey, ranges(4))
    profiler.recordAndReset("range query on Redis 4")
    RangeQuery(rtree, ranges(4))
    profiler.recordAndReset("range query on RTree 4")
    RangeQuery(entries1k.toIterator, ranges(4))
    profiler.recordAndReset("range query on entries 4")

    println(profiler.printAll)
  }
}
