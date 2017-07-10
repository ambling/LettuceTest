package com.github.ambling.lettucetest.rtree

import java.io.{BufferedInputStream, DataInputStream, InputStream}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, SeekableByteChannel}
import java.util

import com.github.ambling.lettucetest.utils.{IndexQuerier, RedisBytesChannel, SyncRedisBytesInputStream}
import com.github.davidmoten.rtree.RTree
import com.github.davidmoten.rtree.geometry.{Geometries, Point, Rectangle}
import com.lambdaworks.redis.api.StatefulRedisConnection

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * Query trajectory elements that are intersected with specific spatial range
  * with an optional period limitation.
  */
object RangeQuery {

  class LFilter(val range: Rectangle) {

    def map(elems: Iterator[Point]): Iterator[Point] = {
      elems.filter(elem => elem.intersects(range))
    }

    def necessaryIndices: Seq[String] = Seq(SpatialRTree.name)

    def fromStream(stream: InputStream, pos: Long): Point = {
      val dataInput = new DataInputStream(stream)

      val length = dataInput.readInt()
      val data = new Array[Byte](length)
      dataInput.readFully(data)
      val buf = ByteBuffer.wrap(data)

      Point2Buffer.fromBuffer(buf)
    }

    def fromChannel(channel: SeekableByteChannel, pos: Long): Point = {
      channel.position(pos)
      val input = Channels.newInputStream(channel)
      fromStream(input, pos)
    }

    def fromHash(connection: StatefulRedisConnection[String, ByteBuffer],
                 key: String,
                 pos: Long): Point = {
      val sync = connection.sync()
      val buf = sync.hget(key, pos.toString)
      Point2Buffer.fromBuffer(buf)
    }

    def query(connection: StatefulRedisConnection[String, ByteBuffer],
              blockID: String,
              queriers: Map[String, IndexQuerier],
              fetch: Boolean,
              useHash: Boolean,
              useChannel: Boolean,
              optimize: Boolean): Array[Point] = {

      val channel = new RedisBytesChannel(connection, blockID, false)

      val querier = queriers.getOrElse(SpatialRTree.name, null)
      // should always fill this stack to check hasNext
      val elements = new ArrayBuffer[Point]()
      val positions = new ArrayBuffer[Long]()
      val nodes = new util.Stack[Int]

      nodes.push(0) // root node
      while (!nodes.empty()) {
        val nodeID = nodes.pop()
        val buf = querier.getNode(nodeID.toString)
        if (buf != null) {
          val node = SpatialRTree.getNode(buf)
          processNode(node)
        }
      }

      def processNode(node: RTreeNode): Unit = node match {
        case leaf: RTreeLeaf =>
          leaf.positions.foreach { case (mbr, pos) =>
            if (mbr.intersects(range)) {
              if (fetch) {
                if (useChannel) elements.append(fromChannel(channel, pos))
                else if (useHash && !optimize) elements.append(fromHash(connection, blockID, pos))
                else positions.append(pos)
              }
              else elements.append(Geometries.point(mbr.x1(), mbr.y2()))
            }
          }
        case nonLeaf: RTreeNonLeaf =>
          nonLeaf.children.foreach { case (mbr, child) =>
            if (mbr.intersects(range)) nodes.push(child)
          }
      }

      if (fetch && !useChannel && useHash && optimize) {
        val fields = positions.map(_.toString)
        if (fields.isEmpty) Array()
        else {
          val bufs = connection.sync().hmget(blockID, fields: _*)
          bufs.asScala.map(Point2Buffer.fromBuffer).toArray
        }
      } else if (fetch && !useChannel && !useHash) {
        val sorted = positions.sorted

        val input = new SyncRedisBytesInputStream(connection, blockID)
        val stream = if (optimize) new BufferedInputStream(input) else input

        var oldPos = 0L
        sorted.map { pos =>
          var skip = pos - oldPos
          while (skip > 0) {
            val skipped = stream.skip(skip)
            skip -= skipped
          }
          oldPos = pos + 20 // 4 bytes size and 16 bytes point
          fromStream(stream, pos)
        }.toArray
      } else elements.toArray
    }
  }

  def apply(connection: StatefulRedisConnection[String, ByteBuffer],
            blockID: String,
            range: Rectangle,
            fetch: Boolean = true,
            useHash: Boolean = false,
            useChannel: Boolean = true,
            optimize: Boolean = false): Array[Point] = {
    val localFilter = new LFilter(range)

    val queriers = Map(
      (SpatialRTree.name, new IndexQuerier(connection, blockID, SpatialRTree.name)))
    localFilter.query(connection, blockID, queriers, fetch, useHash, useChannel, optimize)
  }

  def apply(elems: Iterator[Point],
            range: Rectangle): Array[Point] = {
    val localFilter = new LFilter(range)
    localFilter.map(elems).toArray
  }

  def apply(rtree: RTree[Long, Point],
            range: Rectangle): Array[Point] = {
    rtree.search(range).toBlocking.getIterator.asScala.map(_.geometry()).toArray
  }

}
