package com.github.ambling.lettucetest.rtree

import java.io.DataInputStream
import java.nio.ByteBuffer
import java.nio.channels.{Channels, SeekableByteChannel}
import java.util

import com.github.ambling.lettucetest.utils.{IndexQuerier, RedisBytesChannel}
import com.github.davidmoten.rtree.RTree
import com.github.davidmoten.rtree.geometry.{Geometries, Point, Rectangle}
import com.lambdaworks.redis.api.StatefulRedisConnection

import scala.collection.JavaConverters._

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

    def fromChannel(channel: SeekableByteChannel, pos: Long): Point = {
      channel.position(pos)
      val input = Channels.newInputStream(channel)
      val dataInput = new DataInputStream(input)

      val length = dataInput.readInt()
      val data = new Array[Byte](length)
      dataInput.readFully(data)
      val buf = ByteBuffer.wrap(data)

      Point2Buffer.fromBuffer(buf)
    }

    def query(queriers: Map[String, IndexQuerier],
              channel: SeekableByteChannel,
              fetch: Boolean)
    : Iterator[Point] = new Iterator[Point] {

      private val querier = queriers.getOrElse(SpatialRTree.name, null)
      // should always fill this stack to check hasNext
      private val elements = new util.Stack[Point]
      private val nodes = new util.Stack[Int]

      nodes.push(0) // root node
      fill()

      def processNode(node: RTreeNode): Unit = node match {
        case leaf: RTreeLeaf =>
          leaf.positions.foreach { case (mbr, pos) =>
            if (mbr.intersects(range)) {
              val elem =
                if (fetch) fromChannel(channel, pos)
                else Geometries.point(mbr.x1(), mbr.y2())
              elements.push(elem)
            }
          }
        case nonLeaf: RTreeNonLeaf =>
          nonLeaf.children.foreach { case (mbr, child) =>
            if (mbr.intersects(range)) nodes.push(child)
          }
      }

      def fill(): Unit = {
        // query next elements to the stack
        while (elements.empty() && !nodes.empty()) {
          val nodeID = nodes.pop()
          val buf = querier.getNode(nodeID.toString)
          if (buf != null) {
            val node = SpatialRTree.getNode(buf)
            processNode(node)
          }
        }
      }

      override def hasNext: Boolean = !elements.empty()

      override def next(): Point = {
        val elemNext = elements.pop()

        fill()
        elemNext
      }
    }
  }

  def apply(connection: StatefulRedisConnection[String, ByteBuffer],
            blockID: String,
            range: Rectangle,
            fetch: Boolean): Array[Point] = {
    val localFilter = new LFilter(range)

    val queriers = Map(
      (SpatialRTree.name, new IndexQuerier(connection, blockID, SpatialRTree.name)))
    val channel = new RedisBytesChannel(connection, blockID, false)
    localFilter.query(queriers, channel, fetch).toArray
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
