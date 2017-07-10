package com.github.ambling.lettucetest.rtree

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.Channels

import com.github.ambling.lettucetest.utils._
import com.github.davidmoten.rtree._
import com.github.davidmoten.rtree.geometry.{Geometries, Point, Rectangle}
import com.lambdaworks.redis.api.StatefulRedisConnection

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class SpatialRTreeBuilder(maxChildren: Int = 10) {

  def name: String = SpatialRTree.name
  private var nodeID = 1 // 0 for root

  def storeHashSync(
      connection: StatefulRedisConnection[String, ByteBuffer],
      blockID: String,
      data: Array[Point]): Unit = {
    val syncCommand = connection.sync()
    data.zipWithIndex.foreach { case (p, i) =>
      val buf = Point2Buffer.toBuffer(p)
      syncCommand.hset(blockID, i.toString, buf)
    }
  }

  def storeHashAsync(
      connection: StatefulRedisConnection[String, ByteBuffer],
      blockID: String,
      data: Array[Point]): Unit = {
    val syncCommand = connection.sync()
    val map = data.zipWithIndex.map { case (p, i) =>
      val buf = Point2Buffer.toBuffer(p)
      (i.toString, buf)
    }.toMap.asJava

    syncCommand.hmset(blockID, map)
  }

  def loadHashSync(
      connection: StatefulRedisConnection[String, ByteBuffer],
      blockID: String): Array[(Long, Point)] = {

    val syncCommand = connection.sync()
    val ids = syncCommand.hlen(blockID)
    (0L until ids).toArray.map { id =>
      val buf = syncCommand.hget(blockID, id.toString)
      (id, Point2Buffer.fromBuffer(buf))
    }
  }

  def loadHashAsync(
      connection: StatefulRedisConnection[String, ByteBuffer],
      blockID: String): Array[(Long, Point)] = {

    val syncCommand = connection.sync()
    syncCommand.hgetall(blockID).asScala
      .map(t => (t._1.toLong, Point2Buffer.fromBuffer(t._2)))
      .toArray
  }

  def storeSync(
      connection: StatefulRedisConnection[String, ByteBuffer],
      blockID: String,
      data: Array[Point]): Unit = {
    val stream = new SyncRedisBytesOutputStream(connection, blockID)
    store(stream, data)
  }

  def storeBufferedSync(
      connection: StatefulRedisConnection[String, ByteBuffer],
      blockID: String,
      data: Array[Point]): Unit = {
    val stream = new BufferedOutputStream(new SyncRedisBytesOutputStream(connection, blockID))
    store(stream, data)
  }

  def storeAsync(connection: StatefulRedisConnection[String, ByteBuffer],
                 blockID: String,
                 data: Array[Point]): Unit = {
    val stream = new AsyncRedisBytesOutputStream(connection, blockID)
    store(stream, data)
  }

  def storeChannel(
    connection: StatefulRedisConnection[String, ByteBuffer],
    blockID: String,
    data: Array[Point]): Unit = {
    val channel = new RedisBytesChannel(connection, blockID, true)
    val stream = Channels.newOutputStream(channel)
    store(stream, data)
  }

  def store(stream: OutputStream, data: Array[Point]): Unit = {
    val output = new DataOutputStream(stream)
    data.foreach { point =>
      val buf = Point2Buffer.toBuffer(point)
      output.writeInt(buf.remaining())
      output.write(buf.array(), buf.position(), buf.remaining())
    }
    output.flush()
  }

  def loadStream(stream: InputStream): Array[(Long, Point)] = {
    val input = new DataInputStream(stream)
    new Iterator[(Long, Point)] {
      var finished = false
      var pos = 0L

      override def hasNext: Boolean = input.available() > 0

      override def next(): (Long, Point) = {
        val len = input.readInt()
        val bytes = new Array[Byte](len)
        input.readFully(bytes)
        val buf = ByteBuffer.wrap(bytes)
        val point = Point2Buffer.fromBuffer(buf)
        val p = pos
        pos += 4 + len
        (p, point)
      }
    }.toArray
  }

  def loadChannel(
      connection: StatefulRedisConnection[String, ByteBuffer],
      blockID: String): Array[(Long, Point)] = {

    val channel = new RedisBytesChannel(connection, blockID, false)
    val stream = Channels.newInputStream(channel)
    loadStream(stream)
  }

  def loadSync(
      connection: StatefulRedisConnection[String, ByteBuffer],
      blockID: String): Array[(Long, Point)] = {
    val stream = new SyncRedisBytesInputStream(connection, blockID)
    loadStream(stream)
  }

  def loadSyncBuffered(
      connection: StatefulRedisConnection[String, ByteBuffer],
      blockID: String): Array[(Long, Point)] = {

    val stream = new BufferedInputStream(new SyncRedisBytesInputStream(connection, blockID))
    loadStream(stream)
  }

  def loadAsync(
      connection: StatefulRedisConnection[String, ByteBuffer],
      blockID: String): Array[(Long, Point)] = {

    // just load all data
    val data = connection.sync().get(blockID)
    val stream = new ByteArrayInputStream(data.array())
    loadStream(stream)
  }

  def buildRTree(data: Array[(Long, Point)]): RTree[Long, Point] = {
    val immutableEntries = data.map( elem => {
      Entries.entry[Long, Point](elem._1, elem._2)
    })
    val entries = ArrayBuffer(immutableEntries: _*).asJava

    // Note that immutable list would raise an UnsupportedOperationException
    RTree.maxChildren(maxChildren).create(entries)
  }

  def build(connection: StatefulRedisConnection[String, ByteBuffer],
            blockID: String,
            rtree: RTree[Long, Point]): Unit = {
    val writer = new IndexWriter(connection, blockID, name)

    if (rtree.root().isPresent) {
      // write data size and MBR
      val sizeBuf = ByteBuffer.allocate(4)
      sizeBuf.putInt(rtree.size())
      sizeBuf.flip()
      writer.writeNode("size", sizeBuf)

      val mbrBuf = ByteBuffer.allocate(8 * 4)
      val mbr = rtree.mbr().get()
      mbrBuf.putDouble(mbr.x1())
      mbrBuf.putDouble(mbr.x2())
      mbrBuf.putDouble(mbr.y1())
      mbrBuf.putDouble(mbr.y2())
      mbrBuf.flip()
      writer.writeNode("mbr", mbrBuf)

      val node = rtree.root().get()
      addNode(writer, node, root = true)
    }
  }

  def writeNode(writer: IndexWriter, buf: ByteBuffer, root: Boolean): Int = {
    if (root) {
      writer.writeNode(0.toString, buf)
      0
    } else {
      val prev = nodeID
      nodeID += 1
      writer.writeNode(prev.toString, buf)
      prev
    }
  }

  def addNode(writer: IndexWriter, node: Node[Long, Point], root: Boolean): Int = {
    val baos = new ByteArrayOutputStream()
    val stream = new DataOutputStream(baos)
    node match {
      case leaf: Leaf[Long, Point] =>
        stream.writeBoolean(true) // leaf node
        stream.writeInt(leaf.count())
        leaf.entries().asScala.foreach { entry =>
          val mbr = entry.geometry().mbr()
          stream.writeDouble(mbr.x1())
          stream.writeDouble(mbr.x2())
          stream.writeDouble(mbr.y1())
          stream.writeDouble(mbr.y2())
          stream.writeLong(entry.value())
        }
      case nonLeaf: NonLeaf[Long, Point] =>
        stream.writeBoolean(false) // nonLeaf node
        stream.writeInt(nonLeaf.count())
        nonLeaf.children().asScala.foreach { child =>
          val mbr = child.geometry().mbr()
          stream.writeDouble(mbr.x1())
          stream.writeDouble(mbr.x2())
          stream.writeDouble(mbr.y1())
          stream.writeDouble(mbr.y2())
          val childID = addNode(writer, child, root = false)
          stream.writeInt(childID)
        }
    }
    val bytes = baos.toByteArray
    val buf = ByteBuffer.wrap(bytes)
    writeNode(writer, buf, root)
  }
}

sealed trait RTreeNode
case class RTreeLeaf(positions: Array[(Rectangle, Long)]) extends RTreeNode
case class RTreeNonLeaf(children: Array[(Rectangle, Int)]) extends RTreeNode

object Point2Buffer {
  def fromBuffer(buf: ByteBuffer): Point = {
    val x = buf.getDouble
    val y = buf.getDouble
    Geometries.point(x, y)
  }

  def toBuffer(point: Point): ByteBuffer = {
    val buf = ByteBuffer.allocate(8 * 2)
    buf.putDouble(point.x())
    buf.putDouble(point.y())
    buf.flip()
    buf
  }
}

object SpatialRTree {
  val name = "spatial_rtree"

  def getSize(querier: IndexQuerier): Int = {
    val buf = querier.getNode("size")
    buf.getInt()
  }

  def getMBR(querier: IndexQuerier): Rectangle = {
    val buf = querier.getNode("mbr")
    val x1 = buf.getDouble()
    val x2 = buf.getDouble()
    val y1 = buf.getDouble()
    val y2 = buf.getDouble()
    Geometries.rectangle(x1, y1, x2, y2)
  }

  def getNode(buf: ByteBuffer): RTreeNode = {
    val bais = new ByteArrayInputStream(buf.array(), buf.arrayOffset(), buf.remaining())
    val input = new DataInputStream(bais)
    val isLeaf= input.readBoolean()
    val count = input.readInt()
    if (isLeaf) {
      val positions = new Array[(Rectangle, Long)](count)
      for (i <- 0 until count) {
        val x1 = input.readDouble()
        val x2 = input.readDouble()
        val y1 = input.readDouble()
        val y2 = input.readDouble()
        val pos = input.readLong()
        positions(i) = (Geometries.rectangle(x1, y1, x2, y2), pos)
      }
      RTreeLeaf(positions)
    } else {
      val children = new Array[(Rectangle, Int)](count)
      for (i <- 0 until count) {
        val x1 = input.readDouble()
        val x2 = input.readDouble()
        val y1 = input.readDouble()
        val y2 = input.readDouble()
        val child = input.readInt()
        children(i) = (Geometries.rectangle(x1, y1, x2, y2), child)
      }
      RTreeNonLeaf(children)
    }
  }
}
