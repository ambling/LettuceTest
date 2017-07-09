package com.github.ambling.lettucetest.utils

import scala.collection.mutable.ArrayBuffer

class Profiler(val pname: String) {
  var starttime: Long = System.nanoTime()

  type Event = (String, Long) // name, time

  val buffer: ArrayBuffer[Event] = ArrayBuffer()

  /**
    * clear all records
    */
  def clear(): Unit = {
    starttime = System.nanoTime()
    buffer.clear()
  }

  /**
    * reset the start time
    */
  def reset(): Unit = {
    starttime = System.nanoTime()
  }

  def record(name: String): Int = {
    val id = buffer.size
    buffer.append((name, System.nanoTime() - starttime))
    id
  }

  def recordAndReset(name: String): Int = {
    val re = record(name)
    reset()
    re
  }

  def timeElapse(id1: Int, id2: Int): Long = {
    (buffer(id2)._2 - buffer(id1)._2) / 1000000
  }

  def printTimeElapse(id1: Int, id2: Int): String = {
    val time = timeElapse(id1, id2)
    s"[$pname] From \'${buffer(id1)._1}\' to \'${buffer(id2)._1}\' takes $time ms"
  }

  def printAll: String = {
    val strings = buffer.map { event =>
      s"[$pname] ${event._1}: ${event._2 / 1000000}ms\n"
    }
    strings.foldLeft("")((l, r) => l + r)
  }
}
