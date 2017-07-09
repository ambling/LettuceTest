/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.ambling.lettucetest.utils

import java.nio.ByteBuffer

import com.lambdaworks.redis.api.StatefulRedisConnection

/**
 * A helper class to query index nodes from a Redis store.
 */
class IndexQuerier(
    val connection: StatefulRedisConnection[String, ByteBuffer],
    val blockId: String,
    val indexName: String) {

  private val syncCommand = connection.sync()
  private val key = IndexWriter.indexKey(blockId, indexName)

  def getNode(nodeKey: String): ByteBuffer = {
    syncCommand.hget(key, nodeKey)
  }
}

/**
 *
 * A helper class to write index nodes into a Redis store.
 */
class IndexWriter(
    val connection: StatefulRedisConnection[String, ByteBuffer],
    val blockId: String,
    val indexName: String) {

  private val syncCommand = connection.sync()
  private val allKeys = IndexWriter.allIndexesKey(blockId)
  private val key = IndexWriter.indexKey(blockId, indexName)

  // add this index name to the set of all index keys
  syncCommand.sadd(allKeys, ByteBuffer.wrap(indexName.getBytes))

  def writeNode(nodeKey: String, node: ByteBuffer): Unit = {
    syncCommand.hset(key, nodeKey, node)
  }
}

object IndexWriter {

  def indexKey(blockId: String, indexName: String): String = {
    blockId + "_" + indexName
  }

  def allIndexesKey(blockId: String): String = {
    blockId + "_index"
  }
}