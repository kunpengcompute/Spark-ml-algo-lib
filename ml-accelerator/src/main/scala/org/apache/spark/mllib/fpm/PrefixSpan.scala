/*
* Copyright (C) 2021. Huawei Technologies Co., Ltd.
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
* */
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

package org.apache.spark.mllib.fpm

import java.{lang => jl, util => ju}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * A parallel PrefixSpan algorithm to mine frequent sequential patterns.
 * The PrefixSpan algorithm is described in J. Pei, et al., PrefixSpan: Mining Sequential Patterns
 * Efficiently by Prefix-Projected Pattern Growth
 * (see <a href="http://doi.org/10.1109/ICDE.2001.914830">here</a>).
 *
 * @param minSupport the minimal support level of the sequential pattern, any pattern that appears
 *                   more than (minSupport * size-of-the-dataset) times will be output
 * @param maxPatternLength the maximal length of the sequential pattern, any pattern that appears
 *                         less than maxPatternLength will be output
 * @param maxLocalProjDBSize The maximum number of items (including delimiters used in the internal
 *                           storage format) allowed in a projected database before local
 *                           processing. If a projected database exceeds this size, another
 *                           iteration of distributed prefix growth is run.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Sequential_Pattern_Mining">Sequential Pattern Mining
 * (Wikipedia)</a>
 */
@Since("1.5.0")
class PrefixSpan private (
    private var minSupport: Double,
    private var maxPatternLength: Int,
    private var maxLocalProjDBSize: Long) extends Logging with Serializable {
  import PrefixSpan._

  /**
   * Constructs a default instance with default parameters
   * {minSupport: `0.1`, maxPatternLength: `10`, maxLocalProjDBSize: `32000000L`}.
   */
  @Since("1.5.0")
  def this() = this(0.1, 10, 32000000L)

  /**
   * Get the minimal support (i.e. the frequency of occurrence before a pattern is considered
   * frequent).
   */
  @Since("1.5.0")
  def getMinSupport: Double = minSupport

  /**
   * Sets the minimal support level (default: `0.1`).
   */
  @Since("1.5.0")
  def setMinSupport(minSupport: Double): this.type = {
    require(minSupport >= 0 && minSupport <= 1,
      s"The minimum support value must be in [0, 1], but got $minSupport.")
    this.minSupport = minSupport
    this
  }

  /**
   * Gets the maximal pattern length (i.e. the length of the longest sequential pattern to consider.
   */
  @Since("1.5.0")
  def getMaxPatternLength: Int = maxPatternLength

  /**
   * Sets maximal pattern length (default: `10`).
   */
  @Since("1.5.0")
  def setMaxPatternLength(maxPatternLength: Int): this.type = {
    // TODO: support unbounded pattern length when maxPatternLength = 0
    require(maxPatternLength >= 1,
      s"The maximum pattern length value must be greater than 0, but got $maxPatternLength.")
    this.maxPatternLength = maxPatternLength
    this
  }

  /**
   * Gets the maximum number of items allowed in a projected database before local processing.
   */
  @Since("1.5.0")
  def getMaxLocalProjDBSize: Long = maxLocalProjDBSize

  /**
   * Sets the maximum number of items (including delimiters used in the internal storage format)
   * allowed in a projected database before local processing (default: `32000000L`).
   */
  @Since("1.5.0")
  def setMaxLocalProjDBSize(maxLocalProjDBSize: Long): this.type = {
    require(maxLocalProjDBSize >= 0L,
      s"The maximum local projected database size must be nonnegative, but got $maxLocalProjDBSize")
    this.maxLocalProjDBSize = maxLocalProjDBSize
    this
  }

  /**
   * Finds the complete set of frequent sequential patterns in the input sequences of itemsets.
   * @param data sequences of itemsets.
   * @return a [[PrefixSpanModel]] that contains the frequent patterns
   */
  @Since("1.5.0")
  def run[Item: ClassTag](data: RDD[Array[Array[Item]]]): PrefixSpanModel[Item] = {
    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("Input data is not cached.")
    }

    val totalCount = data.count()
    logInfo(s"number of sequences: $totalCount")
    val minCount = math.ceil(minSupport * totalCount).toLong
    logInfo(s"minimum count for a frequent pattern: $minCount")

    // Find frequent items.
    val freqItems = findFrequentItems(data, minCount)
    logInfo(s"number of frequent items: ${freqItems.length}")

    // Keep only frequent items from input sequences and convert them to internal storage.
    val itemToInt = freqItems.zipWithIndex.toMap
    val dataInternalRepr = toDatabaseInternalRepr(data, itemToInt)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val results = genFreqPatterns(dataInternalRepr, minCount, maxPatternLength, maxLocalProjDBSize)

    def toPublicRepr(pattern: Array[Int]): Array[Array[Item]] = {
      val sequenceBuilder = mutable.ArrayBuilder.make[Array[Item]]
      val itemsetBuilder = mutable.ArrayBuilder.make[Item]
      val n = pattern.length
      var i = 1
      while (i < n) {
        val x = pattern(i)
        if (x == 0) {
          sequenceBuilder += itemsetBuilder.result()
          itemsetBuilder.clear()
        } else {
          itemsetBuilder += freqItems(x - 1) // using 1-indexing in internal format
        }
        i += 1
      }
      sequenceBuilder.result()
    }

    val freqSequences = results.map { case (seq: Array[Int], count: Long) =>
      new FreqSequence(toPublicRepr(seq), count)
    }
    new PrefixSpanModel(freqSequences)
  }

  /**
   * A Java-friendly version of `run()` that reads sequences from a `JavaRDD` and returns
   * frequent sequences in a [[PrefixSpanModel]].
   * @param data ordered sequences of itemsets stored as Java Iterable of Iterables
   * @tparam Item item type
   * @tparam Itemset itemset type, which is an Iterable of Items
   * @tparam Sequence sequence type, which is an Iterable of Itemsets
   * @return a [[PrefixSpanModel]] that contains the frequent sequential patterns
   */
  @Since("1.5.0")
  def run[Item, Itemset <: jl.Iterable[Item], Sequence <: jl.Iterable[Itemset]](
      data: JavaRDD[Sequence]): PrefixSpanModel[Item] = {
    implicit val tag = fakeClassTag[Item]
    run(data.rdd.map(_.asScala.map(_.asScala.toArray).toArray))
  }

}

@Since("1.5.0")
object PrefixSpan extends Logging {

  /**
   * This methods finds all frequent items in a input dataset.
   *
   * @param data Sequences of itemsets.
   * @param minCount The minimal number of sequence an item should be present in to be frequent
   *
   * @return An array of Item containing only frequent items.
   */
  private[fpm] def findFrequentItems[Item: ClassTag](
      data: RDD[Array[Array[Item]]],
      minCount: Long): Array[Item] = {

    data.flatMap { itemsets =>
      val uniqItems = mutable.Set.empty[Item]
      itemsets.foreach(set => uniqItems ++= set)
      uniqItems.toIterator.map((_, 1L))
    }.reduceByKey(_ + _).filter { case (_, count) =>
      count >= minCount
    }.sortBy(v => (-v._2, v._1.hashCode())).map(_._1).collect()
  }

  /**
   * This methods cleans the input dataset from un-frequent items, and translate it's item
   * to their corresponding Int identifier.
   *
   * @param data Sequences of itemsets.
   * @param itemToInt A map allowing translation of frequent Items to their Int Identifier.
   *                  The map should only contain frequent item.
   *
   * @return The internal repr of the inputted dataset. With properly placed zero delimiter.
   */
  private[fpm] def toDatabaseInternalRepr[Item: ClassTag](
      data: RDD[Array[Array[Item]]],
      itemToInt: Map[Item, Int]): RDD[Array[Int]] = {

    data.flatMap { itemsets =>
      val allItems = mutable.ArrayBuilder.make[Int]
      var containsFreqItems = false
      allItems += 0
      itemsets.foreach { itemsets =>
        val items = mutable.ArrayBuilder.make[Int]
        itemsets.foreach { item =>
          if (itemToInt.contains(item)) {
            items += itemToInt(item) + 1 // using 1-indexing in internal format
          }
        }
        val result = items.result()
        if (result.nonEmpty) {
          containsFreqItems = true
          allItems ++= result.sorted
          allItems += 0
        }
      }
      if (containsFreqItems) {
        Iterator.single(allItems.result())
      } else {
        Iterator.empty
      }
    }
  }

  /**
   * Find the complete set of frequent sequential patterns in the input sequences.
   * @param data ordered sequences of itemsets. We represent a sequence internally as Array[Int],
   *             where each itemset is represented by a contiguous sequence of distinct and ordered
   *             positive integers. We use 0 as the delimiter at itemset boundaries, including the
   *             first and the last position.
   * @return an RDD of (frequent sequential pattern, count) pairs,
   * @see [[Postfix]]
   */
  private[fpm] def genFreqPatterns(
      data: RDD[Array[Int]],
      minCount: Long,
      maxPatternLength: Int,
      maxLocalProjDBSize: Long): RDD[(Array[Int], Long)] = {
    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("Input data is not cached.")
    }
    PrefixSpanUtils.computePatterns(data, minCount, maxPatternLength, maxLocalProjDBSize)
  }

  /**
   * Represents a frequent sequence.
   * @param sequence a sequence of itemsets stored as an Array of Arrays
   * @param freq frequency
   * @tparam Item item type
   */
  @Since("1.5.0")
  class FreqSequence[Item] @Since("1.5.0") (
      @Since("1.5.0") val sequence: Array[Array[Item]],
      @Since("1.5.0") val freq: Long) extends Serializable {
    /**
     * Returns sequence as a Java List of lists for Java users.
     */
    @Since("1.5.0")
    def javaSequence: ju.List[ju.List[Item]] = sequence.map(_.toList.asJava).toList.asJava
  }
}
