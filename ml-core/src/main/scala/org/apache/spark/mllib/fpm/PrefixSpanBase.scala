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

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

  /**
   * Represents a prefix.
   * @param items items in this prefix, using the internal format
   * @param length length of this prefix, not counting 0
   */
  private[fpm] class Prefix (val items: Array[Int], val length: Int) extends Serializable {

    /** A unique id for this prefix. */
    val id: Int = Prefix.nextId

    /** Expands this prefix by the input item. */
    def :+(item: Int): Prefix = {
      require(item != 0)
      if (item < 0) {
        new Prefix(items :+ -item, length + 1)
      } else {
        new Prefix(items ++ Array(0, item), length + 1)
      }
    }
  }

  private[fpm] object Prefix {
    /** Internal counter to generate unique IDs. */
    private val counter: AtomicInteger = new AtomicInteger(-1)

    /** Gets the next unique ID. */
    private def nextId: Int = counter.incrementAndGet()

    /** An empty [[Prefix]] instance. */
    val empty: Prefix = new Prefix(Array.empty, 0)
  }

  /**
   * An internal representation of a postfix from some projection.
   * We use one int array to store the items, which might also contains other items from the
   * original sequence.
   * Items are represented by positive integers, and items in each itemset must be distinct and
   * ordered.
   * we use 0 as the delimiter between itemsets.
   * For example, a sequence `(12)(31)1` is represented by `[0, 1, 2, 0, 1, 3, 0, 1, 0]`.
   * The postfix of this sequence w.r.t. to prefix `1` is `(_2)(13)1`.
   * We may reuse the original items array `[0, 1, 2, 0, 1, 3, 0, 1, 0]` to represent the postfix,
   * and mark the start index of the postfix, which is `2` in this example.
   * So the active items in this postfix are `[2, 0, 1, 3, 0, 1, 0]`.
   * We also remember the start indices of partial projections, the ones that split an itemset.
   * For example, another possible partial projection w.r.t. `1` is `(_3)1`.
   * We remember the start indices of partial projections, which is `[2, 5]` in this example.
   * This data structure makes it easier to do projections.
   *
   * @param items a sequence stored as `Array[Int]` containing this postfix
   * @param start the start index of this postfix in items
   * @param partialStarts start indices of possible partial projections, strictly increasing
   */
  private[fpm] class Postfix(
      val items: Array[Int],
      val start: Int = 0,
      val partialStarts: Array[Int] = Array.empty) extends Serializable {

    require(items.last == 0, s"The last item in a postfix must be zero, but got ${items.last}.")
    if (partialStarts.nonEmpty) {
      require(partialStarts.head >= start,
        "The first partial start cannot be smaller than the start index," +
          s"but got partialStarts.head = ${partialStarts.head} < start = $start.")
    }

    /**
     * Start index of the first full itemset contained in this postfix.
     */
    private[this] def fullStart: Int = {
      var i = start
      while (items(i) != 0) {
        i += 1
      }
      i
    }

    /**
     * Generates length-1 prefix items of this postfix with the corresponding postfix sizes.
     * There are two types of prefix items:
     *   a) The item can be assembled to the last itemset of the prefix. For example,
     *      the postfix of `<(12)(123)>1` w.r.t. `<1>` is `<(_2)(123)1>`. The prefix items of this
     *      postfix can be assembled to `<1>` is `_2` and `_3`, resulting new prefixes `<(12)>` and
     *      `<(13)>`. We flip the sign in the output to indicate that this is a partial prefix item.
     *   b) The item can be appended to the prefix. Taking the same example above, the prefix items
     *      can be appended to `<1>` is `1`, `2`, and `3`, resulting new prefixes `<11>`, `<12>`,
     *      and `<13>`.
     * @return an iterator of (prefix item, corresponding postfix size). If the item is negative, it
     *         indicates a partial prefix item, which should be assembled to the last itemset of the
     *         current prefix. Otherwise, the item should be appended to the current prefix.
     */
    def genPrefixItems: Iterator[(Int, Long)] = {
      val n1 = items.length - 1
      // For each unique item (subject to sign) in this sequence, we output exact one split.
      // TODO: use PrimitiveKeyOpenHashMap
      val prefixes = mutable.Map.empty[Int, Long]
      // a) items that can be assembled to the last itemset of the prefix
      partialStarts.foreach { start =>
        var i = start
        var x = -items(i)
        while (x != 0) {
          if (!prefixes.contains(x)) {
            prefixes(x) = n1 - i
          }
          i += 1
          x = -items(i)
        }
      }
      // b) items that can be appended to the prefix
      var i = fullStart
      while (i < n1) {
        val x = items(i)
        if (x != 0 && !prefixes.contains(x)) {
          prefixes(x) = n1 - i
        }
        i += 1
      }
      prefixes.toIterator
    }

    /**
     * Generates length-1 prefix items of this postfix with the corresponding postfix sizes.
     * There are two types of prefix items:
     *   a) The item can be assembled to the last itemset of the prefix. For example,
     *      the postfix of `<(12)(123)>1` w.r.t. `<1>` is `<(_2)(123)1>`. The prefix items of this
     *      postfix can be assembled to `<1>` is `_2` and `_3`, resulting new prefixes `<(12)>` and
     *      `<(13)>`. We flip the sign in the output to indicate that this is a partial prefix item.
     *   b) The item can be appended to the prefix. Taking the same example above, the prefix items
     *      can be appended to `<1>` is `1`, `2`, and `3`, resulting new prefixes `<11>`, `<12>`,
     *      and `<13>`.
     * @return an iterator of (prefix item, corresponding postfix size). If the item is negative, it
     *         indicates a partial prefix item, which should be assembled to the last itemset of the
     *         current prefix. Otherwise, the item should be appended to the current prefix.
     */
    def filtGenPrefixItems(validPosItems: Array[Int]): Iterator[(Int, Long)] = {
      val n1 = items.length - 1
      // For each unique item (subject to sign) in this sequence, we output exact one split.
      // TODO: use PrimitiveKeyOpenHashMap
      val prefixes = mutable.Map.empty[Int, Long]
      // a) items that can be assembled to the last itemset of the prefix
      partialStarts.foreach { start =>
        var i = start
        var x = -items(i)
        while (x != 0) {
          if (!prefixes.contains(x)) {
            prefixes(x) = n1 - i
          }
          i += 1
          x = -items(i)
        }
      }
      // b) items that can be appended to the prefix
      var i = fullStart
      while (i < n1) {
        val x = items(i)
        if (x != 0 && !prefixes.contains(x) && validPosItems.contains(x)) {
          prefixes(x) = n1 - i
        }
        i += 1
      }
      prefixes.toIterator
    }

    /** Tests whether this postfix is non-empty. */
    def nonEmpty: Boolean = items.length > start + 1

    /**
     * Projects this postfix with respect to the input prefix item.
     * @param prefix prefix item. If prefix is positive, we match items in any full itemset; if it
     *               is negative, we do partial projections.
     * @return the projected postfix
     */
    def project(prefix: Int): Postfix = {
      require(prefix != 0)
      val n1 = items.length - 1
      var matched = false
      var newStart = n1
      val newPartialStarts = mutable.ArrayBuilder.make[Int]
      if (prefix < 0) {
        // Search for partial projections.
        val target = -prefix
        partialStarts.foreach { start =>
          var i = start
          var x = items(i)
          while (x != target && x != 0) {
            i += 1
            x = items(i)
          }
          if (x == target) {
            i += 1
            if (!matched) {
              newStart = i
              matched = true
            }
            if (items(i) != 0) {
              newPartialStarts += i
            }
          }
        }
      } else {
        // Search for items in full itemsets.
        // Though the items are ordered in each itemsets, they should be small in practice.
        // So a sequential scan is sufficient here, compared to bisection search.
        val target = prefix
        var i = fullStart
        while (i < n1) {
          val x = items(i)
          if (x == target) {
            if (!matched) {
              newStart = i
              matched = true
            }
            if (items(i + 1) != 0) {
              newPartialStarts += i + 1
            }
          }
          i += 1
        }
      }
      if (n1 > newStart) {
        new Postfix(items, newStart, newPartialStarts.result())
      } else {
        null
      }
    }

    /**
     * Projects this postfix with respect to the input prefix.
     */
    private def project(prefix: Array[Int]): Postfix = {
      var partial = true
      var cur = this
      var i = 0
      val np = prefix.length
      while (i < np && null != cur) {
        val x = prefix(i)
        if (x == 0) {
          partial = false
        } else {
          if (partial) {
            cur = cur.project(-x)
          } else {
            cur = cur.project(x)
            partial = true
          }
        }
        i += 1
      }
      cur
    }

    /**
     * Projects this postfix with respect to the input prefix.
     */
    def project(prefix: Prefix): Postfix = project(prefix.items)

    /**
     * Returns the same sequence with compressed storage if possible.
     */
    def compressed: Postfix = {
      if (start > 0) {
        new Postfix(items.slice(start, items.length), 0, partialStarts.map(_ - start))
      } else {
        this
      }
    }
  }
