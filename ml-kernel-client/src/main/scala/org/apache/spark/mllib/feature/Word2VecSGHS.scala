// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.spark.mllib.feature

import scala.collection.mutable

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

class Word2VecSGHS(
    val minCount: Int,
    val window: Int,
    val vectorSize: Int,
    val vocabSize: Int,
    val trainWordsCount: Long,
    val learningRate: Double,
    val numIterations: Int,
    val seed: Long,
    val maxSentenceLength: Int,
    val regularization: Float,
    val repetition: Int) extends Serializable with Logging {

  def fit[S <: Iterable[String]](
      dataset: RDD[S],
      bcExpTable: Broadcast[Array[Float]],
      bcVocab: Broadcast[Array[VocabWord]],
      bcVocabHash: Broadcast[mutable.HashMap[String, Int]]): Array[Float] = {
    null
  }
}
