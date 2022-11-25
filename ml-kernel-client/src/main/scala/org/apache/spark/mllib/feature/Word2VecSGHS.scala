// scalastyle:off
/*
* Copyright (C) 2022. Huawei Technologies Co., Ltd.
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
* */
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
