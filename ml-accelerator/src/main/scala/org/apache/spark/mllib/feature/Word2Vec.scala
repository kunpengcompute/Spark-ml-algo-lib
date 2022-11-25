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

package org.apache.spark.mllib.feature

import java.lang.{Iterable => JavaIterable}

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.rdd._
import org.apache.spark.util.Utils

/**
 *  Entry in vocabulary
 */
private case class VocabWord(
    var word: String,
    var cn: Int,
    var point: Array[Int],
    var code: Array[Int],
    var codeLen: Int
)

/**
 * Word2Vec creates vector representation of words in a text corpus.
 * The algorithm first constructs a vocabulary from the corpus
 * and then learns vector representation of words in the vocabulary.
 * The vector representation can be used as features in
 * natural language processing and machine learning algorithms.
 *
 * We used skip-gram model in our implementation and hierarchical softmax
 * method to train the model. The variable names in the implementation
 * matches the original C implementation.
 *
 * For original C implementation, see https://code.google.com/p/word2vec/
 * For research papers, see
 * Efficient Estimation of Word Representations in Vector Space
 * and
 * Distributed Representations of Words and Phrases and their Compositionality.
 */
@Since("1.1.0")
class Word2Vec extends Serializable with Logging {

  private var vectorSize = 100
  private var learningRate = 0.025
  private var regularization = 0.05f
  private var repetition = 2
  private var numPartitions = 1
  private var numIterations = 1
  private var seed = Utils.random.nextLong()
  private var minCount = 5
  private var maxSentenceLength = 1000

  /**
   * Sets the regularization coefficient.
   */
  def setRegularization(regularization: Float): this.type = {
    require(regularization >= 0,
      s"The value of regularization must not be smaller than 0 but got ${regularization}")
    this.regularization = regularization
    this
  }

  /**
   * Sets the number of repetitions of data.
   */
  def setRepetition(repetition: Int): this.type = {
    require(repetition >= 0,
      s"The number of repetitions of data must not be smaller than 0 but got ${repetition}")
    this.repetition = repetition
    this
  }

  /**
   * Sets the maximum length (in words) of each sentence in the input data.
   * Any sentence longer than this threshold will be divided into chunks of
   * up to `maxSentenceLength` size (default: 1000)
   */
  @Since("2.0.0")
  def setMaxSentenceLength(maxSentenceLength: Int): this.type = {
    require(maxSentenceLength > 0,
      s"Maximum length of sentences must be positive but got ${maxSentenceLength}")
    this.maxSentenceLength = maxSentenceLength
    this
  }

  /**
   * Sets vector size (default: 100).
   */
  @Since("1.1.0")
  def setVectorSize(vectorSize: Int): this.type = {
    require(vectorSize > 0,
      s"vector size must be positive but got ${vectorSize}")
    this.vectorSize = vectorSize
    this
  }

  /**
   * Sets initial learning rate (default: 0.025).
   */
  @Since("1.1.0")
  def setLearningRate(learningRate: Double): this.type = {
    require(learningRate > 0,
      s"Initial learning rate must be positive but got ${learningRate}")
    this.learningRate = learningRate
    this
  }

  /**
   * Sets number of partitions (default: 1). Use a small number for accuracy.
   */
  @Since("1.1.0")
  def setNumPartitions(numPartitions: Int): this.type = {
    require(numPartitions > 0,
      s"Number of partitions must be positive but got ${numPartitions}")
    this.numPartitions = numPartitions
    this
  }

  /**
   * Sets number of iterations (default: 1), which should be smaller than or equal to number of
   * partitions.
   */
  @Since("1.1.0")
  def setNumIterations(numIterations: Int): this.type = {
    require(numIterations >= 0,
      s"Number of iterations must be nonnegative but got ${numIterations}")
    this.numIterations = numIterations
    this
  }

  /**
   * Sets random seed (default: a random long integer).
   */
  @Since("1.1.0")
  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  /**
   * Sets the window of words (default: 5)
   */
  @Since("1.6.0")
  def setWindowSize(window: Int): this.type = {
    require(window > 0,
      s"Window of words must be positive but got ${window}")
    this.window = window
    this
  }

  /**
   * Sets minCount, the minimum number of times a token must appear to be included in the word2vec
   * model's vocabulary (default: 5).
   */
  @Since("1.3.0")
  def setMinCount(minCount: Int): this.type = {
    require(minCount >= 0,
      s"Minimum number of times must be nonnegative but got ${minCount}")
    this.minCount = minCount
    this
  }

  /**
   * Set parameters of algorithm.
   */
  private def setParams(conf: SparkConf): Unit = {
    setRegularization(Word2Vec.getSparkConfFloatValue(
      conf, "spark.boostkit.mllib.feature.word2vec.regularization", 0.05f))
    setRepetition(Word2Vec.getSparkConfIntValue(
      conf, "spark.boostkit.mllib.feature.word2vec.repetition", 2))
  }

  private val EXP_TABLE_SIZE = 1000
  private val MAX_EXP = 6
  private val MAX_CODE_LENGTH = 40

  /** context words from [-window, window] */
  private var window = 5

  private var trainWordsCount = 0L
  private var vocabSize = 0
  @transient private var vocab: Array[VocabWord] = null
  @transient private var vocabHash = mutable.HashMap.empty[String, Int]

  private def learnVocab[S <: Iterable[String]](dataset: RDD[S]): Unit = {
    val words = dataset.flatMap(x => x)

    vocab = words.map(w => (w, 1))
      .reduceByKey(_ + _)
      .filter(_._2 >= minCount)
      .map(x => VocabWord(
        x._1,
        x._2,
        new Array[Int](MAX_CODE_LENGTH),
        new Array[Int](MAX_CODE_LENGTH),
        0))
      .collect()
      .sortWith((a, b) => a.cn > b.cn)

    vocabSize = vocab.length
    require(vocabSize > 0, "The vocabulary size should be > 0. You may need to check " +
      "the setting of minCount, which could be large enough to remove all your words in sentences.")

    var a = 0
    while (a < vocabSize) {
      vocabHash += vocab(a).word -> a
      trainWordsCount += vocab(a).cn
      a += 1
    }
    logInfo(s"vocabSize = $vocabSize, trainWordsCount = $trainWordsCount")
  }

  private def createExpTable(): Array[Float] = {
    val expTable = new Array[Float](EXP_TABLE_SIZE)
    var i = 0
    while (i < EXP_TABLE_SIZE) {
      val tmp = math.exp((2.0 * i / EXP_TABLE_SIZE - 1.0) * MAX_EXP)
      expTable(i) = (tmp / (tmp + 1.0)).toFloat
      i += 1
    }
    expTable
  }

  private def createBinaryTree(): Unit = {
    val count = new Array[Long](vocabSize * 2 + 1)
    val binary = new Array[Int](vocabSize * 2 + 1)
    val parentNode = new Array[Int](vocabSize * 2 + 1)
    val code = new Array[Int](MAX_CODE_LENGTH)
    val point = new Array[Int](MAX_CODE_LENGTH)
    var a = 0
    while (a < vocabSize) {
      count(a) = vocab(a).cn
      a += 1
    }
    while (a < 2 * vocabSize) {
      count(a) = 1e9.toInt
      a += 1
    }
    var pos1 = vocabSize - 1
    var pos2 = vocabSize

    var min1i = 0
    var min2i = 0

    a = 0
    while (a < vocabSize - 1) {
      if (pos1 >= 0) {
        if (count(pos1) < count(pos2)) {
          min1i = pos1
          pos1 -= 1
        } else {
          min1i = pos2
          pos2 += 1
        }
      } else {
        min1i = pos2
        pos2 += 1
      }
      if (pos1 >= 0) {
        if (count(pos1) < count(pos2)) {
          min2i = pos1
          pos1 -= 1
        } else {
          min2i = pos2
          pos2 += 1
        }
      } else {
        min2i = pos2
        pos2 += 1
      }
      count(vocabSize + a) = count(min1i) + count(min2i)
      parentNode(min1i) = vocabSize + a
      parentNode(min2i) = vocabSize + a
      binary(min2i) = 1
      a += 1
    }
    // Now assign binary code to each vocabulary word
    var i = 0
    a = 0
    while (a < vocabSize) {
      var b = a
      i = 0
      while (b != vocabSize * 2 - 2) {
        code(i) = binary(b)
        point(i) = b
        i += 1
        b = parentNode(b)
      }
      vocab(a).codeLen = i
      vocab(a).point(0) = vocabSize - 2
      b = 0
      while (b < i) {
        vocab(a).code(i - b - 1) = code(b)
        vocab(a).point(i - b) = point(b) - vocabSize
        b += 1
      }
      a += 1
    }
  }

  /**
   * Computes the vector representation of each word in vocabulary.
   * @param dataset an RDD of sentences,
   *                each sentence is expressed as an iterable collection of words
   * @return a Word2VecModel
   */
  @Since("1.1.0")
  def fit[S <: Iterable[String]](dataset: RDD[S]): Word2VecModel = {

    learnVocab(dataset)

    createBinaryTree()

    val sc = dataset.context

    val expTable = sc.broadcast(createExpTable())
    val bcVocab = sc.broadcast(vocab)
    val bcVocabHash = sc.broadcast(vocabHash)
    try {
      setParams(dataset.sparkContext.conf)

      val modelSGHS = new Word2VecSGHS(minCount, window, vectorSize, vocabSize, trainWordsCount,
        learningRate, numIterations, seed, maxSentenceLength, regularization, repetition)
      val vectors = modelSGHS.fit(dataset, expTable, bcVocab, bcVocabHash)

      new Word2VecModel(vocabHash.toMap, vectors)
    } finally {
      expTable.destroy(blocking = false)
      bcVocab.destroy(blocking = false)
      bcVocabHash.destroy(blocking = false)
    }
  }

  /**
   * Computes the vector representation of each word in vocabulary (Java version).
   * @param dataset a JavaRDD of words
   * @return a Word2VecModel
   */
  @Since("1.1.0")
  def fit[S <: JavaIterable[String]](dataset: JavaRDD[S]): Word2VecModel = {
    fit(dataset.rdd.map(_.asScala))
  }
}

object Word2Vec {
  /**
   * Get the float value from spark conf.
   */
  def getSparkConfFloatValue(
      conf: SparkConf,
      valueName: String,
      defaultValue: Float): Float = {
    var value = defaultValue
    val valueOption = conf.getOption(valueName)

    if(valueOption.nonEmpty) {
      try {
        value = valueOption.get.toFloat
      } catch {
        case _: Exception =>
          throw new IllegalArgumentException(s"$valueName should be Float, " +
            s"but was ${valueOption.get}")
      }
    }

    value
  }

  /**
   * Get the int value from spark conf.
   */
  def getSparkConfIntValue(
      conf: SparkConf,
      valueName: String,
      defaultValue: Int): Int = {
    var value = defaultValue
    val valueOption = conf.getOption(valueName)

    if(valueOption.nonEmpty) {
      try {
        value = valueOption.get.toInt
      } catch {
        case _: Exception =>
          throw new IllegalArgumentException(s"$valueName should be Int and greater than 0, " +
            s"but was ${valueOption.get}")
      }
    }

    value
  }
}
