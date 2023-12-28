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

import java.lang.{Iterable => JavaIterable}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.fpm.FPGrowth._
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

/**
 * Model trained by [[FPGrowth]], which holds frequent itemsets.
 * @param freqItemsets frequent itemset, which is an RDD of `FreqItemset`
 * @tparam Item item type
 */
@Since("1.3.0")
class FPGrowthModel[Item: ClassTag] @Since("2.4.0")(
    @Since("1.3.0") val freqItemsets: RDD[FreqItemset[Item]],
    @Since("2.4.0") val itemSupport: Map[Item, Double])
  extends Saveable with Serializable {

  @Since("1.3.0")
  def this(freqItemsets: RDD[FreqItemset[Item]]) = this(freqItemsets, Map.empty)

  /**
   * Generates association rules for the `Item`s in [[freqItemsets]].
   * @param confidence minimal confidence of the rules produced
   */
  @Since("1.5.0")
  def generateAssociationRules(confidence: Double): RDD[AssociationRules.Rule[Item]] = {
    val associationRules = new AssociationRules(confidence)
    associationRules.run(freqItemsets, itemSupport)
  }

  /**
   * Save this model to the given path.
   * It only works for Item datatypes supported by DataFrames.
   *
   * This saves:
   *  - human-readable (JSON) model metadata to path/metadata/
   *  - Parquet formatted data to path/data/
   *
   * The model may be loaded using `FPGrowthModel.load`.
   *
   * @param sc  Spark context used to save model data.
   * @param path  Path specifying the directory in which to save this model.
   *              If the directory already exists, this method throws an exception.
   */
  @Since("2.0.0")
  override def save(sc: SparkContext, path: String): Unit = {
    FPGrowthModel.SaveLoadV1_0.save(this, path)
  }
}

@Since("2.0.0")
object FPGrowthModel extends Loader[FPGrowthModel[_]] {

  @Since("2.0.0")
  override def load(sc: SparkContext, path: String): FPGrowthModel[_] = {
    FPGrowthModel.SaveLoadV1_0.load(sc, path)
  }

  private[fpm] object SaveLoadV1_0 {

    private val thisFormatVersion = "1.0"

    private val thisClassName = "org.apache.spark.mllib.fpm.FPGrowthModel"

    def save(model: FPGrowthModel[_], path: String): Unit = {
      val sc = model.freqItemsets.sparkContext
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()

      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

      // Get the type of item class
      val sample = model.freqItemsets.first().items(0)
      val className = sample.getClass.getCanonicalName
      val classSymbol = runtimeMirror(getClass.getClassLoader).staticClass(className)
      val tpe = classSymbol.selfType

      val itemType = ScalaReflection.schemaFor(tpe).dataType
      val fields = Array(StructField("items", ArrayType(itemType)),
        StructField("freq", LongType))
      val schema = StructType(fields)
      val rowDataRDD = model.freqItemsets.map { x =>
        Row(x.items.toSeq, x.freq)
      }
      spark.createDataFrame(rowDataRDD, schema).write.parquet(Loader.dataPath(path))
    }

    def load(sc: SparkContext, path: String): FPGrowthModel[_] = {
      implicit val formats = DefaultFormats
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()

      val (className, formatVersion, metadata) = Loader.loadMetadata(sc, path)
      assert(className == thisClassName)
      assert(formatVersion == thisFormatVersion)

      val freqItemsets = spark.read.parquet(Loader.dataPath(path))
      val sample = freqItemsets.select("items").head().get(0)
      loadImpl(freqItemsets, sample)
    }

    def loadImpl[Item: ClassTag](freqItemsets: DataFrame, sample: Item): FPGrowthModel[Item] = {
      val freqItemsetsRDD = freqItemsets.select("items", "freq").rdd.map { x =>
        val items = x.getAs[Seq[Item]](0).toArray
        val freq = x.getLong(1)
        new FreqItemset(items, freq)
      }
      new FPGrowthModel(freqItemsetsRDD)
    }
  }
}

/**
 * A parallel FP-growth algorithm to mine frequent itemsets. The algorithm is described in
 * <a href="https://doi.org/10.1145/1454008.1454027">Li et al., PFP: Parallel FP-Growth for Query
 * Recommendation</a>. PFP distributes computation in such a way that each worker executes an
 * independent group of mining tasks. The FP-Growth algorithm is described in
 * <a href="https://doi.org/10.1145/335191.335372">Han et al., Mining frequent patterns without
 * candidate generation</a>.
 *
 * @param minSupport the minimal support level of the frequent pattern, any pattern that appears
 *                   more than (minSupport * size-of-the-dataset) times will be output
 * @param numPartitions number of partitions used by parallel FP-growth
 *
 * @see <a href="http://en.wikipedia.org/wiki/Association_rule_learning">
 * Association rule learning (Wikipedia)</a>
 *
 */
@Since("1.3.0")
class FPGrowth private[spark] (
    private var minSupport: Double,
    private var optLevel: Int,
    private var timeLimit1: Double,
    private var timeLimit2: Double,
    private var numPartitions: Int) extends Logging with Serializable {

  /**
   * Constructs a default instance with default parameters {minSupport: `0.3`, optLevel: '1',
   * timeLimit1: '0.3', timeLimit2: '0.15', numPartitions: same
   * as the input data}.
   *
   */
  @Since("1.3.0")
  def this() = this(0.3, 1, 0.3, 0.15, -1)

  /**
   * Sets the minimal support level (default: `0.3`).
   *
   */
  @Since("1.3.0")
  def setMinSupport(minSupport: Double): this.type = {
    require(minSupport >= 0.0 && minSupport <= 1.0,
      s"Minimal support level must be in range [0, 1] but got ${minSupport}")
    this.minSupport = minSupport
    this
  }

  private def setOptLevel(optLevel: Int): this.type = {
    require(optLevel >= 0 && optLevel <= 2,
      s"optLevel must be [0, 1, 2] but got $optLevel")
    this.optLevel = optLevel
    this
  }

  private def setTimeLimit1(timeLimit: Double): this.type = {
    require(timeLimit > 0.0 && timeLimit <= Double.MaxValue,
      s"Time limit must be greater than 0.0 but got $timeLimit")
    this.timeLimit1 = timeLimit
    this
  }

  private def setTimeLimit2(timeLimit: Double): this.type = {
    require(timeLimit > 0.0 && timeLimit <= Double.MaxValue,
      s"Time limit must be greater than 0.0 but got $timeLimit")
    this.timeLimit2 = timeLimit
    this
  }

  /**
   * Sets the number of partitions used by parallel FP-growth (default: same as input data).
   *
   */
  @Since("1.3.0")
  def setNumPartitions(numPartitions: Int): this.type = {
    require(numPartitions > 0,
      s"Number of partitions must be positive but got ${numPartitions}")
    this.numPartitions = numPartitions
    this
  }

  /**
   * Computes an FP-Growth model that contains frequent itemsets.
   *
   * @param data input data set, each element contains a transaction
   * @return an [[FPGrowthModel]]
   *
   */
  @Since("1.3.0")
  def run[Item: ClassTag](data: RDD[Array[Item]]): FPGrowthModel[Item] = {
    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("Input data is not cached.")
    }
    val count = data.count()
    val minCount = math.ceil(minSupport * count).toLong
    val numParts = if (numPartitions > 0) numPartitions else data.partitions.length
    val partitioner = new HashPartitioner(numParts)
    val freqItemsCount = FPGrowthCore.genFreqItems(data, minCount, partitioner)
    val itemSupport = freqItemsCount.map {
      case (item, cnt) => item -> cnt.toDouble / count
    }.toMap

    setOptLevelFromSparkConf(data.sparkContext)
    if (optLevel == 0) {
      val freqItemsets = FPGrowthCore.genFreqItemsets(data, minCount,
        freqItemsCount.map(_._1), partitioner)
      new FPGrowthModel(freqItemsets, itemSupport)
    } else if (optLevel == 1) {
      setLevel1TimeLimitFromSparkConf(data.sparkContext)
      val freqItemsets = FPGrowthUtils.genFreqItemsetsByOptLevel1(data, minCount,
        freqItemsCount.map(_._1), partitioner, timeLimit1)
      new FPGrowthModel(freqItemsets, itemSupport)
    } else {
      setLevel2TimeLimitFromSparkConf(data.sparkContext)
      val freqItemsets = FPGrowthUtils.genFreqItemsetsByOptLevel2(data, minCount,
        freqItemsCount.map(_._1), partitioner, timeLimit1, timeLimit2)
      new FPGrowthModel(freqItemsets, itemSupport)
    }
  }

  private val optLevelParamKey = "spark.boostkit.ml.fpgrowth.optLevel"
  private val timeLimit1ParamKey = "spark.boostkit.ml.fpgrowth.timeLimit1"
  private val timeLimit2ParamKey = "spark.boostkit.ml.fpgrowth.timeLimit2"

  private def setOptLevelFromSparkConf(sc: SparkContext): Unit = {
    val optLevelStr = sc.conf.getOption(optLevelParamKey)
    if (optLevelStr.nonEmpty) {
      try {
        val optLevel = optLevelStr.get.toInt
        setOptLevel(optLevel)
      } catch {
        case ex: Exception =>
          throw new IllegalArgumentException(s"Parse boostkit parameter" +
            s"($optLevelParamKey) failed, Error reason: ${ex.getMessage}")
      }
    }
  }

  private def setLevel1TimeLimitFromSparkConf(sc: SparkContext): Unit = {
    val timeLimit1Str = sc.conf.getOption(timeLimit1ParamKey)
    if (timeLimit1Str.nonEmpty) {
      try {
        val timeLimit1 = timeLimit1Str.get.toDouble
        setTimeLimit1(timeLimit1)
      } catch {
        case ex: Exception =>
          throw new IllegalArgumentException(s"Parse boostkit parameter" +
            s"($timeLimit1ParamKey) failed, Error reason: ${ex.getMessage}")
      }
    }
  }

  private def setLevel2TimeLimitFromSparkConf(sc: SparkContext): Unit = {
    val timeLimit1Str = sc.conf.getOption(timeLimit1ParamKey)
    val timeLimit2Str = sc.conf.getOption(timeLimit2ParamKey)
    if (timeLimit1Str.nonEmpty && timeLimit2Str.nonEmpty) {
      try {
        val timeLimit1 = timeLimit1Str.get.toDouble
        val timeLimit2 = timeLimit2Str.get.toDouble
        setTimeLimit1(timeLimit1)
        setTimeLimit2(timeLimit2)
      } catch {
        case ex: Exception =>
          throw new IllegalArgumentException(s"Parse boostkit parameter" +
            s"($timeLimit1ParamKey) failed, Error reason: ${ex.getMessage}")
      }
    }
  }

  /**
   * Java-friendly version of `run`.
   */
  @Since("1.3.0")
  def run[Item, Basket <: JavaIterable[Item]](data: JavaRDD[Basket]): FPGrowthModel[Item] = {
    implicit val tag = fakeClassTag[Item]
    run(data.rdd.map(_.asScala.toArray))
  }

}

@Since("1.3.0")
object FPGrowth {

  /**
   * Frequent itemset.
   * @param items items in this itemset. Java users should call `FreqItemset.javaItems` instead.
   * @param freq frequency
   * @tparam Item item type
   *
   */
  @Since("1.3.0")
  class FreqItemset[Item] @Since("1.3.0") (
      @Since("1.3.0") val items: Array[Item],
      @Since("1.3.0") val freq: Long) extends Serializable {

    /**
     * Returns items in a Java List.
     *
     */
    @Since("1.3.0")
    def javaItems: java.util.List[Item] = {
      items.toList.asJava
    }

    override def toString: String = {
      s"${items.mkString("{", ",", "}")}: $freq"
    }
  }
}
