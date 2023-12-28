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

package org.apache.spark.ml.recommendation

import java.{util => ju}
import java.io.IOException
import java.util.Locale

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.{Sorting, Try}
import scala.util.hashing.byteswap64

import com.google.common.collect.{Ordering => GuavaOrdering}
import org.apache.hadoop.fs.Path
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._

import org.apache.spark.{Partitioner, SparkContext, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg.BLAS
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.mllib.optimization.NNLS
import org.apache.spark.rdd.{DeterministicLevel, RDD}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.{OpenHashMap, OpenHashSet, SortDataFormat, Sorter}
import org.apache.spark.util.random.XORShiftRandom

/**
 * Common params for NMF and NMFModel.
 */
private[recommendation] trait NMFModelParams extends Params with HasPredictionCol
  with HasBlockSize {
  /**
   * Param for the column name for user ids. Ids must be integers. Other
   * numeric types are supported for this column, but will be cast to integers as long as they
   * fall within the integer value range.
   * Default: "user"
   *
   * @group param
   */
  val userCol = new Param[String](this, "userCol", "column name for user ids. Ids must be within " +
    "the integer value range.")

  /** @group getParam */
  def getUserCol: String = $(userCol)

  /**
   * Param for the column name for item ids. Ids must be integers. Other
   * numeric types are supported for this column, but will be cast to integers as long as they
   * fall within the integer value range.
   * Default: "item"
   *
   * @group param
   */
  val itemCol = new Param[String](this, "itemCol", "column name for item ids. Ids must be within " +
    "the integer value range.")

  /** @group getParam */
  def getItemCol: String = $(itemCol)

  /**
   * Attempts to safely cast a user/item id to an Int. Throws an exception if the value is
   * out of integer range or contains a fractional part.
   */
  protected[recommendation] val checkedCast = udf { (n: Any) =>
    n match {
      case v: Int => v // Avoid unnecessary casting
      case v: Number =>
        val intV = v.intValue
        // Checks if number within Int range and has no fractional part.
        if (v.doubleValue == intV) {
          intV
        } else {
          throw new IllegalArgumentException(s"NMF only supports values in Integer range " +
            s"and without fractional part for columns ${$(userCol)} and ${$(itemCol)}. " +
            s"Value $n was either out of Integer range or contained a fractional part that " +
            s"could not be converted.")
        }
      case _ => throw new IllegalArgumentException(s"NMF only supports values in Integer range " +
        s"for columns ${$(userCol)} and ${$(itemCol)}. Value $n was not numeric.")
    }
  }

  /**
   * Param for strategy for dealing with unknown or new users/items at prediction time.
   * This may be useful in cross-validation or production scenarios, for handling user/item ids
   * the model has not seen in the training data.
   * Supported values:
   * - "nan":  predicted value for unknown ids will be NaN.
   * - "drop": rows in the input DataFrame containing unknown ids will be dropped from
   * the output DataFrame containing predictions.
   * Default: "nan".
   *
   * @group expertParam
   */
  val coldStartStrategy = new Param[String](this, "coldStartStrategy",
    "strategy for dealing with unknown or new users/items at prediction time. This may be " +
      "useful in cross-validation or production scenarios, for handling user/item ids the model " +
      "has not seen in the training data. Supported values: " +
      s"${NMFModel.supportedColdStartStrategies.mkString(",")}.",
    (s: String) =>
      NMFModel.supportedColdStartStrategies.contains(s.toLowerCase(Locale.ROOT)))

  /** @group expertGetParam */
  def getColdStartStrategy: String = $(coldStartStrategy).toLowerCase(Locale.ROOT)

  setDefault(blockSize -> 4096)
}

/**
 * Common params for NMF.
 */
private[recommendation] trait NMFParams extends NMFModelParams with HasMaxIter with HasRegParam
  with HasCheckpointInterval with HasSeed {

  /**
   * Param for rank of the matrix factorization (positive).
   * Default: 10
   * @group param
   */
  val rank = new IntParam(this, "rank", "rank of the factorization", ParamValidators.gtEq(1))

  /** @group getParam */
  def getRank: Int = $(rank)

  /**
   * Param for number of user blocks (positive).
   * Default: 10
   * @group param
   */
  val numUserBlocks = new IntParam(this, "numUserBlocks", "number of user blocks",
    ParamValidators.gtEq(1))

  /** @group getParam */
  def getNumUserBlocks: Int = $(numUserBlocks)

  /**
   * Param for number of item blocks (positive).
   * Default: 10
   * @group param
   */
  val numItemBlocks = new IntParam(this, "numItemBlocks", "number of item blocks",
    ParamValidators.gtEq(1))

  /** @group getParam */
  def getNumItemBlocks: Int = $(numItemBlocks)

  /**
   * Param for the column name for ratings.
   * Default: "rating"
   *
   * @group param
   */
  val ratingCol = new Param[String](this, "ratingCol", "column name for ratings")

  /** @group getParam */
  def getRatingCol: String = $(ratingCol)

  /**
   * Param for StorageLevel for intermediate datasets. Pass in a string representation of
   * `StorageLevel`. Cannot be "NONE".
   * Default: "MEMORY_AND_DISK".
   *
   * @group expertParam
   */
  val intermediateStorageLevel = new Param[String](this, "intermediateStorageLevel",
    "StorageLevel for intermediate datasets. Cannot be 'NONE'.",
    (s: String) => Try(StorageLevel.fromString(s)).isSuccess && s != "NONE")

  /** @group expertGetParam */
  def getIntermediateStorageLevel: String = $(intermediateStorageLevel)

  /**
   * Param for StorageLevel for NMF model factors. Pass in a string representation of
   * `StorageLevel`.
   * Default: "MEMORY_AND_DISK".
   *
   * @group expertParam
   */
  val finalStorageLevel = new Param[String](this, "finalStorageLevel",
    "StorageLevel for NMF model factors.",
    (s: String) => Try(StorageLevel.fromString(s)).isSuccess)

  /** @group expertGetParam */
  def getFinalStorageLevel: String = $(finalStorageLevel)

  setDefault(rank -> 10, maxIter -> 10, regParam -> 0.1, numUserBlocks -> 10, numItemBlocks -> 10,
    userCol -> "user", itemCol -> "item", ratingCol -> "rating", checkpointInterval -> 10,
    intermediateStorageLevel -> "MEMORY_AND_DISK", finalStorageLevel -> "MEMORY_AND_DISK",
    coldStartStrategy -> "nan")

  /**
   * Validates and transforms the input schema.
   *
   * @param schema input schema
   * @return output schema
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    // user and item will be cast to Int
    SchemaUtils.checkNumericType(schema, $(userCol))
    SchemaUtils.checkNumericType(schema, $(itemCol))
    // rating will be cast to Float
    SchemaUtils.checkNumericType(schema, $(ratingCol))
    SchemaUtils.appendColumn(schema, $(predictionCol), FloatType)
  }
}

/**
 * Model fitted by NMF.
 *
 * @param rank        rank of the matrix factorization model
 * @param userFactors a DataFrame that stores user factors in two columns: `id` and `features`
 * @param itemFactors a DataFrame that stores item factors in two columns: `id` and `features`
 */
class NMFModel private[ml](
  override val uid: String,
  val rank: Int,
  @transient val userFactors: DataFrame,
  @transient val itemFactors: DataFrame)
  extends Model[NMFModel] with NMFModelParams with MLWritable {

  /** @group setParam */
  def setUserCol(value: String): this.type = set(userCol, value)

  /** @group setParam */
  def setItemCol(value: String): this.type = set(itemCol, value)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group expertSetParam */
  def setColdStartStrategy(value: String): this.type = set(coldStartStrategy, value)

  /**
   * Set block size for stacking input data in matrices.
   * Default is 4096.
   *
   * @group expertSetParam
   */
  def setBlockSize(value: Int): this.type = set(blockSize, value)

  private val predict = udf { (featuresA: Seq[Float], featuresB: Seq[Float]) =>
    if (featuresA != null && featuresB != null) {
      var dotProduct = 0.0f
      var i = 0
      while (i < rank) {
        dotProduct += featuresA(i) * featuresB(i)
        i += 1
      }
      dotProduct
    } else {
      Float.NaN
    }
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema)
    // create a new column named map(predictionCol) by running the predict UDF.
    val predictions = dataset
      .join(userFactors,
        checkedCast(dataset($(userCol))) === userFactors("id"), "left")
      .join(itemFactors,
        checkedCast(dataset($(itemCol))) === itemFactors("id"), "left")
      .select(dataset("*"),
        predict(userFactors("features"), itemFactors("features")).as($(predictionCol)))
    getColdStartStrategy match {
      case NMFModel.Drop =>
        predictions.na.drop("all", Seq($(predictionCol)))
      case NMFModel.NaN =>
        predictions
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    // user and item will be cast to Int
    SchemaUtils.checkNumericType(schema, $(userCol))
    SchemaUtils.checkNumericType(schema, $(itemCol))
    SchemaUtils.appendColumn(schema, $(predictionCol), FloatType)
  }

  override def copy(extra: ParamMap): NMFModel = {
    val copied = new NMFModel(uid, rank, userFactors, itemFactors)
    copyValues(copied, extra).setParent(parent)
  }

  override def write: MLWriter = new NMFModel.NMFModelWriter(this)

  override def toString: String = {
    s"NMFModel: uid=$uid, rank=$rank"
  }

  /**
   * Returns top `numItems` items recommended for each user, for all users.
   * @param numItems max number of recommendations for each user
   * @return a DataFrame of (userCol: Int, recommendations), where recommendations are
   *         stored as an array of (itemCol: Int, rating: Float) Rows.
   */
  def recommendForAllUsers(numItems: Int): DataFrame = {
    recommendForAll(userFactors, itemFactors, $(userCol), $(itemCol), numItems, $(blockSize))
  }

  /**
   * Returns top `numItems` items recommended for each user id in the input data set. Note that if
   * there are duplicate ids in the input dataset, only one set of recommendations per unique id
   * will be returned.
   *
   * @param dataset  a Dataset containing a column of user ids.
   *  The column name must match `userCol`.
   * @param numItems max number of recommendations for each user.
   * @return a DataFrame of (userCol: Int, recommendations), where recommendations are
   *         stored as an array of (itemCol: Int, rating: Float) Rows.
   */
  def recommendForUserSubset(dataset: Dataset[_], numItems: Int): DataFrame = {
    val srcFactorSubset = getSourceFactorSubset(dataset, userFactors, $(userCol))
    recommendForAll(srcFactorSubset, itemFactors, $(userCol), $(itemCol), numItems, $(blockSize))
  }

  /**
   * Returns top `numUsers` users recommended for each item, for all items.
   * @param numUsers max number of recommendations for each item
   * @return a DataFrame of (itemCol: Int, recommendations), where recommendations are
   *         stored as an array of (userCol: Int, rating: Float) Rows.
   */
  def recommendForAllItems(numUsers: Int): DataFrame = {
    recommendForAll(itemFactors, userFactors, $(itemCol), $(userCol), numUsers, $(blockSize))
  }

  /**
   * Returns top `numUsers` users recommended for each item id in the input data set. Note that if
   * there are duplicate ids in the input dataset, only one set of recommendations per unique id
   * will be returned.
   *
   * @param dataset  a Dataset containing a column of item ids.
   *  The column name must match `itemCol`.
   * @param numUsers max number of recommendations for each item.
   * @return a DataFrame of (itemCol: Int, recommendations), where recommendations are
   *         stored as an array of (userCol: Int, rating: Float) Rows.
   */
  def recommendForItemSubset(dataset: Dataset[_], numUsers: Int): DataFrame = {
    val srcFactorSubset = getSourceFactorSubset(dataset, itemFactors, $(itemCol))
    recommendForAll(srcFactorSubset, userFactors, $(itemCol), $(userCol), numUsers, $(blockSize))
  }

  /**
   * Returns a subset of a factor DataFrame limited to only those unique ids contained
   * in the input dataset.
   * @param dataset input Dataset containing id column to user to filter factors.
   * @param factors factor DataFrame to filter.
   * @param column column name containing the ids in the input dataset.
   * @return DataFrame containing factors only for those ids present in both the input dataset and
   *         the factor DataFrame.
   */
  private def getSourceFactorSubset(
    dataset: Dataset[_],
    factors: DataFrame,
    column: String): DataFrame = {
    factors
      .join(dataset.select(column), factors("id") === dataset(column), joinType = "left_semi")
      .select(factors("id"), factors("features"))
  }

  /**
   * Makes recommendations for all users (or items).
   *
   * Note: the previous approach used for computing top-k recommendations
   * used a cross-join followed by predicting a score for each row of the joined dataset.
   * However, this results in exploding the size of intermediate data. While Spark SQL makes it
   * relatively efficient, the approach implemented here is significantly more efficient.
   *
   * This approach groups factors into blocks and computes the top-k elements per block,
   * using GEMV (it use less memory compared with GEMM, and is much faster than DOT) and
   * an efficient selection based on [[GuavaOrdering]] (instead of [[BoundedPriorityQueue]]).
   * It then computes the global top-k by aggregating the per block top-k elements with
   * a [[TopByKeyAggregator]]. This significantly reduces the size of intermediate and shuffle data.
   * This is the DataFrame equivalent to the approach used in
   * [[org.apache.spark.mllib.recommendation.MatrixFactorizationModel]].
   *
   * @param srcFactors src factors for which to generate recommendations
   * @param dstFactors dst factors used to make recommendations
   * @param srcOutputColumn name of the column for the source ID in the output DataFrame
   * @param dstOutputColumn name of the column for the destination ID in the output DataFrame
   * @param num max number of recommendations for each record
   * @return a DataFrame of (srcOutputColumn: Int, recommendations), where recommendations are
   *         stored as an array of (dstOutputColumn: Int, rating: Float) Rows.
   */
  private def recommendForAll(
    srcFactors: DataFrame,
    dstFactors: DataFrame,
    srcOutputColumn: String,
    dstOutputColumn: String,
    num: Int,
    blockSize: Int): DataFrame = {
    import srcFactors.sparkSession.implicits._
    import scala.collection.JavaConverters._

    val srcFactorsBlocked = blockify(srcFactors.as[(Int, Array[Float])], blockSize)
    val dstFactorsBlocked = blockify(dstFactors.as[(Int, Array[Float])], blockSize)
    val ratings = srcFactorsBlocked.crossJoin(dstFactorsBlocked)
      .as[(Array[Int], Array[Float], Array[Int], Array[Float])]
      .mapPartitions { iter =>
        var scores: Array[Float] = null
        var idxOrd: GuavaOrdering[Int] = null
        iter.flatMap { case (srcIds, srcMat, dstIds, dstMat) =>
          require(srcMat.length == srcIds.length * rank)
          require(dstMat.length == dstIds.length * rank)
          val m = srcIds.length
          val n = dstIds.length
          if (scores == null || scores.length < n) {
            scores = Array.ofDim[Float](n)
            idxOrd = new GuavaOrdering[Int] {
              override def compare(left: Int, right: Int): Int = {
                Ordering[Float].compare(scores(left), scores(right))
              }
            }
          }

          Iterator.range(0, m).flatMap { i =>
            // scores = i-th vec in srcMat * dstMat
            BLAS.javaBLAS.sgemv("T", rank, n, 1.0F, dstMat, 0, rank,
              srcMat, i * rank, 1, 0.0F, scores, 0, 1)

            val srcId = srcIds(i)
            idxOrd.greatestOf(Iterator.range(0, n).asJava, num).asScala
              .iterator.map { j => (srcId, dstIds(j), scores(j)) }
          }
        }
      }
    // We'll force the IDs to be Int. Unfortunately this converts IDs to Int in the output.
    val topKAggregator = new TopByKeyAggregator[Int, Int, Float](num, Ordering.by(_._2))
    val recs = ratings.as[(Int, Int, Float)].groupByKey(_._1).agg(topKAggregator.toColumn)
      .toDF("id", "recommendations")

    val arrayType = ArrayType(
      new StructType()
        .add(dstOutputColumn, IntegerType)
        .add("rating", FloatType)
    )
    recs.select($"id".as(srcOutputColumn), $"recommendations".cast(arrayType))
  }

  /**
   * Blockifies factors to improve the efficiency of cross join
   */
  private def blockify(
    factors: Dataset[(Int, Array[Float])],
    blockSize: Int): Dataset[(Array[Int], Array[Float])] = {
    import factors.sparkSession.implicits._
    factors.mapPartitions { iter =>
      iter.grouped(blockSize)
        .map(block => (block.map(_._1).toArray, block.flatMap(_._2).toArray))
    }
  }

}

object NMFModel extends MLReadable[NMFModel] {

  private val NaN = "nan"
  private val Drop = "drop"
  private[recommendation] final val supportedColdStartStrategies = Array(NaN, Drop)

  override def read: MLReader[NMFModel] = new NMFModelReader

  override def load(path: String): NMFModel = super.load(path)

  private[NMFModel] class NMFModelWriter(instance: NMFModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val extraMetadata = "rank" -> instance.rank
      DefaultParamsWriter.saveMetadata(instance, path, sc, Some(extraMetadata))
      val userPath = new Path(path, "userFactors").toString
      instance.userFactors.write.format("parquet").save(userPath)
      val itemPath = new Path(path, "itemFactors").toString
      instance.itemFactors.write.format("parquet").save(itemPath)
    }
  }

  private class NMFModelReader extends MLReader[NMFModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[NMFModel].getName

    override def load(path: String): NMFModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      implicit val format = DefaultFormats
      val rank = (metadata.metadata \ "rank").extract[Int]
      val userPath = new Path(path, "userFactors").toString
      val userFactors = sparkSession.read.format("parquet").load(userPath)
      val itemPath = new Path(path, "itemFactors").toString
      val itemFactors = sparkSession.read.format("parquet").load(itemPath)

      val model = new NMFModel(metadata.uid, rank, userFactors, itemFactors)

      metadata.getAndSetParams(model)
      model
    }
  }
}

/**
 * Alternating Least Squares (NMF) matrix factorization.
 *
 * NMF attempts to estimate the ratings matrix `R` as the product of two lower-rank matrices,
 * `X` and `Y`, i.e. `X * Yt = R`. Typically these approximations are called 'factor' matrices.
 * The general approach is iterative. During each iteration, one of the factor matrices is held
 * constant, while the other is solved for using least squares. The newly-solved factor matrix is
 * then held constant while solving for the other factor matrix.
 *
 * This is a blocked implementation of the NMF factorization algorithm that groups the two sets
 * of factors (referred to as "users" and "products") into blocks and reduces communication by only
 * sending one copy of each user vector to each product block on each iteration, and only for the
 * product blocks that need that user's feature vector. This is achieved by pre-computing some
 * information about the ratings matrix to determine the "out-links" of each user (which blocks of
 * products it will contribute to) and "in-link" information for each product (which of the feature
 * vectors it receives from each user block it will depend on). This allows us to send only an
 * array of feature vectors between each user block and product block, and have the product block
 * find the users' ratings and update the products based on these messages.
 *
 * For implicit preference data, the algorithm used is based on
 * "Collaborative Filtering for Implicit Feedback Datasets", available at
 * https://doi.org/10.1109/ICDM.2008.22, adapted for the blocked approach used here.
 *
 * Essentially instead of finding the low-rank approximations to the rating matrix `R`,
 * this finds the approximations for a preference matrix `P` where the elements of `P` are 1 if
 * r is greater than 0 and 0 if r is less than or equal to 0. The ratings then act as 'confidence'
 * values related to strength of indicated user
 * preferences rather than explicit ratings given to items.
 *
 * Note: the input rating dataset to the NMF implementation should be deterministic.
 * Nondeterministic data can cause failure during fitting NMF model.
 * For example, an order-sensitive operation like sampling after a repartition makes dataset
 * output nondeterministic, like `dataset.repartition(2).sample(false, 0.5, 1618)`.
 * Checkpointing sampled dataset or adding a sort before sampling can help make the dataset
 * deterministic.
 */
class NMF(override val uid: String) extends Estimator[NMFModel] with NMFParams
  with DefaultParamsWritable {

  import org.apache.spark.ml.recommendation.NMF.Rating

  def this() = this(Identifiable.randomUID("nmf"))

  /** @group setParam */
  def setRank(value: Int): this.type = set(rank, value)

  /** @group setParam */
  def setNumUserBlocks(value: Int): this.type = set(numUserBlocks, value)

  /** @group setParam */
  def setNumItemBlocks(value: Int): this.type = set(numItemBlocks, value)

  /** @group setParam */
  def setUserCol(value: String): this.type = set(userCol, value)

  /** @group setParam */
  def setItemCol(value: String): this.type = set(itemCol, value)

  /** @group setParam */
  def setRatingCol(value: String): this.type = set(ratingCol, value)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  def setRegParam(value: Double): this.type = set(regParam, value)

  /** @group setParam */
  def setCheckpointInterval(value: Int): this.type = set(checkpointInterval, value)

  /** @group setParam */
  def setSeed(value: Long): this.type = set(seed, value)

  /** @group expertSetParam */
  def setIntermediateStorageLevel(value: String): this.type = set(intermediateStorageLevel, value)

  /** @group expertSetParam */
  def setFinalStorageLevel(value: String): this.type = set(finalStorageLevel, value)

  /** @group expertSetParam */
  def setColdStartStrategy(value: String): this.type = set(coldStartStrategy, value)

  /**
   * Set block size for stacking input data in matrices.
   * Default is 4096.
   *
   * @group expertSetParam
   */
  def setBlockSize(value: Int): this.type = set(blockSize, value)

  /**
   * Sets both numUserBlocks and numItemBlocks to the specific value.
   *
   * @group setParam
   */
  def setNumBlocks(value: Int): this.type = {
    setNumUserBlocks(value)
    setNumItemBlocks(value)
    this
  }

  override def fit(dataset: Dataset[_]): NMFModel = instrumented { instr =>
    transformSchema(dataset.schema)
    import dataset.sparkSession.implicits._

    val r = if ($(ratingCol) != "") col($(ratingCol)).cast(FloatType) else lit(1.0f)

    val ratings = dataset
      .select(checkedCast(col($(userCol))), checkedCast(col($(itemCol))), r)
      .rdd
      .map { row =>
        Rating(row.getInt(0), row.getInt(1), row.getFloat(2))
      }

    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, rank, numUserBlocks, numItemBlocks, userCol,
      itemCol, ratingCol, predictionCol, maxIter, regParam, checkpointInterval,
      seed, intermediateStorageLevel, finalStorageLevel, blockSize)


    val (userFactors, itemFactors) = NMF.train(ratings, rank = $(rank),
      ($(numUserBlocks), $(numItemBlocks)),
      maxIter = $(maxIter), regParam = $(regParam),
      intermediateRDDStorageLevel = StorageLevel.fromString($(intermediateStorageLevel)),
      finalRDDStorageLevel = StorageLevel.fromString($(finalStorageLevel)),
      checkpointInterval = $(checkpointInterval), seed = $(seed))
    val userDF = userFactors.toDF("id", "features")
    val itemDF = itemFactors.toDF("id", "features")
    val model = new NMFModel(uid, $(rank), userDF, itemDF).setBlockSize($(blockSize))
      .setParent(this)
    copyValues(model)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): NMF = defaultCopy(extra)
}


/**
 * An implementation of NMF that supports generic ID types, specialized for Int and Long. This is
 * exposed as a developer API for users who do need other ID types. But it is not recommended
 * because it increases the shuffle size and memory requirement during training. For simplicity,
 * users and items must have the same type. The number of distinct users/items should be smaller
 * than 2 billion.
 */
object NMF extends DefaultParamsReadable[NMF] with Logging {

  /**
   * Rating class for better code readability.
   */
  case class Rating[@specialized(Int, Long) ID](user: ID, item: ID, rating: Float)

  override def load(path: String): NMF = super.load(path)

  /** Trait for least squares solvers applied to the normal equation. */
  private[recommendation] trait LeastSquaresNESolver extends Serializable {
    /** Solves a least squares problem with regularization (possibly with other constraints). */
    def solve(ne: NormalEquation, lambda: Double): Array[Float]
  }

  /** NNLS solver. */
  private[recommendation] class NNLSSolver extends LeastSquaresNESolver {
    private var rank: Int = -1
    private var workspace: NNLS.Workspace = _
    private var ata: Array[Double] = _
    private var initialized: Boolean = false

    private def initialize(rank: Int): Unit = {
      if (!initialized) {
        this.rank = rank
        workspace = NNLS.createWorkspace(rank)
        ata = new Array[Double](rank * rank)
        initialized = true
      } else {
        require(this.rank == rank)
      }
    }

    /**
     * Solves a nonnegative least squares problem with L2 regularization:
     *
     *   min_x_  norm(A x - b)^2^ + lambda * n * norm(x)^2^
     *   subject to x >= 0
     */
    override def solve(ne: NormalEquation, lambda: Double): Array[Float] = {
      val rank = ne.k
      initialize(rank)
      fillAtA(ne.ata, lambda)
      val x = NNLS.solve(ata, ne.atb, workspace)
      ne.reset()
      x.map(x => x.toFloat)
    }

    /**
     * Given a triangular matrix in the order of fillXtX above, compute the full symmetric square
     * matrix that it represents, storing it into destMatrix.
     */
    private def fillAtA(triAtA: Array[Double], lambda: Double): Unit = {
      var i = 0
      var pos = 0
      var a = 0.0
      while (i < rank) {
        var j = 0
        while (j <= i) {
          a = triAtA(pos)
          ata(i * rank + j) = a
          ata(j * rank + i) = a
          pos += 1
          j += 1
        }
        ata(i * rank + i) += lambda
        i += 1
      }
    }
  }

  /**
   * Representing a normal equation to solve the following weighted least squares problem:
   *
   * minimize \sum,,i,, c,,i,, (a,,i,,^T^ x - d,,i,,)^2^ + lambda * x^T^ x.
   *
   * Its normal equation is given by
   *
   * \sum,,i,, c,,i,, (a,,i,, a,,i,,^T^ x - d,,i,, a,,i,,) + lambda * x = 0.
   *
   * Distributing and letting b,,i,, = c,,i,, * d,,i,,
   *
   * \sum,,i,, c,,i,, a,,i,, a,,i,,^T^ x - b,,i,, a,,i,, + lambda * x = 0.
   */
  private[recommendation] class NormalEquation(val k: Int) extends Serializable {

    /** Number of entries in the upper triangular part of a k-by-k matrix. */
    val triK = k * (k + 1) / 2
    /** A^T^ * A */
    val ata = new Array[Double](triK)
    /** A^T^ * b */
    val atb = new Array[Double](k)

    private val da = new Array[Double](k)
    private val upper = "U"

    private def copyToDouble(a: Array[Float]): Unit = {
      var i = 0
      while (i < k) {
        da(i) = a(i)
        i += 1
      }
    }

    /** Adds an observation. */
    def add(a: Array[Float], b: Double, c: Double = 1.0): NormalEquation = {
      require(c >= 0.0)
      require(a.length == k)
      copyToDouble(a)
      BLAS.nativeBLAS.dspr(upper, k, c, da, 1, ata)
      if (b != 0.0) {
        BLAS.nativeBLAS.daxpy(k, b, da, 1, atb, 1)
      }
      this
    }

    /** Merges another normal equation object. */
    def merge(other: NormalEquation): NormalEquation = {
      require(other.k == k)
      BLAS.nativeBLAS.daxpy(ata.length, 1.0, other.ata, 1, ata, 1)
      BLAS.nativeBLAS.daxpy(atb.length, 1.0, other.atb, 1, atb, 1)
      this
    }

    /** Resets everything to zero, which should be called after each solve. */
    def reset(): Unit = {
      ju.Arrays.fill(ata, 0.0)
      ju.Arrays.fill(atb, 0.0)
    }
  }

  /**
   * Implementation of the NMF algorithm.
   * @param ratings rating data
   * @param rank rank
   * @param numBlocks (numUserBlocks, numItemBlocks)
   * @param maxIter max iterations
   * @param regParam regularization parameter
   * @param intermediateRDDStorageLevel intermediate RDD StorageLevel
   * @param finalRDDStorageLevel final RDD StorageLevel
   * @param checkpointInterval  checkpoint interval
   * @param seed seed for initialize factors
   * @param ord order of ID
   * @tparam ID class tag
   * @return
   */
  def train[ID: ClassTag](
    ratings: RDD[Rating[ID]],
    rank: Int = 10,
    numBlocks: (Int, Int) = (10, 10),
    maxIter: Int = 10,
    regParam: Double = 0.1,
    intermediateRDDStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK,
    finalRDDStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK,
    checkpointInterval: Int = 10,
    seed: Long = 0L)(
    implicit ord: Ordering[ID]): (RDD[(ID, Array[Float])], RDD[(ID, Array[Float])]) = {

    require(!ratings.isEmpty(), s"No ratings available from $ratings")
    require(intermediateRDDStorageLevel != StorageLevel.NONE,
      "NMF is not designed to run without persisting intermediate RDDs.")

    val sc = ratings.sparkContext

    // Precompute the rating dependencies of each partition
    val userPart = new NMFPartitioner(numBlocks._1)
    val itemPart = new NMFPartitioner(numBlocks._2)
    val blockRatings = partitionRatings(ratings, userPart, itemPart)
      .persist(intermediateRDDStorageLevel)
    val (userInBlocks, userOutBlocks) =
      makeBlocks("user", blockRatings, userPart, itemPart, intermediateRDDStorageLevel)
    userOutBlocks.count()    // materialize blockRatings and user blocks
    val swappedBlockRatings = blockRatings.map {
      case ((userBlockId, itemBlockId), RatingBlock(userIds, itemIds, localRatings)) =>
        ((itemBlockId, userBlockId), RatingBlock(itemIds, userIds, localRatings))
    }
    val (itemInBlocks, itemOutBlocks) =
      makeBlocks("item", swappedBlockRatings, itemPart, userPart, intermediateRDDStorageLevel)
    itemOutBlocks.count()    // materialize item blocks

    // Encoders for storing each user/item's partition ID and index within its partition using a
    // single integer; used as an optimization
    val userNMFLocalIndexEncoder = new NMFLocalIndexEncoder(userPart.numPartitions)
    val itemNMFLocalIndexEncoder = new NMFLocalIndexEncoder(itemPart.numPartitions)

    // These are the user and item factor matrices that, once trained, are multiplied together to
    // estimate the rating matrix.  The two matrices are stored in RDDs, partitioned by column such
    // that each factor column resides on the same Spark worker as its corresponding user or item.
    val seedGen = new XORShiftRandom(seed)
    var userFactors = initialize(userInBlocks, rank, seedGen.nextLong())
    var itemFactors = initialize(itemInBlocks, rank, seedGen.nextLong())

    var previousCheckpointFile: Option[String] = None
    val shouldCheckpoint: Int => Boolean = (iter) =>
      sc.checkpointDir.isDefined && checkpointInterval != -1 && (iter % checkpointInterval == 0)
    val deletePreviousCheckpointFile: () => Unit = () =>
      previousCheckpointFile.foreach { file =>
        try {
          val checkpointFile = new Path(file)
          checkpointFile.getFileSystem(sc.hadoopConfiguration).delete(checkpointFile, true)
        } catch {
          case e: IOException =>
            logWarning(s"Cannot delete checkpoint file $file:", e)
        }
      }

    val isHighDimensional = chooseSolver(ratings.sparkContext)

    val computeFactors = if (isHighDimensional) {
      logInfo("high dimensional branch")
      (srcFactorBlocks: RDD[(Int, FactorBlock)],
        srcOutBlocks: RDD[(Int, OutBlock)],
        dstInBlocks: RDD[(Int, InBlock[ID])],
        rank: Int,
        regParam: Double,
        srcEncoder: NMFLocalIndexEncoder) =>
        computeFactorsACD(srcFactorBlocks, srcOutBlocks, dstInBlocks, rank, regParam, srcEncoder)
    } else {
      logInfo("low dimensional branch")
      val solver = new NNLSSolver()
      (srcFactorBlocks: RDD[(Int, FactorBlock)],
        srcOutBlocks: RDD[(Int, OutBlock)],
        dstInBlocks: RDD[(Int, InBlock[ID])],
        rank: Int,
        regParam: Double,
        srcEncoder: NMFLocalIndexEncoder) =>
        computeFactorsNNLS(
          srcFactorBlocks, srcOutBlocks, dstInBlocks, rank, regParam, srcEncoder, solver)
    }

    var previousCachedItemFactors: Option[RDD[(Int, FactorBlock)]] = None
    for (iter <- 0 until maxIter) {
      itemFactors = computeFactors(userFactors, userOutBlocks, itemInBlocks,
        rank, regParam, userNMFLocalIndexEncoder)
      if (shouldCheckpoint(iter)) {
        itemFactors.setName(s"itemFactors-$iter").persist(intermediateRDDStorageLevel)
        itemFactors.checkpoint()
        itemFactors.count() // checkpoint item factors and cut lineage
        itemFactors.cleanShuffleDependencies()
        deletePreviousCheckpointFile()
        previousCachedItemFactors.foreach(_.unpersist())
        previousCheckpointFile = itemFactors.getCheckpointFile
        previousCachedItemFactors = Option(itemFactors)
      }
      userFactors = computeFactors(itemFactors, itemOutBlocks, userInBlocks,
        rank, regParam, itemNMFLocalIndexEncoder)
    }

    val userIdAndFactors = userInBlocks
      .mapValues(_.srcIds)
      .join(userFactors)
      .mapPartitions({ items =>
        items.flatMap { case (_, (ids, factors)) =>
          ids.iterator.zip(factors.iterator)
        }
        // Preserve the partitioning because IDs are consistent
        // with the partitioners in userInBlocks
        // and userFactors.
      }, preservesPartitioning = true)
      .setName("userFactors")
      .persist(finalRDDStorageLevel)
    val itemIdAndFactors = itemInBlocks
      .mapValues(_.srcIds)
      .join(itemFactors)
      .mapPartitions({ items =>
        items.flatMap { case (_, (ids, factors)) =>
          ids.iterator.zip(factors.iterator)
        }
      }, preservesPartitioning = true)
      .setName("itemFactors")
      .persist(finalRDDStorageLevel)
    if (finalRDDStorageLevel != StorageLevel.NONE) {
      userIdAndFactors.count()
      userInBlocks.unpersist()
      userOutBlocks.unpersist()
      itemOutBlocks.unpersist()
      blockRatings.unpersist()
      itemIdAndFactors.count()
      itemFactors.unpersist()
      itemInBlocks.unpersist()
    }
    (userIdAndFactors, itemIdAndFactors)
  }

  /**
   * choose solver
   * @param sc sparkContext
   * @return choose high dimensional solver or not
   */
  def chooseSolver(sc: SparkContext): Boolean = {
    val optLevelParamKey = "spark.boostkit.ml.nmf.highDimensional"
    val optLevelStr = sc.conf.getOption(optLevelParamKey).getOrElse("true")
    var optLevel: Boolean = true
    try {
      optLevel = optLevelStr.toBoolean
    } catch {
      case ex: Exception =>
        throw new IllegalArgumentException(s"Parse boostkit parameter" +
          s"($optLevelParamKey) failed, Error reason: ${ex.getMessage} cannot be casted to Boolean")
    }
    optLevel
  }

  /**
   * Factor block that stores factors (Array[Float]) in an Array.
   */
  private type FactorBlock = Array[Array[Float]]

  /**
   * A mapping of the columns of the items factor matrix that are needed when calculating each row
   * of the users factor matrix, and vice versa.
   *
   * Specifically, when calculating a user factor vector, since only those columns of the items
   * factor matrix that correspond to the items that that user has rated are needed, we can avoid
   * having to repeatedly copy the entire items factor matrix to each worker later in the algorithm
   * by precomputing these dependencies for all users, storing them in an RDD of `OutBlock`s.  The
   * items' dependencies on the columns of the users factor matrix is computed similarly.
   *
   * =Example=
   *
   * Using the example provided in the `InBlock` Scaladoc, `userOutBlocks` would look like the
   * following:
   *
   * {{{
   *     userOutBlocks.collect() == Seq(
   *       0 -> Array(Array(0, 1), Array(0, 1)),
   *       1 -> Array(Array(0), Array(0))
   *     )
   * }}}
   *
   * Each value in this map-like sequence is of type `Array[Array[Int]]`.  The values in the
   * inner array are the ranks of the sorted user IDs in that partition; so in the example above,
   * `Array(0, 1)` in partition 0 refers to user IDs 0 and 6, since when all unique user IDs in
   * partition 0 are sorted, 0 is the first ID and 6 is the second.  The position of each inner
   * array in its enclosing outer array denotes the partition number to which item IDs map; in the
   * example, the first `Array(0, 1)` is in position 0 of its outer array, denoting item IDs that
   * map to partition 0.
   *
   * In summary, the data structure encodes the following information:
   *
   * *  There are ratings with user IDs 0 and 6 (encoded in `Array(0, 1)`, where 0 and 1 are the
   * indices of the user IDs 0 and 6 on partition 0) whose item IDs map to partitions 0 and 1
   * (represented by the fact that `Array(0, 1)` appears in both the 0th and 1st positions).
   *
   * *  There are ratings with user ID 3 (encoded in `Array(0)`, where 0 is the index of the user
   * ID 3 on partition 1) whose item IDs map to partitions 0 and 1 (represented by the fact that
   * `Array(0)` appears in both the 0th and 1st positions).
   */
  private type OutBlock = Array[Array[Int]]

  /**
   * In-link block for computing user and item factor matrices.
   *
   * The NMF algorithm partitions the columns of the users factor matrix evenly among Spark workers.
   * Since each column of the factor matrix is calculated using the known ratings of the correspond-
   * ing user, and since the ratings don't change across iterations, the NMF algorithm preshuffles
   * the ratings to the appropriate partitions, storing them in `InBlock` objects.
   *
   * The ratings shuffled by item ID are computed similarly and also stored in `InBlock` objects.
   * Note that this means every rating is stored twice, once as shuffled by user ID and once by item
   * ID.  This is a necessary tradeoff, since in general a rating will not be on the same worker
   * when partitioned by user as by item.
   *
   * =Example=
   *
   * Say we have a small collection of eight items to offer the seven users in our application.  We
   * have some known ratings given by the users, as seen in the matrix below:
   *
   * {{{
   *                       Items
   *            0   1   2   3   4   5   6   7
   *          +---+---+---+---+---+---+---+---+
   *        0 |   |0.1|   |   |0.4|   |   |0.7|
   *          +---+---+---+---+---+---+---+---+
   *        1 |   |   |   |   |   |   |   |   |
   *          +---+---+---+---+---+---+---+---+
   *     U  2 |   |   |   |   |   |   |   |   |
   *     s    +---+---+---+---+---+---+---+---+
   *     e  3 |   |3.1|   |   |3.4|   |   |3.7|
   *     r    +---+---+---+---+---+---+---+---+
   *     s  4 |   |   |   |   |   |   |   |   |
   *          +---+---+---+---+---+---+---+---+
   *        5 |   |   |   |   |   |   |   |   |
   *          +---+---+---+---+---+---+---+---+
   *        6 |   |6.1|   |   |6.4|   |   |6.7|
   *          +---+---+---+---+---+---+---+---+
   * }}}
   *
   * The ratings are represented as an RDD, passed to the `partitionRatings` method as the `ratings`
   * parameter:
   *
   * {{{
   *     ratings.collect() == Seq(
   *       Rating(0, 1, 0.1f),
   *       Rating(0, 4, 0.4f),
   *       Rating(0, 7, 0.7f),
   *       Rating(3, 1, 3.1f),
   *       Rating(3, 4, 3.4f),
   *       Rating(3, 7, 3.7f),
   *       Rating(6, 1, 6.1f),
   *       Rating(6, 4, 6.4f),
   *       Rating(6, 7, 6.7f)
   *     )
   * }}}
   *
   * Say that we are using two partitions to calculate each factor matrix:
   *
   * {{{
   *     val userPart = new NMFPartitioner(2)
   *     val itemPart = new NMFPartitioner(2)
   *     val blockRatings = partitionRatings(ratings, userPart, itemPart)
   * }}}
   *
   * Ratings are mapped to partitions using the user/item IDs modulo the number of partitions.  With
   * two partitions, ratings with even-valued user IDs are shuffled to partition 0 while those with
   * odd-valued user IDs are shuffled to partition 1:
   *
   * {{{
   *     userInBlocks.collect() == Seq(
   *       0 -> Seq(
   *              // Internally, the class stores the ratings in a more optimized format than
   *              // a sequence of `Rating`s, but for clarity we show it as such here.
   *              Rating(0, 1, 0.1f),
   *              Rating(0, 4, 0.4f),
   *              Rating(0, 7, 0.7f),
   *              Rating(6, 1, 6.1f),
   *              Rating(6, 4, 6.4f),
   *              Rating(6, 7, 6.7f)
   *            ),
   *       1 -> Seq(
   *              Rating(3, 1, 3.1f),
   *              Rating(3, 4, 3.4f),
   *              Rating(3, 7, 3.7f)
   *            )
   *     )
   * }}}
   *
   * Similarly, ratings with even-valued item IDs are shuffled to partition 0 while those with
   * odd-valued item IDs are shuffled to partition 1:
   *
   * {{{
   *     itemInBlocks.collect() == Seq(
   *       0 -> Seq(
   *              Rating(0, 4, 0.4f),
   *              Rating(3, 4, 3.4f),
   *              Rating(6, 4, 6.4f)
   *            ),
   *       1 -> Seq(
   *              Rating(0, 1, 0.1f),
   *              Rating(0, 7, 0.7f),
   *              Rating(3, 1, 3.1f),
   *              Rating(3, 7, 3.7f),
   *              Rating(6, 1, 6.1f),
   *              Rating(6, 7, 6.7f)
   *            )
   *     )
   * }}}
   *
   * @param srcIds src ids (ordered)
   * @param dstPtrs dst pointers. Elements in range [dstPtrs(i), dstPtrs(i+1)) of dst indices and
   *                ratings are associated with srcIds(i).
   * @param dstEncodedIndices encoded dst indices
   * @param ratings ratings
   * @see [[NMFLocalIndexEncoder]]
   */
  private[recommendation] case class InBlock[@specialized(Int, Long) ID: ClassTag](
    srcIds: Array[ID],
    dstPtrs: Array[Int],
    dstEncodedIndices: Array[Int],
    ratings: Array[Float]) {
    /** Size of the block. */
    def size: Int = ratings.length
    require(dstEncodedIndices.length == size)
    require(dstPtrs.length == srcIds.length + 1)
  }

  /**
   * Initializes factors randomly given the in-link blocks.
   *
   * @param inBlocks in-link blocks
   * @param rank rank
   * @return initialized factor blocks
   */
  private def initialize[ID](
    inBlocks: RDD[(Int, InBlock[ID])],
    rank: Int,
    seed: Long): RDD[(Int, FactorBlock)] = {
    // Choose a unit vector uniformly at random from the unit sphere, but from the
    // "first quadrant" where all elements are nonnegative. This can be done by choosing
    // elements distributed as Normal(0,1) and taking the absolute value, and then normalizing.
    // This appears to create factorizations that have a slightly better reconstruction
    // (<1%) compared picking elements uniformly at random in [0,1].
    inBlocks.mapPartitions({ iter =>
      iter.map {
        case (srcBlockId, inBlock) =>
          val random = new XORShiftRandom(byteswap64(seed ^ srcBlockId))
          val factors = Array.fill(inBlock.srcIds.length) {
            val factor = Array.fill(rank)(random.nextGaussian().toFloat)
            val nrm = BLAS.nativeBLAS.snrm2(rank, factor, 1)
            BLAS.nativeBLAS.sscal(rank, 1.0f / nrm, factor, 1)
            factor.map(x => {math.max(0, x)})
          }
          (srcBlockId, factors)
      }
    }, preservesPartitioning = true)
  }

  /**
   * A rating block that contains src IDs, dst IDs, and ratings, stored in primitive arrays.
   */
  private[recommendation] case class RatingBlock[@specialized(Int, Long) ID: ClassTag](
    srcIds: Array[ID],
    dstIds: Array[ID],
    ratings: Array[Float]) {
    /** Size of the block. */
    def size: Int = srcIds.length
    require(dstIds.length == srcIds.length)
    require(ratings.length == srcIds.length)
  }

  /**
   * Builder for [[RatingBlock]]. `mutable.ArrayBuilder` is used to avoid boxing/unboxing.
   */
  private[recommendation] class RatingBlockBuilder[@specialized(Int, Long) ID: ClassTag]
    extends Serializable {

    private val srcIds = mutable.ArrayBuilder.make[ID]
    private val dstIds = mutable.ArrayBuilder.make[ID]
    private val ratings = mutable.ArrayBuilder.make[Float]
    var size = 0

    /** Adds a rating. */
    def add(r: Rating[ID]): this.type = {
      size += 1
      srcIds += r.user
      dstIds += r.item
      ratings += r.rating
      this
    }

    /** Merges another [[RatingBlockBuilder]]. */
    def merge(other: RatingBlock[ID]): this.type = {
      size += other.srcIds.length
      srcIds ++= other.srcIds
      dstIds ++= other.dstIds
      ratings ++= other.ratings
      this
    }

    /** Builds a [[RatingBlock]]. */
    def build(): RatingBlock[ID] = {
      RatingBlock[ID](srcIds.result(), dstIds.result(), ratings.result())
    }
  }

  /**
   * Groups an RDD of [[Rating]]s by the user partition and item partition to which each `Rating`
   * maps according to the given partitioners.  The returned pair RDD holds the ratings, encoded in
   * a memory-efficient format but otherwise unchanged, keyed by the (user partition ID, item
   * partition ID) pair.
   *
   * Performance note: This is an expensive operation that performs an RDD shuffle.
   *
   * Implementation note: This implementation produces the same result as the following but
   * generates fewer intermediate objects:
   *
   * {{{
   *     ratings.map { r =>
   *       ((srcPart.getPartition(r.user), dstPart.getPartition(r.item)), r)
   *     }.aggregateByKey(new RatingBlockBuilder)(
   *         seqOp = (b, r) => b.add(r),
   *         combOp = (b0, b1) => b0.merge(b1.build()))
   *       .mapValues(_.build())
   * }}}
   *
   * @param ratings raw ratings
   * @param srcPart partitioner for src IDs
   * @param dstPart partitioner for dst IDs
   * @return an RDD of rating blocks in the form of ((srcBlockId, dstBlockId), ratingBlock)
   */
  private def partitionRatings[ID: ClassTag](
    ratings: RDD[Rating[ID]],
    srcPart: Partitioner,
    dstPart: Partitioner): RDD[((Int, Int), RatingBlock[ID])] = {
    val numPartitions = srcPart.numPartitions * dstPart.numPartitions
    ratings.mapPartitions { iter =>
      val builders = Array.fill(numPartitions)(new RatingBlockBuilder[ID])
      iter.flatMap { r =>
        val srcBlockId = srcPart.getPartition(r.user)
        val dstBlockId = dstPart.getPartition(r.item)
        val idx = srcBlockId + srcPart.numPartitions * dstBlockId
        val builder = builders(idx)
        builder.add(r)
        if (builder.size >= 2048) { // 2048 * (3 * 4) = 24k
          builders(idx) = new RatingBlockBuilder
          Iterator.single(((srcBlockId, dstBlockId), builder.build()))
        } else {
          Iterator.empty
        }
      } ++ {
        builders.iterator.zipWithIndex.filter(_._1.size > 0).map { case (block, idx) =>
          val srcBlockId = idx % srcPart.numPartitions
          val dstBlockId = idx / srcPart.numPartitions
          ((srcBlockId, dstBlockId), block.build())
        }
      }
    }.groupByKey().mapValues { blocks =>
      val builder = new RatingBlockBuilder[ID]
      blocks.foreach(builder.merge)
      builder.build()
    }.setName("ratingBlocks")
  }

  /**
   * Builder for uncompressed in-blocks of (srcId, dstEncodedIndex, rating) tuples.
   *
   * @param encoder encoder for dst indices
   */
  private[recommendation] class UncompressedInBlockBuilder[@specialized(Int, Long) ID: ClassTag](
    encoder: NMFLocalIndexEncoder)(
    implicit ord: Ordering[ID]) {

    private val srcIds = mutable.ArrayBuilder.make[ID]
    private val dstEncodedIndices = mutable.ArrayBuilder.make[Int]
    private val ratings = mutable.ArrayBuilder.make[Float]

    /**
     * Adds a dst block of (srcId, dstLocalIndex, rating) tuples.
     *
     * @param dstBlockId dst block ID
     * @param srcIds original src IDs
     * @param dstLocalIndices dst local indices
     * @param ratings ratings
     */
    def add(
      dstBlockId: Int,
      srcIds: Array[ID],
      dstLocalIndices: Array[Int],
      ratings: Array[Float]): this.type = {
      val sz = srcIds.length
      require(dstLocalIndices.length == sz)
      require(ratings.length == sz)
      this.srcIds ++= srcIds
      this.ratings ++= ratings
      var j = 0
      while (j < sz) {
        this.dstEncodedIndices += encoder.encode(dstBlockId, dstLocalIndices(j))
        j += 1
      }
      this
    }

    /** Builds a [[UncompressedInBlock]]. */
    def build(): UncompressedInBlock[ID] = {
      new UncompressedInBlock(srcIds.result(), dstEncodedIndices.result(), ratings.result())
    }
  }

  /**
   * A block of (srcId, dstEncodedIndex, rating) tuples stored in primitive arrays.
   */
  private[recommendation] class UncompressedInBlock[@specialized(Int, Long) ID: ClassTag](
    val srcIds: Array[ID],
    val dstEncodedIndices: Array[Int],
    val ratings: Array[Float])(
    implicit ord: Ordering[ID]) {

    /** Size the of block. */
    def length: Int = srcIds.length

    /**
     * Compresses the block into an `InBlock`. The algorithm is the same as converting a sparse
     * matrix from coordinate list (COO) format into compressed sparse column (CSC) format.
     * Sorting is done using Spark's built-in Timsort to avoid generating too many objects.
     */
    def compress(): InBlock[ID] = {
      val sz = length
      assert(sz > 0, "Empty in-link block should not exist.")
      sort()
      val uniqueSrcIdsBuilder = mutable.ArrayBuilder.make[ID]
      val dstCountsBuilder = mutable.ArrayBuilder.make[Int]
      var preSrcId = srcIds(0)
      uniqueSrcIdsBuilder += preSrcId
      var curCount = 1
      var i = 1
      while (i < sz) {
        val srcId = srcIds(i)
        if (srcId != preSrcId) {
          uniqueSrcIdsBuilder += srcId
          dstCountsBuilder += curCount
          preSrcId = srcId
          curCount = 0
        }
        curCount += 1
        i += 1
      }
      dstCountsBuilder += curCount
      val uniqueSrcIds = uniqueSrcIdsBuilder.result()
      val numUniqueSrdIds = uniqueSrcIds.length
      val dstCounts = dstCountsBuilder.result()
      val dstPtrs = new Array[Int](numUniqueSrdIds + 1)
      var sum = 0
      i = 0
      while (i < numUniqueSrdIds) {
        sum += dstCounts(i)
        i += 1
        dstPtrs(i) = sum
      }
      InBlock(uniqueSrcIds, dstPtrs, dstEncodedIndices, ratings)
    }

    private def sort(): Unit = {
      val sz = length
      // Since there might be interleaved log messages, we insert a unique id for easy pairing.
      val sortId = Utils.random.nextInt()
      logDebug(s"Start sorting an uncompressed in-block of size $sz. (sortId = $sortId)")
      val start = System.nanoTime()
      val sorter = new Sorter(new UncompressedInBlockSort[ID])
      sorter.sort(this, 0, length, Ordering[KeyWrapper[ID]])
      val duration = (System.nanoTime() - start) / 1e9
      logDebug(s"Sorting took $duration seconds. (sortId = $sortId)")
    }
  }

  /**
   * A wrapper that holds a primitive key.
   *
   * @see [[UncompressedInBlockSort]]
   */
  private class KeyWrapper[@specialized(Int, Long) ID: ClassTag](
    implicit ord: Ordering[ID]) extends Ordered[KeyWrapper[ID]] {

    var key: ID = _

    override def compare(that: KeyWrapper[ID]): Int = {
      ord.compare(key, that.key)
    }

    def setKey(key: ID): this.type = {
      this.key = key
      this
    }
  }

  /**
   * [[SortDataFormat]] of [[UncompressedInBlock]] used by [[Sorter]].
   */
  private class UncompressedInBlockSort[@specialized(Int, Long) ID: ClassTag](
    implicit ord: Ordering[ID])
    extends SortDataFormat[KeyWrapper[ID], UncompressedInBlock[ID]] {

    override def newKey(): KeyWrapper[ID] = new KeyWrapper()

    override def getKey(
      data: UncompressedInBlock[ID],
      pos: Int,
      reuse: KeyWrapper[ID]): KeyWrapper[ID] = {
      if (reuse == null) {
        new KeyWrapper().setKey(data.srcIds(pos))
      } else {
        reuse.setKey(data.srcIds(pos))
      }
    }

    override def getKey(
      data: UncompressedInBlock[ID],
      pos: Int): KeyWrapper[ID] = {
      getKey(data, pos, null)
    }

    private def swapElements[@specialized(Int, Float) T](
      data: Array[T],
      pos0: Int,
      pos1: Int): Unit = {
      val tmp = data(pos0)
      data(pos0) = data(pos1)
      data(pos1) = tmp
    }

    override def swap(data: UncompressedInBlock[ID], pos0: Int, pos1: Int): Unit = {
      swapElements(data.srcIds, pos0, pos1)
      swapElements(data.dstEncodedIndices, pos0, pos1)
      swapElements(data.ratings, pos0, pos1)
    }

    override def copyRange(
      src: UncompressedInBlock[ID],
      srcPos: Int,
      dst: UncompressedInBlock[ID],
      dstPos: Int,
      length: Int): Unit = {
      System.arraycopy(src.srcIds, srcPos, dst.srcIds, dstPos, length)
      System.arraycopy(src.dstEncodedIndices, srcPos, dst.dstEncodedIndices, dstPos, length)
      System.arraycopy(src.ratings, srcPos, dst.ratings, dstPos, length)
    }

    override def allocate(length: Int): UncompressedInBlock[ID] = {
      new UncompressedInBlock(
        new Array[ID](length), new Array[Int](length), new Array[Float](length))
    }

    override def copyElement(
      src: UncompressedInBlock[ID],
      srcPos: Int,
      dst: UncompressedInBlock[ID],
      dstPos: Int): Unit = {
      dst.srcIds(dstPos) = src.srcIds(srcPos)
      dst.dstEncodedIndices(dstPos) = src.dstEncodedIndices(srcPos)
      dst.ratings(dstPos) = src.ratings(srcPos)
    }
  }

  /**
   * Creates in-blocks and out-blocks from rating blocks.
   *
   * @param prefix prefix for in/out-block names
   * @param ratingBlocks rating blocks
   * @param srcPart partitioner for src IDs
   * @param dstPart partitioner for dst IDs
   * @return (in-blocks, out-blocks)
   */
  private def makeBlocks[ID: ClassTag](
    prefix: String,
    ratingBlocks: RDD[((Int, Int), RatingBlock[ID])],
    srcPart: Partitioner,
    dstPart: Partitioner,
    storageLevel: StorageLevel)(
    implicit srcOrd: Ordering[ID]): (RDD[(Int, InBlock[ID])], RDD[(Int, OutBlock)]) = {
    val inBlocks = ratingBlocks.map {
      case ((srcBlockId, dstBlockId), RatingBlock(srcIds, dstIds, ratings)) =>
        // The implementation is a faster version of
        // val dstIdToLocalIndex = dstIds.toSet.toSeq.sorted.zipWithIndex.toMap
        val start = System.nanoTime()
        val dstIdSet = new OpenHashSet[ID](1 << 20)
        dstIds.foreach(dstIdSet.add)
        val sortedDstIds = new Array[ID](dstIdSet.size)
        var i = 0
        var pos = dstIdSet.nextPos(0)
        while (pos != -1) {
          sortedDstIds(i) = dstIdSet.getValue(pos)
          pos = dstIdSet.nextPos(pos + 1)
          i += 1
        }
        assert(i == dstIdSet.size)
        Sorting.quickSort(sortedDstIds)
        val dstIdToLocalIndex = new OpenHashMap[ID, Int](sortedDstIds.length)
        i = 0
        while (i < sortedDstIds.length) {
          dstIdToLocalIndex.update(sortedDstIds(i), i)
          i += 1
        }
        logDebug(
          "Converting to local indices took " + (System.nanoTime() - start) / 1e9 + " seconds.")
        val dstLocalIndices = dstIds.map(dstIdToLocalIndex.apply)
        (srcBlockId, (dstBlockId, srcIds, dstLocalIndices, ratings))
    }.groupByKey(new NMFPartitioner(srcPart.numPartitions))
      .mapValues { iter =>
        val builder =
          new UncompressedInBlockBuilder[ID](new NMFLocalIndexEncoder(dstPart.numPartitions))
        iter.foreach { case (dstBlockId, srcIds, dstLocalIndices, ratings) =>
          builder.add(dstBlockId, srcIds, dstLocalIndices, ratings)
        }
        builder.build().compress()
      }.setName(prefix + "InBlocks")
      .persist(storageLevel)
    val outBlocks = inBlocks.mapValues { case InBlock(srcIds, dstPtrs, dstEncodedIndices, _) =>
      val encoder = new NMFLocalIndexEncoder(dstPart.numPartitions)
      val activeIds = Array.fill(dstPart.numPartitions)(mutable.ArrayBuilder.make[Int])
      var i = 0
      val seen = new Array[Boolean](dstPart.numPartitions)
      while (i < srcIds.length) {
        var j = dstPtrs(i)
        ju.Arrays.fill(seen, false)
        while (j < dstPtrs(i + 1)) {
          val dstBlockId = encoder.blockId(dstEncodedIndices(j))
          if (!seen(dstBlockId)) {
            activeIds(dstBlockId) += i // add the local index in this out-block
            seen(dstBlockId) = true
          }
          j += 1
        }
        i += 1
      }
      activeIds.map { x =>
        x.result()
      }
    }.setName(prefix + "OutBlocks")
      .persist(storageLevel)
    (inBlocks, outBlocks)
  }

  /**
   * Compute dst factors by constructing and solving least square problems.
   *
   * @param srcFactorBlocks src factors
   * @param srcOutBlocks src out-blocks
   * @param dstInBlocks dst in-blocks
   * @param rank rank
   * @param regParam regularization constant
   * @param srcEncoder encoder for src local indices
   * @param solver solver for least squares problems
   * @return dst factors
   */
  private def computeFactorsNNLS[ID](
    srcFactorBlocks: RDD[(Int, FactorBlock)],
    srcOutBlocks: RDD[(Int, OutBlock)],
    dstInBlocks: RDD[(Int, InBlock[ID])],
    rank: Int,
    regParam: Double,
    srcEncoder: NMFLocalIndexEncoder,
    solver: LeastSquaresNESolver): RDD[(Int, FactorBlock)] = {
    val numSrcBlocks = srcFactorBlocks.partitions.length
    val srcOut = srcOutBlocks.join(srcFactorBlocks).flatMap {
      case (srcBlockId, (srcOutBlock, srcFactors)) =>
        srcOutBlock.iterator.zipWithIndex.map { case (activeIndices, dstBlockId) =>
          (dstBlockId, (srcBlockId, activeIndices.map(idx => srcFactors(idx))))
        }
    }
    val merged = srcOut.groupByKey(new NMFPartitioner(dstInBlocks.partitions.length))

    // SPARK-28927: Nondeterministic RDDs causes inconsistent in/out blocks in case of rerun.
    // It can cause runtime error when matching in/out user/item blocks.
    val isBlockRDDNondeterministic =
    dstInBlocks.outputDeterministicLevel == DeterministicLevel.INDETERMINATE ||
      srcOutBlocks.outputDeterministicLevel == DeterministicLevel.INDETERMINATE

    dstInBlocks.join(merged).mapValues {
      case (InBlock(dstIds, srcPtrs, srcEncodedIndices, ratings), srcFactors) =>
        val sortedSrcFactors = new Array[FactorBlock](numSrcBlocks)
        srcFactors.foreach { case (srcBlockId, factors) =>
          sortedSrcFactors(srcBlockId) = factors
        }
        val dstFactors = new Array[Array[Float]](dstIds.length)
        var j = 0
        val ls = new NormalEquation(rank)
        while (j < dstIds.length) {
          ls.reset()
          var i = srcPtrs(j)
          var numExplicits = 0
          while (i < srcPtrs(j + 1)) {
            val encoded = srcEncodedIndices(i)
            val blockId = srcEncoder.blockId(encoded)
            val localIndex = srcEncoder.localIndex(encoded)
            var srcFactor: Array[Float] = null
            try {
              srcFactor = sortedSrcFactors(blockId)(localIndex)
            } catch {
              case a: ArrayIndexOutOfBoundsException if isBlockRDDNondeterministic =>
                val errMsg = "A failure detected when matching In/Out blocks of users/items. " +
                  "Because at least one In/Out block RDD is found to be nondeterministic now, " +
                  "the issue is probably caused by nondeterministic input data. You can try to " +
                  "checkpoint training data to make it deterministic. If you do `repartition` + " +
                  "`sample` or `randomSplit`, you can also try to sort it before `sample` or " +
                  "`randomSplit` to make it deterministic."
                throw new SparkException(errMsg, a)
            }
            val rating = ratings(i)
            ls.add(srcFactor, rating)
            numExplicits += 1
            i += 1
          }
          // Weight lambda by the number of explicit ratings based on the ALS-WR paper.
          dstFactors(j) = solver.solve(ls, numExplicits * regParam)
          j += 1
        }
        dstFactors
    }
  }

  /**
   * update factors by NMFSolver
   * @param srcFactorBlocks src factors
   * @param srcOutBlocks src out-blocks
   * @param dstInBlocks dst in-blocks
   * @param rank rank
   * @param regParam regularization constant
   * @param srcEncoder encoder for src local indices
   * @return updated factors
   */
  private def computeFactorsACD[ID](
    srcFactorBlocks: RDD[(Int, FactorBlock)],
    srcOutBlocks: RDD[(Int, OutBlock)],
    dstInBlocks: RDD[(Int, InBlock[ID])],
    rank: Int,
    regParam: Double,
    srcEncoder: NMFLocalIndexEncoder): RDD[(Int, FactorBlock)] = {
    val numSrcBlocks = srcFactorBlocks.partitions.length
    val srcOut = srcOutBlocks.join(srcFactorBlocks).flatMap {
      case (srcBlockId, (srcOutBlock, srcFactors)) =>
        srcOutBlock.iterator.zipWithIndex.map { case (activeIndices, dstBlockId) =>
          (dstBlockId, (srcBlockId, activeIndices.map(idx => srcFactors(idx))))
        }
    }
    val merged = srcOut.groupByKey(new NMFPartitioner(dstInBlocks.partitions.length))

    dstInBlocks.join(merged).mapValues {
      case (InBlock(dstIds, srcPtrs, srcEncodedIndices, ratings), srcFactors) =>
        NMFSolver.updateFactors(
          dstIds,
          srcPtrs,
          srcEncodedIndices,
          ratings,
          srcFactors,
          numSrcBlocks,
          rank,
          regParam.toFloat,
          srcEncoder)
    }
  }

  /**
   * Partitioner used by NMF. We require that getPartition is a projection. That is, for any key k,
   * we have getPartition(getPartition(k)) = getPartition(k). Since the default HashPartitioner
   * satisfies this requirement, we simply use a type alias here.
   */
  private[recommendation] type NMFPartitioner = org.apache.spark.HashPartitioner
}
