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

package org.apache.spark.ml.feature

import org.json4s.{DefaultFormats, JObject}
import org.json4s.JsonDSL._

import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.param.shared.HasWeightCol
import org.apache.spark.ml.tree._
import org.apache.spark.ml.tree.impl.DecisionTreeBucket
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.DefaultParamsReader.Metadata
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, Strategy => OldStrategy}
import org.apache.spark.mllib.tree.model.{RandomForestModel => OldRandomForestModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructType}


private[ml] trait DecisionTreeBucketizerParams extends Params
  with DecisionTreeClassifierParams with HasWeightCol {

  /**
   * Param for bucketedFeatures column name.
   * @group param
   */
  final val bucketedFeaturesCol: Param[String] =
    new Param[String](this, "bucketedFeaturesCol", "bucketedFeatures column name")

  setDefault(bucketedFeaturesCol, "bucketedFeatures")

  /** @group getParam */
  final def getBucketedFeaturesCol: String = $(bucketedFeaturesCol)

  /**
   * Validates and transforms the input schema with the provided param map.
   * copy from [[org.apache.spark.ml.PredictorParams]].validateAndTransformSchema
   *
   * @param schema input schema
   * @param fitting whether this is in fitting
   * @return output schema
   */
  protected def validateAndTransformSchema(
      schema: StructType,
      fitting: Boolean): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    if (fitting) {
      SchemaUtils.checkNumericType(schema, $(labelCol))

      this match {
        case p: HasWeightCol =>
          if (isDefined(p.weightCol) && $(p.weightCol).nonEmpty) {
            SchemaUtils.checkNumericType(schema, $(p.weightCol))
          }
        case _ =>
      }
    }
    SchemaUtils.appendColumn(schema, $(bucketedFeaturesCol), new VectorUDT)
  }

}

/**
 * Decision tree bucketing algorithm for data discretization.
 */
@Since("1.4.0")
class DecisionTreeBucketizer @Since("1.4.0") (
    @Since("1.4.0") override val uid: String)
  extends Estimator[DecisionTreeBucketModel]
  with DecisionTreeBucketizerParams with DecisionTreeClassifierParams with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("dtb"))

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setBucketedFeaturesCol(value: String): this.type = set(bucketedFeaturesCol, value)

  // Override parameter setters from parent trait for Java API compatibility.
  /** @group setParam */
  @Since("1.4.0")
  override def setMaxDepth(value: Int): this.type = set(maxDepth, value)

  /** @group setParam */
  @Since("1.4.0")
  override def setMaxBins(value: Int): this.type = set(maxBins, value)

  /** @group setParam */
  @Since("1.4.0")
  override def setMinInstancesPerNode(value: Int): this.type = set(minInstancesPerNode, value)

  /** @group setParam */
  @Since("1.4.0")
  override def setMinInfoGain(value: Double): this.type = set(minInfoGain, value)

  /** @group expertSetParam */
  @Since("1.4.0")
  override def setMaxMemoryInMB(value: Int): this.type = set(maxMemoryInMB, value)

  /** @group expertSetParam */
  @Since("1.4.0")
  override def setCacheNodeIds(value: Boolean): this.type = set(cacheNodeIds, value)

  /**
   * Specifies how often to checkpoint the cached node IDs.
   * E.g. 10 means that the cache will get checkpointed every 10 iterations.
   * This is only used if cacheNodeIds is true and if the checkpoint directory is set in
   * [[org.apache.spark.SparkContext]].
   * Must be at least 1.
   * (default = 10)
   * @group setParam
   */
  @Since("1.4.0")
  override def setCheckpointInterval(value: Int): this.type = set(checkpointInterval, value)

  /** @group setParam */
  @Since("1.4.0")
  override def setImpurity(value: String): this.type = set(impurity, value)

  /** @group setParam */
  @Since("1.6.0")
  override def setSeed(value: Long): this.type = set(seed, value)

  /** @group setParam */
  @Since("3.0.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  /**
   * Extract [[labelCol]] and [[featuresCol]] from the given dataset,
   * and put it in an RDD with strong types.
   *
   * @param dataset  DataFrame with columns for labels ([[org.apache.spark.sql.types.NumericType]])
   *                 and features (`Vector`).
   * @param numClasses  Number of classes label can take.  Labels must be integers in the range
   *                    [0, numClasses).
   * @note Throws `SparkException` if any label is a non-integer or is negative
   */
  private[ml] def extractLabeledPoints(dataset: Dataset[_], numClasses: Int): RDD[LabeledPoint] = {
    require(numClasses > 0, s"Classifier (in extractLabeledPoints) found numClasses =" +
      s" $numClasses, but requires numClasses > 0.")
    dataset.select(col($(labelCol)), col($(featuresCol))).rdd.map {
      case Row(label: Double, features: Vector) =>
        require(label % 1 == 0 && label >= 0 && label < numClasses, s"Classifier was given" +
          s" dataset with invalid label $label.  Labels must be integers in range" +
          s" [0, $numClasses).")
        LabeledPoint(label, features)
    }
  }

  /**
   * Get the number of classes.  This looks in column metadata first, and if that is missing,
   * then this assumes classes are indexed 0,1,...,numClasses-1 and computes numClasses
   * by finding the maximum label value.
   *
   * Label validation (ensuring all labels are integers >= 0) needs to be handled elsewhere,
   * such as in `extractLabeledPoints()`.
   *
   * @param dataset  Dataset which contains a column [[labelCol]]
   * @param maxNumClasses  Maximum number of classes allowed when inferred from data.  If numClasses
   *                       is specified in the metadata, then maxNumClasses is ignored.
   * @return  number of classes
   * @throws IllegalArgumentException  if metadata does not specify numClasses, and the
   *                                   actual numClasses exceeds maxNumClasses
   */
  private[ml] def getNumClasses(dataset: Dataset[_], maxNumClasses: Int = 100): Int = {
    MetadataUtils.getNumClasses(dataset.schema($(labelCol))) match {
      case Some(n: Int) => n
      case None =>
        // Get number of classes from dataset itself.
        val maxLabelRow: Array[Row] = dataset.select(max($(labelCol))).take(1)
        if (maxLabelRow.isEmpty || maxLabelRow.head.isNullAt(0)) {
          throw new SparkException("ML algorithm was given empty dataset.")
        }
        val maxDoubleLabel: Double = maxLabelRow.head.getDouble(0)
        require((maxDoubleLabel + 1).isValidInt, s"Classifier found max label value =" +
          s" $maxDoubleLabel but requires integers in range [0, ... ${Int.MaxValue})")
        val numClasses = maxDoubleLabel.toInt + 1
        require(numClasses <= maxNumClasses, s"Classifier inferred $numClasses from label values" +
          s" in column $labelCol, but this exceeded the max numClasses ($maxNumClasses) allowed" +
          s" to be inferred from values.  To avoid this error for labels with > $maxNumClasses" +
          s" classes, specify numClasses explicitly in the metadata; this can be done by applying" +
          s" StringIndexer to the label column.")
        logInfo(this.getClass.getCanonicalName + s" inferred $numClasses classes for" +
          s" labelCol=$labelCol since numClasses was not specified in the column metadata.")
        numClasses
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, true)
  }

  override def fit(dataset: Dataset[_]): DecisionTreeBucketModel = {
    // This handles a few items such as schema validation.
    // Developers only need to implement train().
    transformSchema(dataset.schema, logging = true)

    // Cast LabelCol to DoubleType and keep the metadata.
    val labelMeta = dataset.schema($(labelCol)).metadata
    val labelCasted = dataset.withColumn($(labelCol), col($(labelCol)).cast(DoubleType), labelMeta)

    // Cast WeightCol to DoubleType and keep the metadata.
    val casted = this match {
      case p: HasWeightCol =>
        if (isDefined(p.weightCol) && $(p.weightCol).nonEmpty) {
          val weightMeta = dataset.schema($(p.weightCol)).metadata
          labelCasted.withColumn($(p.weightCol), col($(p.weightCol)).cast(DoubleType), weightMeta)
        } else {
          labelCasted
        }
      case _ => labelCasted
    }

    copyValues(train(casted).setParent(this))
  }

  private[ml] def train(dataset: Dataset[_]): DecisionTreeBucketModel = {
    val categoricalFeatures: Map[Int, Int] =
      MetadataUtils.getCategoricalFeatures(dataset.schema($(featuresCol)))
    val numClasses: Int = getNumClasses(dataset)

    val oldDataset: RDD[LabeledPoint] = extractLabeledPoints(dataset, numClasses)
    val strategy = getOldStrategy(categoricalFeatures, numClasses)

    val instr = Instrumentation.create(this, oldDataset)
    instr.logParams(params: _*)

    val trees = DecisionTreeBucket.run(oldDataset, strategy, getSeed, Some(instr))
      .map(_.asInstanceOf[DecisionTreeClassificationModel])

    val numFeatures = oldDataset.first().features.size
    val m = new DecisionTreeBucketModel(uid, trees, numFeatures, numClasses)
    instr.logSuccess(m)
    m
  }

  /** (private[ml]) Train decision trees on an RDD */
  private[ml] def train(data: RDD[LabeledPoint],
      oldStrategy: OldStrategy): DecisionTreeBucketModel = {
    val instr = Instrumentation.create(this, data)
    instr.logParams(params: _*)

    val trees = DecisionTreeBucket.run(data, oldStrategy, getSeed, Some(instr))
      .map(_.asInstanceOf[DecisionTreeClassificationModel])

    val numFeatures = data.first().features.size
    val m = new DecisionTreeBucketModel(uid, trees, numFeatures, oldStrategy.numClasses)
    instr.logSuccess(m)
    m
  }

  /** (private[ml]) Create a Strategy instance to use with the old API. */
  private[ml] def getOldStrategy(
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): OldStrategy = {
    super.getOldStrategy(categoricalFeatures, numClasses, OldAlgo.Classification, getOldImpurity,
      subsamplingRate = 1.0)
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): DecisionTreeBucketizer = defaultCopy(extra)
}

@Since("1.4.0")
object DecisionTreeBucketizer extends DefaultParamsReadable[DecisionTreeBucketizer] {
  /** Accessor for supported impurities: entropy, gini */
  @Since("1.4.0")
  final val supportedImpurities: Array[String] = TreeClassifierParams.supportedImpurities

  @Since("2.0.0")
  override def load(path: String): DecisionTreeBucketizer = super.load(path)
}

/**
 * Decision tree bucket model for data discretization.
 * @param _trees  Decision trees of all features.
 */
@Since("1.4.0")
class DecisionTreeBucketModel private[ml] (
    @Since("1.5.0") override val uid: String,
    private val _trees: Array[DecisionTreeClassificationModel],
    val numFeatures: Int,
    val numClasses: Int)
  extends Model[DecisionTreeBucketModel]
  with DecisionTreeBucketizerParams with DecisionTreeClassifierParams
  with TreeEnsembleModel[DecisionTreeClassificationModel]
  with MLWritable with Serializable {

  require(_trees.nonEmpty, "DecisionTreeBucketModel requires at least 1 tree.")

  /**
   * Construct a decision tree bucket model, with all trees weighted equally.
   *
   * @param trees  Component trees
   */
  private[ml] def this(
      trees: Array[DecisionTreeClassificationModel],
      numFeatures: Int,
      numClasses: Int) =
    this(Identifiable.randomUID("dtb"), trees, numFeatures, numClasses)

  def getNumTrees: Int = _trees.length

  @Since("1.4.0")
  override def trees: Array[DecisionTreeClassificationModel] = _trees

  // Note: We may add support for weights (based on tree performance) later on.
  private lazy val _treeWeights: Array[Double] = Array.fill[Double](_trees.length)(1.0)

  @Since("1.4.0")
  override def treeWeights: Array[Double] = _treeWeights

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, false)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    // val outputData = super.transform(dataset)
    val leafUDF = udf { features: Vector => predictLeaf(features) }
    dataset.withColumn($(bucketedFeaturesCol), leafUDF(col($(featuresCol))))
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): DecisionTreeBucketModel = {
    copyValues(new DecisionTreeBucketModel(uid, _trees, numFeatures, numClasses), extra)
      .setParent(parent)
  }

  @Since("1.4.0")
  override def toString: String = {
    s"DecisionTreeBucketModel (uid=$uid) with $getNumTrees trees"
  }

  /** (private[ml]) Convert to a model in the old API */
  def toOld: OldRandomForestModel = {
    new OldRandomForestModel(OldAlgo.Classification, _trees.map(_.toOld))
  }

  @Since("2.0.0")
  override def write: MLWriter =
    new DecisionTreeBucketModel.DecisionTreeBucketModelWriter(this)
}

@Since("2.0.0")
object DecisionTreeBucketModel extends MLReadable[DecisionTreeBucketModel] {

  @Since("2.0.0")
  override def read: MLReader[DecisionTreeBucketModel] =
    new DecisionTreeBucketModelReader

  @Since("2.0.0")
  override def load(path: String): DecisionTreeBucketModel = super.load(path)

  private[DecisionTreeBucketModel]
  class DecisionTreeBucketModelWriter(instance: DecisionTreeBucketModel)
    extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      // Note: numTrees is not currently used, but could be nice to store for fast querying.
      val extraMetadata: JObject = Map(
        "numFeatures" -> instance.numFeatures,
        "numClasses" -> instance.numClasses,
        "numTrees" -> instance.getNumTrees)
      EnsembleModelReadWrite.saveImpl(instance, path, sparkSession, extraMetadata)
    }
  }

  private class DecisionTreeBucketModelReader
    extends MLReader[DecisionTreeBucketModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[DecisionTreeBucketModel].getName
    private val treeClassName = classOf[DecisionTreeClassificationModel].getName

    override def load(path: String): DecisionTreeBucketModel = {
      implicit val format = DefaultFormats
      val (metadata: Metadata, treesData: Array[(Metadata, Node)], _) =
        EnsembleModelReadWrite.loadImpl(path, sparkSession, className, treeClassName)
      val numFeatures = (metadata.metadata \ "numFeatures").extract[Int]
      val numClasses = (metadata.metadata \ "numClasses").extract[Int]
      val numTrees = (metadata.metadata \ "numTrees").extract[Int]

      val trees: Array[DecisionTreeClassificationModel] = treesData.map {
        case (treeMetadata, root) =>
          val tree =
            new DecisionTreeClassificationModel(treeMetadata.uid, root, numFeatures, numClasses)
          DefaultParamsReader.getAndSetParams(tree, treeMetadata)
          tree
      }
      require(numTrees == trees.length, s"DecisionTreeBucketModel.load expected $numTrees" +
        s" trees based on metadata but found ${trees.length} trees.")

      val model = new DecisionTreeBucketModel(metadata.uid, trees, numFeatures, numClasses)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  /** Convert a model from the old API */
  private[ml] def fromOld(
      oldModel: OldRandomForestModel,
      parent: DecisionTreeBucketizer,
      categoricalFeatures: Map[Int, Int],
      numClasses: Int,
      numFeatures: Int = -1): DecisionTreeBucketModel = {
    require(oldModel.algo == OldAlgo.Classification, "Cannot convert RandomForestModel" +
      s" with algo=${oldModel.algo} (old API) to DecisionTreeBucketModel (new API).")
    val newTrees = oldModel.trees.map { tree =>
      // parent for each tree is null since there is no good way to set this.
      DecisionTreeClassificationModel.fromOld(tree, null, categoricalFeatures)
    }
    val uid = if (parent != null) parent.uid else Identifiable.randomUID("dtb")
    new DecisionTreeBucketModel(uid, newTrees, numFeatures, numClasses)
  }
}
