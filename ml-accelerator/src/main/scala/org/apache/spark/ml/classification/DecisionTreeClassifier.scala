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

package org.apache.spark.ml.classification

import org.apache.spark.annotation.Since
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tree._
import org.apache.spark.ml.tree.impl.DecisionForest
import org.apache.spark.ml.util._
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, Strategy => OldStrategy}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset


/**
 * Decision tree learning algorithm (http://en.wikipedia.org/wiki/Decision_tree_learning)
 * for classification.
 * It supports both binary and multiclass labels, as well as both continuous and categorical
 * features.
 */
@Since("1.4.0")
class DecisionTreeClassifier @Since("1.4.0") (
    @Since("1.4.0") override val uid: String)
  extends ProbabilisticClassifier[Vector, DecisionTreeClassifier, DecisionTreeClassificationModel]
  with DecisionTreeClassifierParams with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("dtc"))

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

  override protected def train(dataset: Dataset[_]): DecisionTreeClassificationModel = {
    val categoricalFeatures: Map[Int, Int] =
      MetadataUtils.getCategoricalFeatures(dataset.schema($(featuresCol)))
    val numClasses: Int = getNumClasses(dataset)

    if (isDefined(thresholds)) {
      require($(thresholds).length == numClasses, this.getClass.getSimpleName +
        ".train() called with non-matching numClasses and thresholds.length." +
        s" numClasses=$numClasses, but thresholds has length ${$(thresholds).length}")
    }

    val oldDataset: RDD[LabeledPoint] = extractLabeledPoints(dataset, numClasses)
    val strategy = getOldStrategy(categoricalFeatures, numClasses)

    val instr = Instrumentation.create(this, oldDataset)
    instr.logParams(params: _*)

    val trees = DecisionForest.run(oldDataset, strategy, numTrees = 1,
      featureSubsetStrategy = "all",
      seed = $(seed), instr = Some(instr), parentUID = Some(uid))

    val m = trees.head.asInstanceOf[DecisionTreeClassificationModel]
    instr.logSuccess(m)
    m
  }

  /** (private[ml]) Train a decision tree on an RDD */
  private[ml] def train(data: RDD[LabeledPoint],
      oldStrategy: OldStrategy): DecisionTreeClassificationModel = {
    val instr = Instrumentation.create(this, data)
    instr.logParams(params: _*)

    val trees = DecisionForest.run(data, oldStrategy, numTrees = 1,
      featureSubsetStrategy = "all",
      seed = 0L, instr = Some(instr), parentUID = Some(uid))

    val m = trees.head.asInstanceOf[DecisionTreeClassificationModel]
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
  override def copy(extra: ParamMap): DecisionTreeClassifier = defaultCopy(extra)
}

@Since("1.4.0")
object DecisionTreeClassifier extends DefaultParamsReadable[DecisionTreeClassifier] {
  /** Accessor for supported impurities: entropy, gini */
  @Since("1.4.0")
  final val supportedImpurities: Array[String] = TreeClassifierParams.supportedImpurities

  @Since("2.0.0")
  override def load(path: String): DecisionTreeClassifier = super.load(path)
}
