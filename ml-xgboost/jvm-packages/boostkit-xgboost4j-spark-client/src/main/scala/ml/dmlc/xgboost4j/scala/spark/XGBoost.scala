// scalastyle:off header.matches
/*
* Copyright (C) 2021. Huawei Technologies Co., Ltd.
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
* */
/*
 Copyright (c) 2014 by Contributors

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package ml.dmlc.xgboost4j.scala.spark

import ml.dmlc.xgboost4j.scala.{EvalTrait, ExternalCheckpointParams, ObjectiveTrait}
import ml.dmlc.xgboost4j.scala.spark.params.LearningTaskParams
import org.apache.commons.logging.LogFactory
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


/**
 * Rabit tracker configurations.
 *
 * @param workerConnectionTimeout The timeout for all workers to connect to the tracker.
 *                                Set timeout length to zero to disable timeout.
 *                                Use a finite, non-zero timeout value to prevent tracker from
 *                                hanging indefinitely (in milliseconds)
 *                                (supported by "scala" implementation only.)
 * @param trackerImpl Choice between "python" or "scala". The former utilizes the Java wrapper of
 *                    the Python Rabit tracker (in dmlc_core), whereas the latter is implemented
 *                    in Scala without Python components, and with full support of timeouts.
 *                    The Scala implementation is currently experimental, use at your own risk.
 */
case class TrackerConf(workerConnectionTimeout: Long, trackerImpl: String)

object TrackerConf {
  def apply(): TrackerConf = TrackerConf(0L, "python")
}

private[this] case class XGBoostExecutionEarlyStoppingParams(numEarlyStoppingRounds: Int,
                                                             maximizeEvalMetrics: Boolean)

private[this] case class XGBoostExecutionInputParams(trainTestRatio: Double, seed: Long)

private[spark] case class XGBoostExecutionParams(
    numWorkers: Int,
    var numRounds: Int,
    useExternalMemory: Boolean,
    obj: ObjectiveTrait,
    eval: EvalTrait,
    missing: Float,
    allowNonZeroForMissing: Boolean,
    trackerConf: TrackerConf,
    timeoutRequestWorkers: Long,
    checkpointParam: Option[ExternalCheckpointParams],
    xgbInputParams: XGBoostExecutionInputParams,
    earlyStoppingParams: XGBoostExecutionEarlyStoppingParams,
    cacheTrainingSet: Boolean) {

  private var rawParamMap: Map[String, Any] = _

  def setRawParamMap(inputMap: Map[String, Any]): Unit = {
    rawParamMap = inputMap
  }

  def toMap: Map[String, Any] = {
    rawParamMap
  }
}

private[this] class XGBoostExecutionParamsFactory(rawParams: Map[String, Any], sc: SparkContext){

  private val logger = LogFactory.getLog("XGBoostSpark")

  private val overridedParams = overrideParams(rawParams, sc)

  /**
   * Check to see if Spark expects SSL encryption (`spark.ssl.enabled` set to true).
   * If so, throw an exception unless this safety measure has been explicitly overridden
   * via conf `xgboost.spark.ignoreSsl`.
   */
  private def validateSparkSslConf: Unit = {
    val (sparkSslEnabled: Boolean, xgboostSparkIgnoreSsl: Boolean) =
      SparkSession.getActiveSession match {
        case Some(ss) =>
          (ss.conf.getOption("spark.ssl.enabled").getOrElse("false").toBoolean,
            ss.conf.getOption("xgboost.spark.ignoreSsl").getOrElse("false").toBoolean)
        case None =>
          (sc.getConf.getBoolean("spark.ssl.enabled", false),
            sc.getConf.getBoolean("xgboost.spark.ignoreSsl", false))
      }
    if (sparkSslEnabled) {
      if (xgboostSparkIgnoreSsl) {
        logger.warn(s"spark-xgboost is being run without encrypting data in transit!  " +
          s"Spark Conf spark.ssl.enabled=true was overridden with xgboost.spark.ignoreSsl=true.")
      } else {
        throw new Exception("xgboost-spark found spark.ssl.enabled=true to encrypt data " +
          "in transit, but xgboost-spark sends non-encrypted data over the wire for efficiency. " +
          "To override this protection and still use xgboost-spark at your own risk, " +
          "you can set the SparkSession conf to use xgboost.spark.ignoreSsl=true.")
      }
    }
  }

  /**
   * we should not include any nested structure in the output of this function as the map is
   * eventually to be feed to xgboost4j layer
   */
  private def overrideParams(
      params: Map[String, Any],
      sc: SparkContext): Map[String, Any] = {
    val coresPerTask = sc.getConf.getInt("spark.task.cpus", 1)
    var overridedParams = params
    if (overridedParams.contains("nthread")) {
      val nThread = overridedParams("nthread").toString.toInt
      require(nThread <= coresPerTask,
        s"the nthread configuration ($nThread) must be no larger than " +
          s"spark.task.cpus ($coresPerTask)")
    } else {
      overridedParams = overridedParams + ("nthread" -> coresPerTask)
    }

    val numEarlyStoppingRounds = overridedParams.getOrElse(
      "num_early_stopping_rounds", 0).asInstanceOf[Int]
    overridedParams += "num_early_stopping_rounds" -> numEarlyStoppingRounds
    if (numEarlyStoppingRounds > 0 &&
      !overridedParams.contains("maximize_evaluation_metrics")) {
      if (overridedParams.contains("custom_eval")) {
        throw new IllegalArgumentException("custom_eval does not support early stopping")
      }
      val eval_metric = overridedParams("eval_metric").toString
      val maximize = LearningTaskParams.evalMetricsToMaximize contains eval_metric
      logger.info("parameter \"maximize_evaluation_metrics\" is set to " + maximize)
      overridedParams += ("maximize_evaluation_metrics" -> maximize)
    }
    overridedParams
  }

  def buildXGBRuntimeParams: XGBoostExecutionParams = {
    val nWorkers = overridedParams("num_workers").asInstanceOf[Int]
    val round = overridedParams("num_round").asInstanceOf[Int]
    val useExternalMemory = overridedParams("use_external_memory").asInstanceOf[Boolean]
    val obj = overridedParams.getOrElse("custom_obj", null).asInstanceOf[ObjectiveTrait]
    val eval = overridedParams.getOrElse("custom_eval", null).asInstanceOf[EvalTrait]
    val missing = overridedParams.getOrElse("missing", Float.NaN).asInstanceOf[Float]
    val allowNonZeroForMissing = overridedParams
      .getOrElse("allow_non_zero_for_missing", false)
      .asInstanceOf[Boolean]
    validateSparkSslConf
    if (overridedParams.contains("tree_method")) {
      require(overridedParams("tree_method") == "hist" ||
        overridedParams("tree_method") == "approx" ||
        overridedParams("tree_method") == "auto", "xgboost4j-spark only supports tree_method as" +
        " 'hist', 'approx' and 'auto'")
    }
    if (overridedParams.contains("train_test_ratio")) {
      logger.warn("train_test_ratio is deprecated since XGBoost 0.82, we recommend to explicitly" +
        " pass a training and multiple evaluation datasets by passing 'eval_sets' and " +
        "'eval_set_names'")
    }
    require(nWorkers > 0, "you must specify more than 0 workers")
    if (obj != null) {
      require(overridedParams.get("objective_type").isDefined, "parameter \"objective_type\" " +
        "is not defined, you have to specify the objective type as classification or regression" +
        " with a customized objective function")
    }
    val trackerConf = overridedParams.get("tracker_conf") match {
      case None => TrackerConf()
      case Some(conf: TrackerConf) => conf
      case _ => throw new IllegalArgumentException("parameter \"tracker_conf\" must be an " +
        "instance of TrackerConf.")
    }
    val timeoutRequestWorkers: Long = overridedParams.get("timeout_request_workers") match {
      case None => 0L
      case Some(interval: Long) => interval
      case _ => throw new IllegalArgumentException("parameter \"timeout_request_workers\" must be" +
        " an instance of Long.")
    }
    val checkpointParam =
      ExternalCheckpointParams.extractParams(overridedParams)

    val trainTestRatio = overridedParams.getOrElse("train_test_ratio", 1.0)
      .asInstanceOf[Double]
    val seed = overridedParams.getOrElse("seed", System.nanoTime()).asInstanceOf[Long]
    val inputParams = XGBoostExecutionInputParams(trainTestRatio, seed)

    val earlyStoppingRounds = overridedParams.getOrElse(
      "num_early_stopping_rounds", 0).asInstanceOf[Int]
    val maximizeEvalMetrics = overridedParams.getOrElse(
      "maximize_evaluation_metrics", true).asInstanceOf[Boolean]
    val xgbExecEarlyStoppingParams = XGBoostExecutionEarlyStoppingParams(earlyStoppingRounds,
      maximizeEvalMetrics)

    val cacheTrainingSet = overridedParams.getOrElse("cache_training_set", false)
      .asInstanceOf[Boolean]

    val xgbExecParam = XGBoostExecutionParams(nWorkers, round, useExternalMemory, obj, eval,
      missing, allowNonZeroForMissing, trackerConf,
      timeoutRequestWorkers,
      checkpointParam,
      inputParams,
      xgbExecEarlyStoppingParams,
      cacheTrainingSet)
    xgbExecParam.setRawParamMap(overridedParams)
    xgbExecParam
  }

  private[spark] def buildRabitParams : scala.collection.mutable.Map[String, String] =
    scala.collection.mutable.Map(
      "rabit_reduce_ring_mincount" ->
        overridedParams.getOrElse("rabit_ring_reduce_threshold", 32 << 10).toString,
      "rabit_debug" ->
        (overridedParams.getOrElse("verbosity", 0).toString.toInt == 3).toString,
      "rabit_timeout" ->
        (overridedParams.getOrElse("rabit_timeout", -1).toString.toInt >= 0).toString,
      "rabit_timeout_sec" -> {
        if (overridedParams.getOrElse("rabit_timeout", -1).toString.toInt >= 0) {
          overridedParams.get("rabit_timeout").toString
        } else {
          "1800"
        }
      },
      "rabit_enable_tcp_no_delay" ->
        overridedParams.getOrElse("rabit_enable_tcp_no_delay", false).toString,
      "DMLC_WORKER_CONNECT_RETRY" ->
        overridedParams.getOrElse("dmlc_worker_connect_retry", 5).toString
    )
}
