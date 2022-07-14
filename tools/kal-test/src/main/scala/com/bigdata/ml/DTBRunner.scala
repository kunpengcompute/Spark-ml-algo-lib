package com.bigdata.ml

import java.io.{File, FileWriter}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Pipeline
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import scala.beans.BeanProperty
import java.util
import java.util.Date
import com.bigdata.utils.{DTBucketUtils, Utils}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.feature.{DecisionTreeBucketModel, DecisionTreeBucketizer, StringIndexer}
import org.apache.spark.ml.linalg.{SparseVector, Vector => MLVector, Vectors => MLVectors}
import org.apache.spark.ml.param.{ParamMap, ParamPair}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.storage.StorageLevel

class DTBConfig extends Serializable {
  @BeanProperty var dtb: util.HashMap[String, util.HashMap[String, util.HashMap[String, Object]]] = _
}

class DTBParams extends Serializable {
  @BeanProperty var pt: Int = _
  @BeanProperty var maxDepth: Int = _
  @BeanProperty var maxBins: Int = _
  @BeanProperty var useNodeIdCache: Boolean = _
  @BeanProperty var checkpointInterval: Int = _
  @BeanProperty var numCopiesInput: Int = _
  @BeanProperty var maxMemoryInMB: Int = _
  @BeanProperty var genericPt: Int = _
  @BeanProperty var featuresType: String = _
  @BeanProperty var bcVariables: Boolean = _
  @BeanProperty var saveBucketedRes: Boolean = _
  @BeanProperty var verifyBucketedRes: Boolean = _
  @BeanProperty var bucketedResPath: String = _

  @BeanProperty var trainingDataPath: String = _
  @BeanProperty var testDataPath: String = _
  @BeanProperty var apiName: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var cpuName: String = _
  @BeanProperty var isRaw: String = _
  @BeanProperty var verified: String = _
}

object DTBRunner {
  def main(args: Array[String]): Unit = {
    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, apiName, saveOrVerify, bucketedResPath) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2), modelConfSplit(3))

      val dataPath = args(1)
      val dataPathSplit = dataPath.split(",")
      val (trainingDataPath, testDataPath) = (dataPathSplit(0), dataPathSplit(1))

      val cpuName = args(2)
      val isRaw = args(3)
      val sparkConfSplit = args(4).split("_")
      val (master, deployMode, numExec, execCores, execMem) =
        (sparkConfSplit(0), sparkConfSplit(1), sparkConfSplit(2), sparkConfSplit(3), sparkConfSplit(4))

      val stream = Utils.getStream("conf/ml/dtb/dtb.yml")
      val representer = new Representer
      representer.addClassTag(classOf[DTBParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[DTBConfig]), representer, options)
      val description = new TypeDescription(classOf[DTBParams])
      yaml.addTypeDescription(description)

      val configs: DTBConfig = yaml.load(stream).asInstanceOf[DTBConfig]
      val params = new DTBParams()

      val dtbParamMap: util.HashMap[String, Object] = configs.dtb.get(isRaw match {
        case "no" => "opt"
        case _ => "raw"
      }).get(datasetName)
      params.setGenericPt(dtbParamMap.getOrDefault("genericPt", "1000").asInstanceOf[Int])
      params.setMaxMemoryInMB(dtbParamMap.getOrDefault("maxMemoryInMB", "256").asInstanceOf[Int])
      params.setPt(dtbParamMap.getOrDefault("pt", "1000").asInstanceOf[Int])
      params.setNumCopiesInput(dtbParamMap.getOrDefault("numCopiesInput", "1").asInstanceOf[Int])
      params.setMaxDepth(dtbParamMap.getOrDefault("maxDepth", "5").asInstanceOf[Int])
      params.setMaxBins(dtbParamMap.getOrDefault("maxBins", "32").asInstanceOf[Int])
      params.setUseNodeIdCache(dtbParamMap.getOrDefault("useNodeIdCache", "false").asInstanceOf[Boolean])
      params.setCheckpointInterval(dtbParamMap.getOrDefault("checkpointInterval", "10").asInstanceOf[Int])
      params.setFeaturesType(dtbParamMap.getOrDefault("featuresType", "array").asInstanceOf[String])
      params.setBcVariables(dtbParamMap.getOrDefault("bcVariables", "false").asInstanceOf[Boolean])
      params.setTrainingDataPath(trainingDataPath)
      params.setTestDataPath(testDataPath)
      params.setApiName(apiName)
      params.setDatasetName(datasetName)
      params.setCpuName(cpuName)
      params.setIsRaw(isRaw)
      params.setBucketedResPath(bucketedResPath)
      params.setSaveBucketedRes(saveOrVerify.equals("save"))
      params.setVerifyBucketedRes(saveOrVerify.equals("verify"))

      val conf = new SparkConf().setMaster(master)
      val commonParas = Array (
        ("spark.submit.deployMode", deployMode),
        ("spark.executor.instances", numExec),
        ("spark.executor.cores", execCores),
        ("spark.executor.memory", execMem)
      )
      conf.setAll(commonParas)

      var appName = s"DTB_RAW_${datasetName}_${apiName}"
      if ("no" == isRaw.asInstanceOf[String]) {
        appName = s"DTB_${datasetName}_${apiName}"
        conf.set("spark.boostkit.ml.rf.binnedFeaturesDataType", params.featuresType)
        conf.set("spark.boostkit.ml.rf.numTrainingDataCopies", params.numCopiesInput.toString)
        conf.set("spark.boostkit.ml.rf.numPartsPerTrainingDataCopy", params.pt.toString)
        conf.set("spark.boostkit.ml.rf.broadcastVariables", params.bcVariables.toString)
        }
      conf.setAppName(apiName)
      val spark = SparkSession.builder.config(conf).getOrCreate()
      val sc = spark.sparkContext
      println(s"[KernelEx] initialized spark session. ${new Date().toString}")

      // 检查hdfs的分箱结果文件
      val fs = FileSystem.get(sc.hadoopConfiguration)
      val bucketedResFile = new Path(params.bucketedResPath)
      if (!params.saveBucketedRes && !params.verifyBucketedRes) {
        println("You can only choose Option save or Option verify")
        System.exit(-1)
      }
      if (params.saveBucketedRes && fs.exists(bucketedResFile)) {
        if (!fs.delete(bucketedResFile, true)) {
          println(s"Bucketed result file/path(${params.bucketedResPath}) can't be deleted!")
          System.exit(-1)
        } else {
          println(s"Bucketed result file/path(${params.bucketedResPath}) is deleted!")
        }
        assert(!fs.exists(bucketedResFile), s"Bucketed result file/path(${params.bucketedResPath}) is deleted!")
      }
      if (params.verifyBucketedRes && !fs.exists(bucketedResFile)) {
        println(s"Bucketed result verification is enabled. But reference bucketed result file/path(${params.bucketedResPath}) is not found!")
        System.exit(-1)
      }
      val numFeatures = datasetName.toLowerCase match {
        case "mnist8m" => 784
        case "higgs" => 28
      }
      val numPtTrainData = isRaw match {
        case "no" => params.genericPt
        case _ => params.pt
      }
      val startTime = System.currentTimeMillis()
      val reader = spark.read.format("libsvm")
      val trainingData = reader.load(trainingDataPath).repartition(numPtTrainData).persist(StorageLevel.MEMORY_AND_DISK_SER)

      val labelIndexer = new StringIndexer()
        .setInputCol("label")
        .setOutputCol("indexedLabel")
        .fit(trainingData)
      val fmtTrainingData = labelIndexer.transform(trainingData)

      val (bucketedTrainingData, numTrees, trainFinishTime) = isRaw match {
        case "no" =>
          val dtb = new DecisionTreeBucketizer()
            .setLabelCol("indexedLabel")
            .setFeaturesCol("features")
            .setMaxBins(params.maxBins)
            .setMaxDepth(params.maxDepth)
            .setCacheNodeIds(params.useNodeIdCache)
            .setCheckpointInterval(params.checkpointInterval)
            .setMaxMemoryInMB(params.maxMemoryInMB)
          val pipeline = new Pipeline().setStages(Array(dtb))

          val paramMap = ParamMap(dtb.maxBins -> params.maxBins)
            .put(dtb.maxDepth, params.maxDepth)
          val paramMaps: Array[ParamMap] = new Array[ParamMap](2)
          for (i <- 0 to paramMaps.size - 1) {
            paramMaps(i) = ParamMap(dtb.maxBins -> params.maxBins)
              .put(dtb.maxDepth, params.maxDepth)
          }
          val checkpointIntervalParamPair = ParamPair(dtb.checkpointInterval, params.checkpointInterval)
          val models = params.apiName match {
            case "fit1" => pipeline.fit(fmtTrainingData, paramMap)
            case "fit2" =>
              val models = pipeline.fit(fmtTrainingData, paramMaps)
              models(0)
            case "fit3" => pipeline.fit(fmtTrainingData, checkpointIntervalParamPair)
            case _ => pipeline.fit(fmtTrainingData)
          }
          val trainFinishTime = System.currentTimeMillis()
          val bucketedTrainingData = models.transform(fmtTrainingData).rdd.map(_.toString()).cache()
          bucketedTrainingData.foreachPartition(_ => {})
          val dtbModel = models.stages(0).asInstanceOf[DecisionTreeBucketModel]
          println(s"totalNumNodes = ${dtbModel.totalNumNodes}")
          (bucketedTrainingData, dtbModel.getNumTrees, trainFinishTime)
        case _ =>
          val dtcModels = Array.range(0, numFeatures).map { index =>
            val dtc = new DecisionTreeClassifier()
              .setLabelCol("indexedLabel")
              .setFeaturesCol("features")
              .setMaxBins(params.maxBins)
              .setMaxDepth(params.maxDepth)
              .setCacheNodeIds(params.useNodeIdCache)
              .setCheckpointInterval(params.checkpointInterval)
              .setMaxMemoryInMB(params.maxMemoryInMB)
            val pipeline = new Pipeline().setStages(Array(dtc))
            val curTrainingData = spark.createDataFrame(fmtTrainingData.rdd.map {row =>
              val featureVal = row.getAs[MLVector]("features")(index)
              (row.getAs[Double]("indexedLabel"), MLVectors.dense(Array(featureVal)))
            }).toDF("indexedLabel", "features")

            val paramMap = ParamMap(dtc.maxBins -> params.maxBins)
              .put(dtc.maxDepth, params.maxDepth)
            val paramMaps: Array[ParamMap] = new Array[ParamMap](2)
            for (i <- 0 to paramMaps.size - 1) {
              paramMaps(i) = ParamMap(dtc.maxBins -> params.maxBins)
                .put(dtc.maxDepth, params.maxDepth)
            }
            val checkpointIntervalParamPair = ParamPair(dtc.checkpointInterval, params.checkpointInterval)
            val models = params.apiName match {
              case "fit1" => pipeline.fit(curTrainingData, paramMap)
              case "fit2" =>
                val models = pipeline.fit(curTrainingData, paramMaps)
                models(0)
              case "fit3" => pipeline.fit(curTrainingData, checkpointIntervalParamPair)
              case _ => pipeline.fit(curTrainingData)
            }
            models.stages(0).asInstanceOf[DecisionTreeClassificationModel]
          }.map(org.apache.spark.mllib.tree.helper.toOldDTModel)
          val trainFinishTime = System.currentTimeMillis()
          val numTrees = dtcModels.length
          println(s"totalNumNodes = ${dtcModels.map(_.numNodes).sum}")
          val func2 = (x: MLVector) => {
            val bucketedValues = Range(0, numTrees).map {index =>
              val model = dtcModels(index)
              val treeLeafArray: Array[Int] = DTBucketUtils.getLeafNodes(model.topNode)
              val treePredict = DTBucketUtils.predictModify(model.topNode, new SparseVector(1, Array(0), Array(x(index))))
              val bucket_num = treeLeafArray.indexOf(treePredict)
              bucket_num.toDouble
            }.toArray
            MLVectors.dense(bucketedValues)
          }
          val udfDiscretizerUDF = udf(func2)
          val bucketedTrainingData = fmtTrainingData.withColumn(s"bucketedFeatures", udfDiscretizerUDF(col("features")))
            .rdd.map(_.toString()).cache()
          bucketedTrainingData.foreachPartition(_ => {})
          (bucketedTrainingData, numTrees, trainFinishTime)
      }
      val costTime = (System.currentTimeMillis() - startTime) / 1000
      params.setCostTime(costTime)
      bucketedTrainingData.take(2).foreach(println)
      var verifiedResult = if (params.verifyBucketedRes) "mismatch" else "unknown"
      if (params.verifyBucketedRes) {
        val output = bucketedTrainingData.repartition(params.genericPt)
        val refRes = spark.sparkContext.textFile(params.bucketedResPath).repartition(params.genericPt)
        val dataDiff1Cnt = output.subtract(refRes).count()
        val dataDiff2Cnt = refRes.subtract(output).count()
        if (dataDiff1Cnt != 0 || dataDiff2Cnt != 0) {
          System.err.println(s"[ERROR] diff1Cnt: ${dataDiff1Cnt}, diff2Cnt: ${dataDiff2Cnt}")
          System.err.println("output data is mismatch!")
          verifiedResult = "mismatch"
        } else {
          println("output data is verified!")
          verifiedResult = "verified"
        }
      }
      params.setVerified(verifiedResult)
      if (params.saveBucketedRes) {
        bucketedTrainingData.repartition(100).saveAsTextFile(params.bucketedResPath)
        println("bucketed result saved successful!")
      }

      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(s"report/DTB_${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")

      yaml.dump(params, writer)
      println(s"Exec Successful: costTime: ${costTime}s; verify: ${params.verified}")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
    }
  }
}
