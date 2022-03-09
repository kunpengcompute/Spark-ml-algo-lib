package com.bigdata.ml

import java.io.{File, FileWriter, PrintWriter}
import java.util.HashMap

import com.bigdata.utils.Utils
import org.apache.spark.ml.feature.{PCA => MLPCA}
import org.apache.spark.ml.linalg.{Vectors => MLVectors}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.feature.{PCA => MLibPCA}
import org.apache.spark.mllib.linalg.{Vectors => MLibVectors}
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions.udf
import org.apache.spark.storage.StorageLevel
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import scala.beans.BeanProperty

class PCAConfig extends Serializable {

  @BeanProperty var pca: HashMap[String, Object] = _
}

class PCAParams extends Serializable {

  @BeanProperty var numPartitions: Int = _
  @BeanProperty var k: Int = _
  @BeanProperty var numCols: Int = _
  @BeanProperty var numRows: Int = _
  @BeanProperty var dataFormat: String = _
  @BeanProperty var sep: String = _
  @BeanProperty var dataPath: String = _
  @BeanProperty var apiName: String = _
  @BeanProperty var dataStructure: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var datasetCpuName: String = _
  @BeanProperty var isRaw: String = "no"
  @BeanProperty var costTime: Double = _
  @BeanProperty var loadDataTime: Double = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}

class PCAKernel {

  def runDataFrameJob(spark: SparkSession, params: PCAParams): (Array[Double], Double) = {

    val sc = spark.sparkContext
    val numColsBC = sc.broadcast(params.numCols)
    val sepBC = sc.broadcast(params.sep)

    val startTime = System.currentTimeMillis()

    val data = if (params.dataFormat == "coo") {
      spark.createDataFrame(sc.textFile(params.dataPath, params.numPartitions)
        .map(line => {
          val entry = line.split(sepBC.value)
          (entry(0).toInt, (entry(1).toInt, entry(2).toDouble))
        }).groupByKey()
        .map { case (_, vectorEntries) => MLVectors.sparse(numColsBC.value, vectorEntries.toSeq) }
        .repartition(params.numPartitions)
        .map(Tuple1.apply))
        .toDF("matrix").persist(StorageLevel.MEMORY_ONLY)
    } else {
      spark.createDataFrame(sc.textFile(params.dataPath)
        .map(row => Row(MLVectors.dense(row.split(sepBC.value).map(_.toDouble))))
        .repartition(params.numPartitions),
        StructType(List(StructField("matrix", VectorType)))
      ).persist(StorageLevel.MEMORY_ONLY)
    }

    println("count: " + data.count())
    val loadDataTime = (System.currentTimeMillis() - startTime) / 1000.0

    val pca = new MLPCA().setK(params.k).setInputCol("matrix")

    val paramMaps: Array[ParamMap] = new Array[ParamMap](2)
    for (i <- 0 to paramMaps.size - 1) {
      paramMaps(i) = ParamMap(pca.k -> params.k)
        .put(pca.inputCol, "matrix")
    }

    val model = params.apiName match {
      case "fit" => pca.fit(data)
      case "fit1" =>
        val models = pca.fit(data, paramMaps)
        models(0)
    }
    val costTime = (System.currentTimeMillis() - startTime) / 1000.0
    params.setLoadDataTime(loadDataTime)
    val res = model.pc.values
    (res, costTime)
  }

  def runRDDJob(spark: SparkSession, params: PCAParams): (Array[Double], Double) = {

    val sc = spark.sparkContext
    val numColsBC = sc.broadcast(params.numCols)
    val sepBC = sc.broadcast(params.sep)

    val startTime = System.currentTimeMillis()

    val data = if (params.dataFormat == "coo") {
      sc.textFile(params.dataPath, params.numPartitions)
        .map(line => {
          val entry = line.split(sepBC.value)
          (entry(0).toInt, (entry(1).toInt, entry(2).toDouble))
        }).groupByKey()
        .map { case (_, vectorEntries) => MLibVectors.sparse(numColsBC.value, vectorEntries.toSeq) }
        .repartition(params.numPartitions).persist(StorageLevel.MEMORY_ONLY)
    } else {
      sc.textFile(params.dataPath)
        .map(row => MLibVectors.dense(row.split(sepBC.value).map(_.toDouble)))
        .repartition(params.numPartitions).persist(StorageLevel.MEMORY_ONLY)
    }

    println("count: " + data.count())
    val loadDataTime = (System.currentTimeMillis() - startTime) / 1000.0

    val pca = new MLibPCA(params.k).fit(data)

    val costTime = (System.currentTimeMillis() - startTime) / 1000.0
    params.setLoadDataTime(loadDataTime)
    val res = pca.pc.values
    (res, costTime)
  }
}

object PCARunner {

  def main(args: Array[String]): Unit = {

    try {
      val modelConfSplit = args(0).split("-")
      val (dataStructure, datasetName, apiName, cpuName) =
        (modelConfSplit(0), modelConfSplit(1), modelConfSplit(2), modelConfSplit(3))

      val dataPath = args(1)

      val datasetCpuName = s"${datasetName}_${cpuName}"
      val stream = Utils.getStream("conf/ml/pca/pca.yml")

      val representer = new Representer
      representer.addClassTag(classOf[PCAParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[PCAConfig]), representer, options)
      val description = new TypeDescription(classOf[PCAParams])
      yaml.addTypeDescription(description)
      val config: PCAConfig = yaml.load(stream).asInstanceOf[PCAConfig]
      val paramsMap = config.pca.get(datasetCpuName).asInstanceOf[HashMap[String, Object]]

      val params = new PCAParams()

      params.setApiName(apiName)
      params.setDataPath(dataPath)
      params.setDatasetName(datasetName)
      params.setDatasetCpuName(datasetCpuName)
      params.setDataStructure(dataStructure)
      params.setAlgorithmName("PCA")
      params.setTestcaseType(s"PCA_${dataStructure}_${datasetName}_${apiName}")
      if (cpuName == "raw") {
        params.setIsRaw("yes")
      }


      params.setNumPartitions(paramsMap.get("numPartitions").toString.toInt)
      params.setK(paramsMap.get("k").toString.toInt)
      params.setNumCols(paramsMap.get("numCols").toString.toInt)
      params.setNumRows(paramsMap.get("numRows").toString.toInt)
      params.setDataFormat(paramsMap.get("dataFormat").toString)
      params.setSep(paramsMap.get("sep").toString)

      var appName = s"PCA_${dataStructure}_${datasetName}_${apiName}"
      if (cpuName.equals("raw")){
        appName = s"PCA_RAW_${dataStructure}_${datasetName}_${apiName}"
      }
      params.setTestcaseType(appName)

      val conf = new SparkConf()
        .setAppName(appName)
      val spark = SparkSession.builder.config(conf).getOrCreate()

      val (res, costTime) = dataStructure match {
        case "dataframe" => new PCAKernel().runDataFrameJob(spark, params)
        case "rdd" => new PCAKernel().runRDDJob(spark, params)
      }
      params.setCostTime(costTime)
      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(s"report/PCA_${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")
      yaml.dump(params, writer)
      val resOutput = new File(s"report/PCAResult_${dataStructure}_${datasetName}_${apiName}_${cpuName}.txt")
      val pw = new PrintWriter(resOutput, "UTF-8")
      res.foreach(pw.println)
      pw.close()
      println(s"Exec Successful: costTime: ${costTime}s")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}
