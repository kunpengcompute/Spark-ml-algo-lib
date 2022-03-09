package org.apache.spark.ml.clustering

import com.bigdata.utils.Utils
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.{VectorUDT, Vectors}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import java.io.{File, FileWriter}
import java.util
import scala.beans.BeanProperty

class DBSCANConfig extends Serializable {

  @BeanProperty var dbscan: util.HashMap[String, Object] = _
}

class DBSParams extends Serializable {
  @BeanProperty var numPartitions: Int = _
  @BeanProperty var epsilon: Double = _
  @BeanProperty var minPoints: Int = _
  @BeanProperty var sampleRate: Double = _
  @BeanProperty var dataPath: String = _
  @BeanProperty var datasetName: String = _
  @BeanProperty var datasetCpuName: String = _
  @BeanProperty var isRaw: String = "no"
  @BeanProperty var costTime: Double = _
  @BeanProperty var numTotal: util.HashMap[String, Int] = _
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
}

class DBSCANKernel {
  def runJob(spark: SparkSession, params: DBSParams): (util.HashMap[String, Int],Double) = {
    val startTime = System.currentTimeMillis()

    val sc = spark.sparkContext

    val inputSchema = StructType(
      Seq(
        StructField("features", new VectorUDT, false)
      )
    )
    val dataRow = sc.textFile(params.dataPath).map(x => Row(Vectors.dense(x.split(" ").map(_.toDouble))))
    val data = spark.createDataFrame(dataRow, inputSchema)
      .repartition(params.numPartitions).cache()

    val model = new DBSCAN()
      .setEpsilon(params.epsilon)
      .setMinPoints(params.minPoints)
      .setSampleRate(params.sampleRate)

    val globalClustersDF = model.fitPredict(data)
    globalClustersDF.foreachPartition(_ => {})

    globalClustersDF.cache()
    val pointTypeArr = globalClustersDF.select("prediction").collect().map(_(0)).map(x => x.toString.toInt)
    val clusterIDArr = globalClustersDF.select("label").collect().map(_(0)).map(x => x.toString.toInt)
    val costTime = (System.currentTimeMillis() - startTime) / 1000.0
    val numClusters: Int = clusterIDArr.toSet.size
    val numCore: Int = pointTypeArr.count(_ == 0)
    val numBorder: Int = pointTypeArr.count(_ == 1)
    val numNoise: Int = pointTypeArr.count(_ == -1)
    val numTotal = new util.HashMap[String, Int]()
    numTotal.put(s"numClusters",numClusters)
    numTotal.put(s"numCore",numCore)
    numTotal.put(s"numBorder",numBorder)
    numTotal.put(s"numNoise",numNoise)
    (numTotal,costTime)
  }
}

object DBSCANRunner {

  def main(args: Array[String]): Unit = {

    try {
      val modelConfSplit = args(0).split("-")
      val (datasetName, platformName) = (modelConfSplit(0), modelConfSplit(1))

      val dataPath = args(1)

      val datasetCpuName = s"${datasetName}_${platformName}"
      val stream = Utils.getStream("conf/ml/dbscan/dbscan.yml")

      val representer = new Representer
      representer.addClassTag(classOf[DBSParams], Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[DBSCANConfig]), representer, options)
      val description = new TypeDescription(classOf[DBSParams])
      yaml.addTypeDescription(description)

      val config: DBSCANConfig = yaml.load(stream).asInstanceOf[DBSCANConfig]
      val paramsMap = config.dbscan.get(datasetName).asInstanceOf[util.HashMap[String, Object]]

      val params = new DBSParams()

      params.setDataPath(dataPath)
      params.setDatasetName(datasetName)
      params.setDatasetCpuName(datasetCpuName)
      params.setAlgorithmName("DBSCAN")
      params.setTestcaseType(s"DBSCAN_${datasetName}")
      params.setEpsilon(paramsMap.get("epsilon").toString.toDouble)
      params.setMinPoints(paramsMap.get("minPoints").toString.toInt)
      params.setSampleRate(paramsMap.get("sampleRate").toString.toDouble)

      params.setNumPartitions(paramsMap.get("numPartitions").toString.toInt)


      val conf = new SparkConf().setAppName(s"DBSCAN_${datasetName}_${platformName}")

      val spark = SparkSession.builder.config(conf).getOrCreate()

      val dbscan = new DBSCANKernel()
      val tuple = dbscan.runJob(spark, params)
      params.setNumTotal(tuple._1)
      params.setCostTime(tuple._2)
      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(s"report/DBSCAN${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")
      yaml.dump(params, writer)
      println(s"Exec Successful: costTime: ${params.getCostTime}s")
    } catch {
      case e: Throwable =>
        println(s"Exec Failure: ${e.getMessage}")
        throw e
    }
  }
}



