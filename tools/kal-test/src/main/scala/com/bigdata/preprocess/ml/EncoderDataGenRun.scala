package com.bigdata.preprocess.ml

import org.json4s.jackson.Serialization
import org.json4s.DefaultFormats
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.util.RandomUUIDGenerator
import org.apache.spark.sql.functions.{col, concat_ws, udf}

import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class DataAndMapGen {

  var mapPath = ""
  var dataPath = ""

  def setMapPath(mapPath: String): this.type = {
    this.mapPath = mapPath
    this
  }

  def setDataPath(dataPath: String): this.type = {
    this.dataPath = dataPath
    this
  }

  var featureMap:mutable.Map[String,Int] = mutable.Map.empty

  def dataGenFunc(numSamples: Int, spark: SparkSession): Dataset[Row] = {
    val gen = RandomUUIDGenerator(0L)
    println("Creating dataset")
    val rdd = spark.sparkContext.parallelize(0 until numSamples)
      .map(_ => {
        var row = mutable.ArrayBuffer[String]()
        val uid = gen.getNextUUIDUTF8String().toString.replace("-", "")
        for (strLen <- Array(2, 4, 4)) {
          for (listLen <- Array(1, 1, 1, 1, 1)) {
            var offset = 0
            val list = mutable.ArrayBuffer[String]()
            for (_ <- 0 until listLen) {
              list.append(uid.slice(offset, offset + strLen))
              offset += 1
            }
            row.append(list.mkString("^"))
          }
        }

        (row(0), row(1), row(2), row(3), row(4), row(5),  row(6), row(7), row(8), row(9),
          row(10), row(11), row(12), row(13), row(14))
      })

    rdd.saveAsTextFile(dataPath)
    println("save data done")

    import spark.implicits._
    val df = spark.sparkContext.textFile(dataPath).map{
      t =>
        val row = t.split(",")
        (row(0), row(1), row(2), row(3), row(4), row(5), row(6), row(7), row(8), row(9),
          row(10), row(11), row(12), row(13), row(14))
    }.toDF("1xxx","2xxx","3xxx","4xxx","5xxx","6xxx","7xxx","8xxx","9xxx","10xxx","11xxx","12xxx","13xxx","14xxx","15xxx")
      .repartition(800)
      .cache()

    df.show()
    df
  }

  def updateFeatureMap(input: DataFrame, key:String, value:String): Unit={
    val kvMap = input
      .select(key,value)
      .rdd
      .map(x=>(x.get(0).toString,x.get(1).toString.toDouble.toInt+1))
      .collectAsMap()
    featureMap ++= kvMap
  }

  def saveMutableMap(mapFile:Any,savePath:String):Unit={
    implicit val formats: DefaultFormats = org.json4s.DefaultFormats
    val map = mapFile.asInstanceOf[mutable.Map[String,Any]]
    val data = Serialization.write(map)

    val outputFileName = savePath
    val outputDir = outputFileName.split("\\/").dropRight(1).mkString("/")

    println(s"file will be save at ${outputFileName}")
    val file = new File(outputDir)
    if (!file.exists()) {
      //如果文件夹不存在
      val mkdirsFlag = file.mkdirs() //创建文件夹
      if (mkdirsFlag == true) {
        println(s"create dir successed:${outputDir}")
      }
    }
    var writer: BufferedWriter = null
    try {
      val fileName = new File(outputFileName)
      if (fileName.exists()){
        val deleteFlg = fileName.delete()
        if (deleteFlg)
          println("delete file successed")
      }
      writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName, true), "UTF-8"))
      writer.write(data)
    } finally {
      if (writer != null) writer.close()
    }
  }

  def mapGenFunc(spark: SparkSession, input: DataFrame, inputCols: String*): Unit = {
    val crossUDF = udf((featOne:String,featTwo:String)=>{
      val featureArrayBuf = ArrayBuffer[String]()
      for (i <- featOne.split("\\^")){
        for (j <- featTwo.split("\\^")){
          featureArrayBuf.append(i+Const.ACTUAL_SEP_CHAR+j)
        }
      }
      featureArrayBuf.mkString("^")
    })
    for (index <- 0 until inputCols.length) {
      val cols = inputCols(index)
      println(s"===================now is ${cols},completed ${index}/${inputCols.length - 1}")
      val columnName = cols.replace(Const.CONFIG_CROSS_SEP_CHAR, Const.ACTUAL_SEP_CHAR)
      var featureMapDF = if (cols.indexOf(Const.CONFIG_CROSS_SEP_CHAR) != (-1)) {
        if (cols.split(Const.CONFIG_CROSS_SEP_CHAR).size > 2) {
          input.withColumn(columnName, concat_ws(Const.ACTUAL_SEP_CHAR, cols.split(Const.CONFIG_CROSS_SEP_CHAR).map(x => col(x.trim())): _*))
            .select(columnName)
        } else {
          input.withColumn(columnName, crossUDF(cols.split(Const.CONFIG_CROSS_SEP_CHAR).map(x => col(x.trim())): _*))
        }
      } else input.select(columnName)

      featureMapDF.na.drop(Array(columnName)).createOrReplaceTempView("featureMap")
      featureMapDF = spark.sql(s"select ${columnName},row_number() over(order by cnt desc) as rank from (" +
        s"select ${columnName},count(1) as cnt from featureMap group by ${columnName} having count(1)>0)t")
      //      featureMapDF = spark.sql(s"select ${columnName} from featureMap where length(${columnName})>0 group by ${columnName} having count(1)> ${minCount}")
      import spark.implicits._
      //      val featureMapRdd = featureMapDF.rdd.map(x => s"${columnName},"+x.get(0).toString).zipWithIndex().map(x => (x._1,x._2+2))
      val featureMapRdd = featureMapDF.rdd.map(x => (s"${columnName}," + x.get(0).toString, x.get(1).toString.toInt + 1))

      featureMapDF = featureMapRdd.toDF("key", "value")
      //append feature index map
      updateFeatureMap(featureMapDF, "key", "value")
    }
    saveMutableMap(featureMap, mapPath)
  }
}

object Const {
  val ACTUAL_SEP_CHAR = "__"
  val CONFIG_CROSS_SEP_CHAR = "#"
  val DENSE_MAP_SEQ_CHAR = "\\*"
  val DENSE_MAP_SUFFIX = "_c"
  private var MASK_RATE = 0.0
}

object EncoderDataGenRun {
  def main(args: Array[String]): Unit = {
    val isLocal = false
    val sparkMaster = if (isLocal) "local[*]" else "yarn"
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("DateGen")
      .master(sparkMaster)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    var mapPath = ""
    var dataPath = ""
    var numSamples = 400000000

    args.sliding(2, 2).foreach {
      case Array("--mapPath", value) => mapPath = value
      case Array("--dataPath", value) => dataPath = value
      case Array("--numSamples", value) => numSamples = value.toInt
      case Array(name, value) => System.err.println(s"[ERROR] unknown argument, name:$name, value:$value")
    }

    val dg = new DataAndMapGen().setDataPath(dataPath).setMapPath(mapPath)
    val df = dg.dataGenFunc(numSamples, spark)
    dg.mapGenFunc(spark, df, df.columns:_*)
  }
}
