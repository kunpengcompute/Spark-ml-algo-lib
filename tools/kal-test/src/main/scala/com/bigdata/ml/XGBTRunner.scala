package com.bigdata.ml

import com.bigdata.utils.Utils
import ml.dmlc.xgboost4j.scala.spark.{XGBoostClassifier, XGBoostRegressor}
import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import org.yaml.snakeyaml.{DumperOptions,TypeDescription,Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import java.io.{File, FileWriter, PrintWriter, StringWriter}
import scala.beans.BeanProperty
import java.util

class XGBTConfig extends Serializable{
  @BeanProperty var xgbt: util.HashMap[String,util.HashMap[String,util.HashMap[String,util.HashMap[String,Object]]]]=_
}
class XGBTParams extends Serializable{
  @BeanProperty var algorithmName: String = _
  @BeanProperty var testcaseType: String = _
  @BeanProperty var mode: String = _
  @BeanProperty var eta: Double = _
  @BeanProperty var gamma: Double = _
  @BeanProperty var min_child_weight: Int = _
  @BeanProperty var max_depth: Int = _
  @BeanProperty var verbosity: Int = _
  @BeanProperty var allow_non_zero_for_missing: Boolean = _
  @BeanProperty var vectorType: String = _
  @BeanProperty var enable_bbgen: Boolean = _
  @BeanProperty var rabit_enable_tcp_no_delay: Boolean = _
  @BeanProperty var tree_method: String = _
  @BeanProperty var grow_policy: String = _
  @BeanProperty var tr_fname: String = _
  @BeanProperty var ts_fname: String = _
  @BeanProperty var objective: String = _
  @BeanProperty var num_round: Int = _
  @BeanProperty var num_workers: Int = _
  @BeanProperty var num_class: Int = _
  @BeanProperty var nthread: Int = _
  @BeanProperty var evaluation: Double = _
  @BeanProperty var costTime: Double = _
  @BeanProperty var isRaw: String = _

}
object XGBTRunner {
  def main(args: Array[String]): Unit = {
    try{
      val modelConf = args(0)
      val modelConfSplit = modelConf.split("_")
      val (algType,datasetName)=(modelConfSplit(0),modelConfSplit(1))
      val dataPath = args(1)
      val dataPathSplit = dataPath.split(",")
      val (trainDataPath,testDataPath)=(dataPathSplit(0),dataPathSplit(1))
      val cpuName = args(2)
      val isRaw = args(3)
      val sparkConfSplit = args(4).split("_")
      val (master,deployMode)=(sparkConfSplit(0),sparkConfSplit(1))

      val OptOrPri = isRaw match{
        case "yes" => "primary"
        case "no" => "optimized"
      }
      val stream = cpuName match{
        case "x86_64" => Utils.getStream("conf/ml/xgbt/xgbt_x86.yml")
        case "aarch64" => Utils.getStream("conf/ml/xgbt/xgbt_arm.yml")
      }

      val representer =new Representer
      representer.addClassTag(classOf[XGBTParams],Tag.MAP)
      val options = new DumperOptions
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
      val yaml = new Yaml(new Constructor(classOf[XGBTConfig]),representer,options)
      val description = new TypeDescription(classOf[XGBTParams])
      yaml.addTypeDescription(description)
      val configs: XGBTConfig = yaml.load(stream).asInstanceOf[XGBTConfig]
      val params = new XGBTParams()
      val xgbtParamMap: util.HashMap[String,Object] = cpuName match{
        case "x86_64" => configs.xgbt.get(OptOrPri).get(algType).get(datasetName)
        case "aarch64" => configs.xgbt.get(OptOrPri).get(algType).get(datasetName)
      }

      params.setEta(xgbtParamMap.get("eta").asInstanceOf[Double])
      params.setGamma(xgbtParamMap.get("gamma").asInstanceOf[Int])
      params.setMin_child_weight(xgbtParamMap.get("min_child_weight").asInstanceOf[Int])
      params.setMax_depth(xgbtParamMap.get("max_depth").asInstanceOf[Int])
      params.setAllow_non_zero_for_missing(xgbtParamMap.get("allow_non_zero_for_missing").asInstanceOf[Boolean])
      params.setVectorType(xgbtParamMap.get("vectorType").asInstanceOf[String])
      params.setObjective(xgbtParamMap.get("objective").asInstanceOf[String])
      params.setNum_round(xgbtParamMap.get("num_round").asInstanceOf[Int])
      params.setNum_workers(xgbtParamMap.get("num_workers").asInstanceOf[Int])
      params.setNthread(xgbtParamMap.get("nthread").asInstanceOf[Int])
      params.setTree_method(xgbtParamMap.get("tree_method").asInstanceOf[String])
      params.setGrow_policy(xgbtParamMap.get("grow_policy").asInstanceOf[String])
      if(modelConf=="classification_mnist8m"){
        params.setNum_class(xgbtParamMap.get("num_class").asInstanceOf[Int])
      }
      if(isRaw=="no"){
        params.setEnable_bbgen(xgbtParamMap.get("enable_bbgen").asInstanceOf[Boolean])
        params.setRabit_enable_tcp_no_delay(xgbtParamMap.get("rabit_enable_tcp_no_delay").asInstanceOf[Boolean])
      }
      params.setTr_fname(trainDataPath)
      params.setTs_fname(testDataPath)

      params.setIsRaw(isRaw)

      val appName = isRaw match {
        case "yes" => s"XGBT_${datasetName}_${algType}_openSource"
        case "no" => s"XGBT_${datasetName}_${algType}_${cpuName}"
      }

      val conf = new SparkConf().setAppName(appName).setMaster(master)
      val commonParas =Array(
        ("spark.submit.deployMode",deployMode)
      )
      conf.setAll(commonParas)
      val spark= SparkSession.builder().config(conf).getOrCreate()
      var paramsAnyMap: scala.Predef.Map[String,Any]=Map[String,Any]()
      paramsAnyMap += ("eta" -> xgbtParamMap.get("eta"))
      paramsAnyMap += ("gamma" -> xgbtParamMap.get("gamma"))
      paramsAnyMap += ("min_child_weight" -> xgbtParamMap.get("min_child_weight"))
      paramsAnyMap += ("max_depth" -> xgbtParamMap.get("max_depth"))
      paramsAnyMap += ("allow_non_zero_for_missing" -> xgbtParamMap.get("allow_non_zero_for_missing"))
      paramsAnyMap += ("vectorType" -> xgbtParamMap.get("vectorType"))
      paramsAnyMap += ("objective" -> xgbtParamMap.get("objective"))
      paramsAnyMap += ("num_round" -> xgbtParamMap.get("num_round"))
      paramsAnyMap += ("num_workers" -> xgbtParamMap.get("num_workers"))
      paramsAnyMap += ("nthread" -> xgbtParamMap.get("nthread"))
      paramsAnyMap += ("tree_method" -> xgbtParamMap.get("tree_method"))
      paramsAnyMap += ("grow_policy" -> xgbtParamMap.get("grow_policy"))
      if(modelConf=="classification_mnist8m") {
        paramsAnyMap += ("num_class" -> xgbtParamMap.get("num_class"))
      }
      if (isRaw=="no"){
        paramsAnyMap += ("enable_bbgen" -> xgbtParamMap.get("enable_bbgen"))
        paramsAnyMap += ("rabit_enable_tcp_no_delay" -> xgbtParamMap.get("rabit_enable_tcp_no_delay"))
      }
      val start_time = System.currentTimeMillis()
      val XGBTrain = algType match {
        case "classification" =>
          new XGBoostClassifier(paramsAnyMap).setLabelCol("label").setFeaturesCol("features")
        case "regression" =>
          new XGBoostRegressor(paramsAnyMap).setLabelCol("label").setFeaturesCol("features")
      }

      val train_data = getTrainData(spark,params).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val model = XGBTrain.fit(train_data)
      val fitEndTime = System.currentTimeMillis()
      val test_data = getTestData(spark,params).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val predictions = model.transform(test_data)

      val evaluator = algType match {
        case "classification" =>
          new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")
        case "regression" =>
          new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse")
      }
      val res = evaluator.evaluate(predictions)
      println(s"Test Error =  ${(1.0-res)}")

      val costTime = (fitEndTime-start_time)/1000.0
      params.setEvaluation(res)
      params.setCostTime(costTime)
      params.setAlgorithmName("xgboost")
      params.setTestcaseType(appName)

      predictions.select("prediction","label","features").show(5)
      val folder = new File("report")
      if (!folder.exists()) {
        val mkdir = folder.mkdirs()
        println(s"Create dir report ${mkdir}")
      }
      val writer = new FileWriter(s"report/XGBT_${
        Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
          System.currentTimeMillis())
      }.yml")

      yaml.dump(params, writer)
      println(s"Exec Successful: costTime: ${costTime}s; evaluation: ${res}")
    }catch {
      case e: Throwable=>
        println(s"Exec Failure: ${e.getMessage}")
        val sw: StringWriter = new StringWriter()
        val pw: PrintWriter = new PrintWriter(sw)
        e.printStackTrace(pw)
        println("=============>>printStackTraceStr Exception: " + e.getClass + "\n===>" + sw.toString)
    }
  }
  def getTrainData(spark:SparkSession,config:XGBTParams): Dataset[Row] ={
    val tr_fname = config.getTr_fname
    println("tr_fname",tr_fname)
    var reader = spark.read.format("libsvm").option("vectorType",config.getVectorType)
    val tr_data = reader.load(tr_fname)
    tr_data
  }
  def getTestData(spark: SparkSession, config: XGBTParams): Dataset[Row] = {
    val ts_fname = config.getTs_fname
    println("ts_fname" , ts_fname)
    var reader= spark.read.format("libsvm").option("vectorType",config.getVectorType)
    val ts_data = reader.load(ts_fname)
    ts_data
  }
}
