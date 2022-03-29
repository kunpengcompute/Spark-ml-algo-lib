package com.bigdata.ml

import com.bigdata.utils.Utils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.feature.{IDF, IDFModel}
import org.yaml.snakeyaml.Yaml

import java.io.{File, FileInputStream, FileOutputStream, FileWriter, ObjectInputStream, ObjectOutputStream}
import java.util
import scala.beans.BeanProperty

class IDFConfig extends Serializable {
  @BeanProperty var pr: util.HashMap[String, Object] = _
}

object IDFRunner{
  def main(args: Array[String]): Unit = {
    //get parameters
    val ifRaw = args(0)
    val datasetName = args(1)
    val stream = Utils.getStream("conf/ml/idf/idf.yml")
    val yaml = new Yaml
    val yamlCfg = yaml.load(stream).asInstanceOf[util.HashMap[String, Object]].get("idf")
      .asInstanceOf[util.HashMap[String, Object]].get(ifRaw)
      .asInstanceOf[util.HashMap[String, Object]].get(datasetName)
      .asInstanceOf[util.HashMap[String, Object]]
    val pt = yamlCfg.get("pt").asInstanceOf[Int]
    val combineStrategy = yamlCfg.get("combineStrategy").asInstanceOf[String]
    val fetchMethod = yamlCfg.get("fetchMethod").asInstanceOf[String]

    val dataPath = args(2)
    val modelPath = args(3)
    val checkModel = java.lang.Boolean.parseBoolean(args(4))

    //init
    val conf = new SparkConf().setAppName(s"IDF_${datasetName}_${ifRaw}")
    conf.set("spark.driver.maxResultSize", "256G")
    if (ifRaw == "opt"){
      conf.set("spark.sophon.ml.idf.combineStrategy", combineStrategy)
      conf.set("spark.sophon.ml.idf.fetchMethod", fetchMethod)
    }
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val startTime = System.currentTimeMillis()
    //convert to orc(The orc data type is used by default.To change the data type, modify the code.)
    val orcData = spark.read.schema(Encoders.product[DocSchema].schema).format("orc").load(dataPath)
    //repatition
    val data = if (pt > 0){
      orcData.select("tf").repartition(pt)
    } else {
      orcData.select("tf")
    }
    val idf = new IDF().setInputCol("tf").setOutputCol("tf_idf")
    val model = idf.fit(data)
    val trainFinishTime = System.currentTimeMillis()
    //save model
    saveModelToLocal(model, s"${modelPath}/${ifRaw}/")
    //check Models
    if (checkModel){
      val expectModel = loadModelFromLocal(s"${modelPath}/pri/IDFModel")
      val numMismatch = expectModel.idf.toArray.zip(model.idf.toArray).count(v => math.abs(v._1 - v._2) > 1E-10)
      if (numMismatch != 0){
        println("IDF model mismatch!")
      } else {
        println("IDF model matched!")
      }
    }
    val endTime = System.currentTimeMillis()
    val algTime = (trainFinishTime - startTime)/1000
    val allTime = (endTime - startTime)/1000
    println(s"IDF run complete, algTime = ${algTime}s, allTime = ${allTime}s")
    yamlCfg.put("algTime", algTime.toString+"s")
    yamlCfg.put("allTime", allTime.toString+"s")
    yamlCfg.put("algorithmName", "IDF")
    yamlCfg.put("dataset", datasetName)
    yamlCfg.put("Flag", Array[String]("idf", datasetName))
    val writer = new FileWriter(s"report/IDF_${Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",System.currentTimeMillis())}.yml")
    yaml.dump(yamlCfg, writer)
  }
  def saveModelToLocal(model: IDFModel, dirPath: String): Unit = {
    val dir = new File(dirPath)
    if (!dir.exists()){
      dir.mkdirs()
    }
    val outObj = new ObjectOutputStream(new FileOutputStream(dirPath+"/IDFModel"))
    outObj.writeObject(model)
    outObj.close()
  }
  def loadModelFromLocal(modelPath: String): IDFModel = {
    val inObj = new ObjectInputStream(new FileInputStream(modelPath))
    val model = inObj.readObject().asInstanceOf[IDFModel]
    model
  }
  case class DocSchema(id: Long, tf: Vector)
}
