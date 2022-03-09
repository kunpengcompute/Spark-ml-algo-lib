package com.bigdata.graph

import java.io.{File, FileWriter}
import java.util.{HashMap => JHashMap}

import com.bigdata.utils.Utils
import org.apache.spark._
import org.apache.spark.graphx._
import org.yaml.snakeyaml.{DumperOptions, TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

import scala.collection.immutable.IntMap
import org.apache.spark.storage.StorageLevel

import scala.beans.BeanProperty

case class Index(clic:String)

/**
 * refer to [[https://github.com/DMGroup-IUPUI/Spark-kCore/blob/be0465f11201bb099747dfa32c212609f8b2bb8b/src/main/scala/KcoreMain.scala]],
 * add cycle check and change result store
 */
class KCoreRawParams extends Serializable {
    @BeanProperty var partition = new JHashMap[String, Int]
    @BeanProperty var split = new JHashMap[String, String]
    @BeanProperty var iterNum = new JHashMap[String, Int]

    @BeanProperty var datasetName: String = _
    @BeanProperty var costTime: Double = _
    @BeanProperty var isRaw: String = _
    @BeanProperty var curPartition: String = _
    @BeanProperty var algorithmName: String = _
    @BeanProperty var testcaseType: String = _
}

object KCore {

    val initialMsg="-10"
    def mergeMsg(msg1: String, msg2:String): String = msg1+":"+msg2

    def vprog(vertexId: VertexId, value: (Int, Int,IntMap[Int],Int), message: String): (Int, Int,IntMap[Int],Int) = {
        if (message == initialMsg){
            return (value._1,value._2,value._3,value._4)
        }
        else{
            val msg=message.split(":")
            val elems=msg //newWeights.values
            var counts:Array[Int]=new Array[Int](value._1+1)
            for (m <-elems){
                val im=m.toInt
                if(im<=value._1)
                {
                    counts(im)=counts(im)+1
                }
                else{
                    counts(value._1)=counts(value._1)+1
                }
            }
            var curWeight =  0 //value._4-newWeights.size
            for(i<-value._1 to 1 by -1){
                curWeight=curWeight+counts(i)
                if(i<=curWeight){
                    return (i, value._1,value._3,value._4)  
                }
            }
            return (0, value._1,value._3,value._4)
            }
    }

    def sendMsg(triplet: EdgeTriplet[(Int, Int,IntMap[Int],Int), Int]): Iterator[(VertexId, String)] = {
        val sourceVertex = triplet.srcAttr
        val destVertex=triplet.dstAttr
        return Iterator((triplet.dstId,sourceVertex._1.toString),(triplet.srcId,destVertex._1.toString))

    }



    def main(args: Array[String]){
        val RESULT_SPLIT = ","
        val PARAM_FILEPATH = "conf/graph/kcore/kcore.yml"
        if (args.length < 5) {
            println(args.mkString(","))
            println("Usage:KCore <dataset_name> <input_path> <output_path> <is_raw> <cpu_name>")
            System.exit(-1)
        }
        val dataset = args(0)
        val inputPath = args(1)
        val outputPath = args(2)
        val isRaw = args(3)
        val cpuName = args(4)

        val representer = new Representer
        representer.addClassTag(classOf[KCoreRawParams], Tag.MAP)
        val options = new DumperOptions
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
        val yaml = new Yaml(new Constructor(classOf[KCoreRawParams]), representer, options)
        val description = new TypeDescription(classOf[KCoreRawParams])
        yaml.addTypeDescription(description)
        val params = yaml.load(Utils.getStream(PARAM_FILEPATH)).asInstanceOf[KCoreRawParams]
        val partition = params.getPartition.get(s"${dataset}_${cpuName}_${isRaw}")
        val split = params.getSplit.get(dataset)
        val appName = s"KCORE_RAW_${dataset}_${cpuName}"
        val maxIter = params.iterNum.get(dataset)

        //setting up spark environment
        val conf: SparkConf = new SparkConf()
            .setAppName(appName)
        val sc = new SparkContext(conf)

        val startTime = System.currentTimeMillis()
        val graphData = Util.readUndirectDataFromHDFS(sc, inputPath, split, partition)
          .map(x => (x._1.toLong, x._2.toLong))
          .persist(StorageLevel.MEMORY_AND_DISK_SER)
        val ygraph = Util.convertToGraphXGraph(graphData)
          .partitionBy(PartitionStrategy.RandomVertexCut)
          .groupEdges((e1, e2) => e1)
          .subgraph(epred = edge => edge.srcId != edge.dstId)

        val deg = ygraph.degrees

        val mgraph=ygraph.outerJoinVertices(deg)((id, oldattr, newattr) =>newattr.getOrElse(0)).mapVertices((id, attr) =>(attr,-1,IntMap[Int](),attr))
        ygraph.unpersist()
        val minGraph = mgraph.pregel(initialMsg,maxIter,EdgeDirection.Either)(vprog,sendMsg,mergeMsg)

        minGraph.vertices.map(x => s"${x._1}${RESULT_SPLIT}${x._2._1}").saveAsTextFile(outputPath)

        val costTime = (System.currentTimeMillis() - startTime) / 1000.0
        params.setCostTime(costTime)

        println(s"Exec Successful: KCore Decomposition Raw costTime: ${costTime}s")

        params.setDatasetName(dataset)
        params.setIsRaw(isRaw)
        params.setCurPartition(s"$partition")
        params.setAlgorithmName("KCoreRaw")
        params.setTestcaseType(s"KCore_Raw_${dataset}")
        val folder = new File("report")
        if (!folder.exists()) {
            val mkdir = folder.mkdirs()
            println(s"Create dir report ${mkdir}")
        }
        val writer = new FileWriter(s"report/KCORE_RAW_${
            Utils.getDateStrFromUTC("yyyyMMdd_HHmmss",
                System.currentTimeMillis())
        }.yml")
        yaml.dump(params, writer)
        sc.stop()
    }
}