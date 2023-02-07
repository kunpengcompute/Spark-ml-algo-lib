#!/bin/bash
set -e

function usage() {
  echo "Usage: <dataset name>"
  echo "1st argument: name of dataset: e.g. glove"
}

case "$1" in
-h | --help | ?)
  usage
  exit 0
  ;;
esac

if [ $# -ne 1 ]; then
  usage
  exit 0
fi

dataset_name=$1
cpu_name=$(lscpu | grep Architecture | awk '{print $2}')
source conf/ml/ml_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}
kal_version=kalVersion
kal_version_val=${!kal_version}
scala_version=scalaVersion
scala_version_val=${!scala_version}
save_resultPath=saveResultPath
save_resultPath_val=${!save_resultPath}
data_path_val=${!dataset_name}
echo "${dataset_name} : ${data_path_val}"

numExe=15
exeCore=10
exeMem=50

groundTruthLocalPath="result/KNN/${dataset_name}_truth"
groundTruthHDFSPath="${save_resultPath_val}/KNN/${dataset_name}_truth"
testNum=50000
testBatchSize=5000
k=100
pt=150

rm -rf ${groundTruthLocalPath}
mkdir -p ${groundTruthLocalPath}

echo "--------------- 生成真实解 ---------------"
spark-submit \
--class com.bigdata.compare.ml.KNNVerify \
--driver-class-path "./lib/kal-test_${scala_version_val}-0.1.jar" \
--master yarn \
--deploy-mode client \
--num-executors ${numExe} \
--executor-cores ${exeCore} \
--executor-memory ${exeMem}g \
--driver-cores 50 \
--driver-memory 50g \
--conf "spark.executor.extraJavaOptions=-Xms${exeMem}g" \
--conf "spark.driver.maxResultSize=256G" \
--conf "spark.scheduler.mode=FAIR" \
--conf "spark.network.timeout=10000000" \
--conf "spark.executor.heartbeatInterval=1000" \
--conf "spark.scheduler.maxRegisteredResourcesWaitingTime=24h" \
--conf "spark.scheduler.minRegisteredResourcesRatio=1.0" \
./lib/kal-test_${scala_version_val}-0.1.jar \
--task "write" \
--pt ${pt} \
--k ${k} \
--testNum ${testNum} \
--testBatchSize ${testBatchSize} \
--dataPath ${data_path_val} \
--groundTruthLocalPath ${groundTruthLocalPath}

hadoop fs -mkdir -p ${groundTruthHDFSPath}
hadoop fs -rm -r ${groundTruthHDFSPath}
hadoop fs -put ${groundTruthLocalPath} ${groundTruthHDFSPath}


echo "--------------- 生成自研算法结果，并与真实解做对比 ---------------"
spark-submit \
--class com.bigdata.compare.ml.KNNVerify \
--jars "lib/fastutil-8.3.1.jar,lib/boostkit-ml-acc_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar,lib/boostkit-ml-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar,lib/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
--driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/fastutil-8.3.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-ml-acc_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar:lib/boostkit-ml-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar:lib/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
--conf "spark.executor.extraClassPath=/opt/ml_classpath/fastutil-8.3.1.jar:/opt/ml_classpath/boostkit-ml-acc_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar:/opt/ml_classpath/boostkit-ml-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar:/opt/ml_classpath/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
--master yarn \
--deploy-mode client \
--num-executors ${numExe} \
--executor-cores ${exeCore} \
--executor-memory ${exeMem}g \
--driver-cores 50 \
--driver-memory 50g \
--conf "spark.executor.extraJavaOptions=-Xms${exeMem}g" \
--conf "spark.driver.maxResultSize=256G" \
--conf "spark.scheduler.mode=FAIR" \
--conf "spark.network.timeout=10000000" \
--conf "spark.executor.heartbeatInterval=1000" \
--conf "spark.scheduler.maxRegisteredResourcesWaitingTime=24h" \
--conf "spark.scheduler.minRegisteredResourcesRatio=1.0" \
./lib/kal-test_${scala_version_val}-0.1.jar \
--task "verify" \
--pt ${pt} \
--k ${k} \
--testNum ${testNum} \
--testBatchSize ${testBatchSize} \
--dataset_name ${dataset_name} \
--dataPath ${data_path_val} \
--groundTruthHDFSPath ${groundTruthHDFSPath}