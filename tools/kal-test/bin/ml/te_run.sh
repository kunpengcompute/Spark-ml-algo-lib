#!/bin/bash
set -e

function usage() {
  echo "Usage: <dataset name> <isRaw> <isCheck>"
  echo "1rd argument: name of dataset: e.g. movielens, taobao, criteo40m, criteo150m"
  echo "2th argument: optimization algorithm or raw: [no/yes]"
  echo "3th argument: Whether to Compare Results [no/yes]"
}

case "$1" in
-h | --help | ?)
  usage
  exit 0
esac

if [ $# -ne 3 ]; then
  usage
  exit 0
fi


source conf/ml/te/te_spark.properties
dataset_name=$1
is_raw=$2
if_check=$3
cpu_name=$(lscpu | grep Architecture | awk '{print $2}')
model_conf=${dataset_name}-${is_raw}-${if_check}

# concatnate strings as a new variable
num_executors=${cpu_name}_${dataset_name}"_numExecutors"
executor_cores=${cpu_name}_${dataset_name}"_executorCores"
executor_memory=${cpu_name}_${dataset_name}"_executorMemory"
driver_cores=${cpu_name}_${dataset_name}"_driverCores"
driver_memory=${cpu_name}_${dataset_name}"_driverMemory"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
driver_cores_val=${!driver_cores}
driver_memory_val=${!driver_memory}

echo "master : ${master}"
echo "deployMode : ${deployMode}"
echo "${driver_cores} : ${driver_cores_val}"
echo "${driver_memory} : ${driver_memory_val}"
echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "cpu_name : ${cpu_name}"

if [ ! ${num_executors_val} ] \
    || [ ! ${executor_cores_val} ] \
    || [ ! ${executor_memory_val} ] \
    || [ ! ${driver_cores_val} ] \
    || [ ! ${driver_memory_val} ] \
    || [ ! ${master} ] \
    || [ ! ${deployMode} ] \
    || [ ! ${cpu_name} ]; then
  echo "Some values are NULL, please confirm with the property files"
  exit 0
fi

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


echo "start to clean cache and sleep 3s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 3

echo "start to submit spark jobs --- TargetEncoder-${model_conf}"
if [ ${is_raw} == "no" ]; then
  scp lib/boostkit-ml-acc_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-ml-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent1:/opt/ml_classpath/
  scp lib/boostkit-ml-acc_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-ml-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent2:/opt/ml_classpath/
  scp lib/boostkit-ml-acc_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-ml-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent3:/opt/ml_classpath/

  mkdir -p log
  spark-submit \
  --class com.bigdata.ml.TERunner \
  --master ${master} \
  --deploy-mode ${deployMode} \
  --driver-cores ${driver_cores_val} \
  --driver-memory ${driver_memory_val} \
  --num-executors ${num_executors_val} \
  --executor-cores ${executor_cores_val} \
  --executor-memory ${executor_memory_val} \
  --conf spark.driver.memory=128g \
  --conf spark.locality.wait=0s \
  --conf spark.scheduler.minRegisteredResourcesRatio=1 \
  --conf spark.driver.maxResultSize=40g \
  --conf spark.network.timeout=60000s \
  --conf spark.rpc.askTimeout=60000s \
  --conf spark.executor.heartbeatInterval=600s \
  --conf spark.eventLog.enabled=false \
  --driver-class-path "lib/fastutil-8.3.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-ml-acc_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar:lib/boostkit-ml-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar:lib/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --jars "lib/sparkling-water-assembly-extensions_2.12-3.38.0.1-1-3.1-all.jar,lib/sparkling-water-assembly_2.12-3.38.0.1-1-3.1-all.jar,lib/sparkling-water-assembly-scoring_2.12-3.38.0.1-1-3.1-all.jar,lib/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${model_conf} ${data_path_val} ${cpu_name} ${save_resultPath_val}| tee ./log/log
else
  spark-submit \
  --class com.bigdata.ml.TERunner \
  --master ${master} \
  --deploy-mode ${deployMode} \
  --driver-cores ${driver_cores_val} \
  --driver-memory ${driver_memory_val} \
  --num-executors ${num_executors_val} \
  --executor-cores ${executor_cores_val} \
  --executor-memory ${executor_memory_val} \
  --conf spark.driver.memory=128g \
  --conf spark.locality.wait=0s \
  --conf spark.scheduler.minRegisteredResourcesRatio=1 \
  --conf spark.driver.maxResultSize=40g \
  --conf spark.network.timeout=60000s \
  --conf spark.rpc.askTimeout=60000s \
  --conf spark.executor.heartbeatInterval=600s \
  --conf spark.eventLog.enabled=false \
  --driver-class-path "lib/snakeyaml-1.19.jar" \
  --jars "lib/sparkling-water-assembly-extensions_2.12-3.38.0.1-1-3.1-all.jar,lib/sparkling-water-assembly_2.12-3.38.0.1-1-3.1-all.jar,lib/sparkling-water-assembly-scoring_2.12-3.38.0.1-1-3.1-all.jar,lib/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${model_conf} ${data_path_val} ${cpu_name} ${save_resultPath_val}| tee ./log/log
fi