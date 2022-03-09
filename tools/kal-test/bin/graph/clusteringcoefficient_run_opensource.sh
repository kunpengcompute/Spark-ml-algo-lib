#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <dataset name><api name>"
  echo "1st argument: name of dataset: name of dataset: e.g. cit_patents"
  echo "2nd argument: name of api: e.g. lcc,avgcc,globalcc"
  exit 0
  ;;
esac

if [ $# -ne 2 ];then
  echo "Usage:<dataset name><api name>"
 	echo "dataset name:graph500_22,graph500_23,graph500_24,graph500_25"
 	echo "api name:lcc,avgcc,globalcc"
	exit 0
fi

current_path=$(dirname $(readlink -f "$0"))
echo "current folder path: ${current_path}"

source conf/graph/clusteringcoefficient/clusteringcoefficient_spark_opensource.properties

dataset_name=$1
api_name=$2

if
   [ ${dataset_name} != "graph500_22" ] &&
   [ ${dataset_name} != "graph500_23" ] &&
   [ ${dataset_name} != "graph500_24" ] &&
   [ ${dataset_name} != "graph500_25" ] ;then
  echo "invalid dataset name,dataset name:graph500_22,graph500_23,graph500_24,graph500_25"
  exit 1
fi
if [ ${api_name} != "lcc" ] &&
   [ ${api_name} != "avgcc" ] &&
   [ ${api_name} != "globalcc" ] ;then
  echo "invalid argument value,api name: lcc,avgcc,globalcc"
  exit 1
fi
outputPath="/tmp/graph/result/clusteringcoefficient/${dataset_name}"

hdfs dfs -rm -r -f ${outputPath}

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')


# concatnate strings as a new variable
num_executors="${dataset_name}_numExecutors_${cpu_name}"
executor_cores="${dataset_name}_executorCores_${cpu_name}"
executor_memory="${dataset_name}_executorMemory_${cpu_name}"
num_partitions="${dataset_name}_numPartitions_${cpu_name}"
split="${dataset_name}_split"
deploy_mode="deployMode"
driver_memory="driverMemory"

split_val=${!split}
num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
deploy_mode_val=${!deploy_mode}
num_partitions_val=${!num_partitions}
driver_memory_val=${!driver_memory}

echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${deploy_mode} : ${deploy_mode_val}"
echo "${num_partitions} : ${num_partitions_val}"
echo "${driver_memory}:${driver_memory_val}"
echo "${split}:${split_val}"

if [ ! ${num_executors_val} ] ||
  [ ! ${executor_cores_val} ] ||
  [ ! ${executor_memory_val} ] ||
  [ ! ${num_partitions_val} ] ; then
  echo "Some values are NULL, please confirm with the property files"
  exit 0
fi

source conf/graph/graph_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}
data_path_val=${!dataset_name}
echo "${dataset_name} : ${data_path_val}"

mkdir -p log

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

echo "start to submit spark jobs"
spark-submit \
--class com.huawei.bigdata.graphx.Test \
--master yarn \
--deploy-mode ${deploy_mode_val} \
--name "clusteringcoefficient_${1}_${2}" \
--num-executors ${num_executors_val} \
--executor-memory ${executor_memory_val} \
--executor-cores ${executor_cores_val} \
--driver-memory ${driver_memory_val} \
--conf spark.rpc.askTimeout=36000 \
--conf spark.scheduler.maxRegisteredResourcesWaitingTime=3600000 \
--conf spark.worker.timeout=3600 \
--conf spark.network.timeout=6000s \
--conf spark.storage.blockManagerSlaveTimeoutMs=600000 \
--conf spark.shuffle.blockTransferService=nio \
--conf spark.driver.maxResultSize=100g \
--conf spark.shuffle.manager=SORT \
--conf spark.broadcast.blockSize=25g \
--conf spark.network.timeout=1200s \
--conf spark.rpc.message.maxSize=2046 \
--conf spark.core.connection.ack.wait.timeout=60000s \
--conf spark.executor.extraJavaOptions="-Xms35g" \
--conf spark.rdd.compress=true \
--jars "lib/lcc_kaiyuan.jar" \
--driver-class-path "lib/lcc_kaiyuan.jar:lib/graph_test_2.11-1.0.0.jar" \
--conf "spark.executor.extraClassPath=lcc_kaiyuan.jar" \
./lib/graph_test_2.11-1.0.0.jar clusteringcoefficient yarn ${data_path_val} "${split_val}" ${outputPath} ${num_partitions_val} ${api_name} > clusteringcoefficient_temp.log
CostTime=$(cat clusteringcoefficient_temp.log | grep "LabelPropagation is finished" | awk '{print $8}')
currentTime=$(date "+%Y%m%d_%H%M%S")
rm -rf clusteringcoefficient_temp.log
echo -e "algorithmName: clusteringcoefficient\ncostTime: $CostTime\ndatasetName: ${dataset_name}\napi_name: ${api_name}\nisRaw: 'yes'\ntestcaseType: clusteringcoefficient_opensource_${dataset_name}_${api_name}\n" > ./report/"ClusteringCoefficient_${currentTime}.yml"
echo "Exec Successful: end." > ./log/log