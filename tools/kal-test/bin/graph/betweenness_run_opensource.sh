#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <dataset name>"
  echo "1st argument: name of dataset: name of dataset: e.g. cit_patents"
  exit 0
  ;;
esac

if [ $# -ne 1 ]; then
  echo "Usage:<dataset name>"
  echo "dataset name:cit_patents,enwiki_2018,uk_2002"
  exit 0
fi

current_path=$(dirname $(readlink -f "$0"))
echo "current folder path: ${current_path}"

source conf/graph/betweenness/betweenness_spark_opensource.properties

dataset_name=$1

if [ ${dataset_name} != "cit_patents" ] &&
  [ ${dataset_name} != "enwiki_2018" ] &&
  [ ${dataset_name} != "uk_2002" ]; then
  echo "invalid dataset name,dataset name:cit_patents,enwiki_2018,uk_2002"
  exit 1
fi
outputPath=/tmp/graph/result/betweenness/${dataset_name}_opensource
hdfs dfs -rm -r -f ${outputPath}

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

# concatnate strings as a new variable
num_executors="${dataset_name}_numExecutors_${cpu_name}"
executor_cores="${dataset_name}_executorCores_${cpu_name}"
executor_memory="${dataset_name}_executorMemory_${cpu_name}"
num_partitions="${dataset_name}_numPartitions_${cpu_name}"
pivots="${dataset_name}_pivots_${cpu_name}"
iteration="${dataset_name}_iteration_${cpu_name}"
graph_split="${dataset_name}_graphSplit_${cpu_name}"
deploy_mode="deployMode"
driver_memory="driverMemory"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
deploy_mode_val=${!deploy_mode}
num_partitions_val=${!num_partitions}
driver_memory_val=${!driver_memory}
pivots_val=${!pivots}
iteration_val=${!iteration}
graph_split_val=${!graph_split}

echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${deploy_mode} : ${deploy_mode_val}"
echo "${num_partitions} : ${num_partitions_val}"
echo "${driver_memory}:${driver_memory_val}"
echo "${pivots}:${pivots_val}"
echo "${iteration}:${iteration_val}"
echo "${graph_split}:${graph_split_val}"

if [ ! ${num_executors_val} ] ||
  [ ! ${executor_cores_val} ] ||
  [ ! ${executor_memory_val} ] ||
  [ ! ${pivots_val} ] ||
  [ ! ${iteration_val} ] ||
  [ ! ${graph_split_val} ]; then
  echo "Some values are NULL, please confirm with the property files"
  exit 0
fi

source conf/graph/graph_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}
gt_path="${dataset_name}_gt"
data_path_val=${!dataset_name}
gt_path_val=${!gt_path}
echo "${dataset_name} : ${data_path_val}"
echo "${gt_path} : ${gt_path_val}"

mkdir -p log

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

echo "start to submit spark jobs--betweenness_${dataset_name}_opensource"

spark-submit \
--master yarn \
--deploy-mode ${deploy_mode_val} \
--name "Betweenness_${dataset_name}_opensource" \
--num-executors ${num_executors_val} \
--executor-memory ${executor_memory_val} \
--executor-cores ${executor_cores_val} \
--driver-memory ${driver_memory_val} \
--conf spark.kryoserializer.buffer.max=2047m \
--conf spark.driver.maxResultSize=0 \
--conf spark.ui.showConsoleProgress=true \
--conf spark.driver.extraJavaOptions="-Xms${driver_memory_val} -XX:hashCode=0" \
--conf spark.executor.extraJavaOptions="-Xms${executor_memory_val} -XX:hashCode=0" \
--conf spark.rpc.askTimeout=1000000s \
--conf spark.network.timeout=1000000s \
--conf spark.executor.heartbeatInterval=100000s \
--conf spark.rpc.message.maxSize=1000 \
--jars "./lib/scopt_2.11-3.2.0.jar" \
./lib/hbse_2.11-0.1.jar \
-m yarn \
-s ${graph_split_val} \
-n ${num_partitions_val} \
-i ${data_path_val} \
-o ${outputPath} \
-g ${gt_path_val} \
-p ${pivots_val} \
-b ${iteration_val} > betweenness_temp.log
CostTime=$(cat betweenness_temp.log |grep "CostTime of Top-K" | awk '{print $6}')
Accuracy=$(cat betweenness_temp.log |grep "Accuracy of Top-K" | awk '{print $6}')
currentTime=$(date "+%Y%m%d_H%M%S")
rm -rf betweenness_temp.log
echo -e "algorithmName: Betweenness\ncostTime: $CostTime\ndatasetName: ${dataset_name}\nisRaw: 'yes'\nAccuracy: ${Accuracy}\ntestcaseType: Betweenness_opensource_${dataset_name}\n" > ./report/"Betweenness_${currentTime}.yml"
echo "Exec Successful: end." > ./log/log
