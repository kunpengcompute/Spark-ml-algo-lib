#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <dataset name><weight or not>"
  echo "1st argument: name of dataset: name of dataset: e.g. cit_patents"
  echo "2nd argument: weight or not: e.g. weighted,unweighted"
  exit 0
  ;;
esac

if [ $# -ne 2 ];then
  echo "Usage:<dataset name><weight or not>"
 	echo "dataset name:cit_patents,graph500_23,uk_2002"
  echo "weight or not:weighted,unweighted"
	exit 0
fi

current_path=$(dirname $(readlink -f "$0"))
echo "current folder path: ${current_path}"

source conf/graph/closeness/closeness_spark.properties

dataset_name=$1
weight=$2

if [ ${dataset_name} != "cit_patents" ] &&
   [ ${dataset_name} != "graph500_23" ] &&
   [ ${dataset_name} != "uk_2002" ] ;then
  echo "invalid dataset name,dataset name:cit_patents,graph500_23,uk_2002"
  exit 1
fi
if [ ${weight} != "weighted" ] && [ ${weight} != "unweighted" ];then
  echo "invalid argument value,must be: weighted or unweighted"
  exit 1
fi

hdfs dfs -rm -r -f "/tmp/graph/result/closeness/${dataset_name}_${weight}"

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')


# concatnate strings as a new variable
num_executors="${dataset_name}_${weight}_numExecutors_${cpu_name}"
executor_cores="${dataset_name}_${weight}_executorCores_${cpu_name}"
executor_memory="${dataset_name}_${weight}_executorMemory_${cpu_name}"
num_partitions="${dataset_name}_${weight}_numPartitions_${cpu_name}"
ratio="${dataset_name}_${weight}_ratio_${cpu_name}"
output_node_num="outputNodeNum"
deploy_mode="deployMode"
driver_memory="driverMemory"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
deploy_mode_val=${!deploy_mode}
num_partitions_val=${!num_partitions}
ratio_val=${!ratio}
output_node_num_val=${!output_node_num}
driver_memory_val=${!driver_memory}

echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${deploy_mode} : ${deploy_mode_val}"
echo "${num_partitions} : ${num_partitions_val}"
echo "${ratio} : ${ratio_val}"
echo "${output_node_num} : ${output_node_num_val}"
echo "${driver_memory}:${driver_memory_val}"

if [ ! ${num_executors_val} ] ||
  [ ! ${executor_cores_val} ] ||
  [ ! ${executor_memory_val} ] ||
  [ ! ${num_partitions_val} ] ||
  [ ! ${ratio_val} ] ||
  [ ! ${output_node_num_val} ]; then
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
--class com.bigdata.graph.ClosenessRunner \
--master yarn \
--deploy-mode ${deploy_mode_val} \
--num-executors ${num_executors_val} \
--executor-memory ${executor_memory_val} \
--executor-cores ${executor_cores_val} \
--driver-memory ${driver_memory_val} \
--conf spark.worker.timeout=3600 \
--conf spark.driver.maxResultSize=200g \
--conf spark.rpc.askTimeout=36000 \
--conf spark.rdd.compress=true \
--conf spark.network.timeout=6000s \
--conf spark.broadcast.blockSize=4m \
--conf spark.shuffle.manager=SORT \
--conf spark.shuffle.blockTransferService=nio \
--conf spark.locality.wait.node=0 \
--jars "lib/fastutil-8.3.1.jar,lib/boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
--driver-class-path "lib/kal-test_2.11-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
--conf "spark.executor.extraClassPath=fastutil-8.3.1.jar:boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
./lib/kal-test_2.11-0.1.jar ${dataset_name} ${num_partitions_val} ${weight} ${output_node_num_val} ${ratio_val} "no" ${data_path_val} "false" | tee ./log/log
