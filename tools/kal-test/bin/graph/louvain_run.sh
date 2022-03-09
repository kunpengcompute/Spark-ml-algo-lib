#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <dataset name>"
  echo "1st argument: name of dataset: name of dataset: e.g. graph500_22"
  exit 0
  ;;
esac

if [ $# -ne 1 ];then
  echo "Usage:<dataset name>"
 	echo "dataset name:graph500_22,graph500_24,graph500_25,cit_patents,uk_2002 or arabic_2005"
	exit 0
fi

current_path=$(dirname $(readlink -f "$0"))
echo "current folder path: ${current_path}"

source conf/graph/louvain/louvain_spark.properties

dataset_name=$1

if [ ${dataset_name} != "graph500_22" ] &&
   [ ${dataset_name} != "graph500_24" ] &&
   [ ${dataset_name} != "graph500_25" ] &&
   [ ${dataset_name} != "cit_patents" ] &&
   [ ${dataset_name} != "uk_2002" ] &&
   [ ${dataset_name} != "arabic_2005" ];then
  echo "invalid dataset name,dataset name:graph500_22,graph500_24,graph500_25,cit_patents,uk_2002 or arabic_20055"
  exit 1
fi

hdfs dfs -rm -r -f "/tmp/graph/result/louvain/${dataset_name}"

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')


# concatnate strings as a new variable
num_executors="${dataset_name}_numExecutors_${cpu_name}"
executor_cores="${dataset_name}_executorCores_${cpu_name}"
executor_memory="${dataset_name}_executorMemory_${cpu_name}"
extra_java_options="${dataset_name}_extraJavaOptions_${cpu_name}"
num_partitions="${dataset_name}_numPartitions_${cpu_name}"
deploy_mode="deployMode"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
extra_java_options_val=${!extra_java_options}
deploy_mode_val=${!deploy_mode}
num_partitions_val=${!num_partitions}

echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${extra_java_options} : ${extra_java_options_val}"
echo "${deploy_mode} : ${deploy_mode_val}"
echo "${num_partitions} : ${num_partitions_val}"

if [ ! ${num_executors_val} ] ||
  [ ! ${executor_cores_val} ] ||
  [ ! ${executor_memory_val} ] ||
  [ ! ${extra_java_options_val} ] ||
  [ ! ${num_partitions_val} ]; then
  echo "Some values are NULL, please confirm with the property files"
  exit 0
fi

source conf/graph/graph_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}
data_path_val=${!dataset_name}
echo "${dataset_name} : ${data_path_val}"

echo "start to submit spark jobs"
echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

spark-submit \
--class com.bigdata.graph.LouvainRunner \
--master yarn \
--deploy-mode ${deploy_mode_val} \
--num-executors ${num_executors_val} \
--executor-memory ${executor_memory_val} \
--executor-cores ${executor_cores_val} \
--driver-memory 16g \
--conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
--conf spark.scheduler.maxRegisteredResourcesWaitingTime=3600000 \
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
./lib/kal-test_2.11-0.1.jar ${dataset_name} ${num_partitions_val} "no" ${data_path_val}
