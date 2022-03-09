#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <dataset name><api name><weight or not>"
  echo "1st argument: name of dataset: name of dataset: e.g. cit_patents"
  echo "2nd argument: name of api: e.g. lcc,avgcc,globalcc"
  echo "3nd argument: weight or not: e.g. weighted,unweighted"
  exit 0
  ;;
esac

if [ $# -ne 3 ];then
  echo "Usage:<dataset name><api name><weight or not>"
 	echo "dataset name:cit_patents,uk_2002,arabic_2005,graph500_22,graph500_23,graph500_24,graph500_25"
 	echo "api name:lcc,avgcc,globalcc"
  echo "weight or not:weighted,unweighted"
	exit 0
fi

current_path=$(dirname $(readlink -f "$0"))
echo "current folder path: ${current_path}"

source conf/graph/clusteringcoefficient/clusteringcoefficient_spark.properties

dataset_name=$1
api_name=$2
weight=$3

if [ ${dataset_name} != "cit_patents" ] &&
   [ ${dataset_name} != "uk_2002" ] &&
   [ ${dataset_name} != "arabic_2005" ] &&
   [ ${dataset_name} != "graph500_22" ] &&
   [ ${dataset_name} != "graph500_23" ] &&
   [ ${dataset_name} != "graph500_24" ] &&
   [ ${dataset_name} != "graph500_25" ] ;then
  echo "invalid dataset name,dataset name:cit_patents,uk_2002,arabic_2005,graph500_22,graph500_23,graph500_24,graph500_25"
  exit 1
fi
if [ ${api_name} != "lcc" ] &&
   [ ${api_name} != "avgcc" ] &&
   [ ${api_name} != "globalcc" ] ;then
  echo "invalid argument value,api name: lcc,avgcc,globalcc"
  exit 1
fi
if [ ${weight} != "weighted" ] && [ ${weight} != "unweighted" ];then
  echo "invalid argument value,must be: weighted or unweighted"
  exit 1
fi

hdfs dfs -rm -r -f "/tmp/graph/result/clusteringcoefficient/${dataset_name}_${3}"

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')


# concatnate strings as a new variable
num_executors="${dataset_name}_numExecutors_${cpu_name}"
executor_cores="${dataset_name}_executorCores_${cpu_name}"
executor_memory="${dataset_name}_executorMemory_${cpu_name}"
num_partitions="${dataset_name}_numPartitions_${cpu_name}"
deploy_mode="deployMode"
driver_memory="driverMemory"

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
--class com.bigdata.graph.ClusteringCoefficientRunner \
--master yarn \
--deploy-mode ${deploy_mode_val} \
--num-executors ${num_executors_val} \
--executor-memory ${executor_memory_val} \
--executor-cores ${executor_cores_val} \
--driver-memory ${driver_memory_val} \
--conf spark.executor.memoryOverhead=2048 \
--conf spark.executor.extraJavaOptions="-Xms12g" \
--jars "lib/fastutil-8.3.1.jar,lib/boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
--driver-class-path "lib/kal-test_2.11-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
--conf "spark.executor.extraClassPath=fastutil-8.3.1.jar:boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
./lib/kal-test_2.11-0.1.jar ${dataset_name} ${num_partitions_val} ${weight} "no" ${data_path_val} ${api_name} | tee ./log/log
