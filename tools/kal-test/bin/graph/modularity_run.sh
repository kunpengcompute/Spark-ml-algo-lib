#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <dataset name>"
  echo "1st argument: name of dataset: e.g. grah500_22"
  exit 0
  ;;
esac

if [ $# -ne 1 ]; then
  echo "please input 1 argument: <dataset name>"
  echo "1st argument: name of dataset: e.g. graph500_22"
  exit 0
fi

current_path=$(dirname $(readlink -f "$0"))
echo "current folder path: ${current_path}"


source conf/graph/modularity/modularity_spark.properties
source conf/graph/graph_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}

dataset_name=$1
cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

# concatnate strings as a new variable
num_executors="${dataset_name}_numExectuors_${cpu_name}"
executor_cores="${dataset_name}_executorCores_${cpu_name}"
executor_memory="${dataset_name}_executorMemory_${cpu_name}"
extra_java_options="${dataset_name}_extraJavaOptions_${cpu_name}"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
extra_java_options_val=${!extra_java_options}

echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${extra_java_options} : ${extra_java_options_val}"

if [ ! ${num_executors_val} ] ||
  [ ! ${executor_cores_val} ] ||
  [ ! ${executor_memory_val} ] ||
  [ ! ${extra_java_options_val} ]; then
  echo "Some values are NULL, please confirm with the property files"
  exit 0
fi

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30
mkdir -p log
echo "start to submit spark jobs"


spark-submit \
--class com.bigdata.graph.ModularityRunner \
--driver-memory 80g \
--master yarn \
--num-executors ${num_executors_val} \
--executor-cores ${executor_cores_val} \
--executor-memory ${executor_memory_val} \
--conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
--jars "lib/fastutil-8.3.1.jar,lib/boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
--driver-class-path "lib/kal-test_2.11-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
--conf "spark.executor.extraClassPath=fastutil-8.3.1.jar:boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
./lib/kal-test_2.11-0.1.jar ${dataset_name} | tee ./log/log
