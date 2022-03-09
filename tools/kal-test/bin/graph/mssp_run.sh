#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <dataset name> <source number>"
  echo "1st argument: name of dataset: e.g. uk_2002 5 or uk_2002 5 raw or livejournal 5 or arabic_2005 5"
  exit 0
  ;;
esac

if [ $# -lt 2 ]; then
  echo "please input 2 argument: <dataset name> <source number>"
  echo "1st argument: name of dataset: e.g. uk_2002 5; uk_2002 5 raw"
  exit 0
fi

current_path=$(dirname $(readlink -f "$0"))
echo "current folder path: ${current_path}"

source conf/graph/mssp/mssp_spark.properties
source conf/graph/graph_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}

dataset_name=$1
source_num=$2
is_raw=$3

hdfs dfs -rm -r -f /tmp/graph/result/mssp/${dataset_name}_${source_num}

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

# concatnate strings as a new variable
num_executors="${dataset_name}_${source_num}_numExectuors_${cpu_name}${is_raw}"
executor_cores="${dataset_name}_${source_num}_executorCores_${cpu_name}${is_raw}"
executor_memory="${dataset_name}_${source_num}_executorMemory_${cpu_name}${is_raw}"
driver_memory="${dataset_name}_${source_num}_driverMemory_${cpu_name}${is_raw}"
extra_java_options="${dataset_name}_${source_num}_extraJavaOptions_${cpu_name}${is_raw}"
compute_partition="${dataset_name}_${source_num}_computePartition_${cpu_name}${is_raw}"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
driver_memory_val=${!driver_memory}
extra_java_options_val=${!extra_java_options}
compute_partition_val=${!compute_partition}

echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${driver_memory} : ${driver_memory_val}"
echo "${extra_java_options} : ${extra_java_options_val}"
echo "${compute_partition} : ${compute_partition_val}"

if [ ! ${num_executors_val} ] ||
  [ ! ${executor_cores_val} ] ||
  [ ! ${driver_memory_val} ] ||
  [ ! ${extra_java_options_val} ] ||
  [ ! ${compute_partition_val} ] ||
  [ ! ${executor_memory_val} ]; then
  echo "Some values are NULL, please confirm with the property files"
  exit 0
fi

echo "start to submit spark jobs"
echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

mkdir -p log
if [[ ${is_raw} == "raw" ]]; then
spark-submit \
--driver-class-path "lib/kal-test_2.11-0.1.jar:lib/snakeyaml-1.19.jar" \
--class com.bigdata.graph.MSSPRunner \
--master yarn \
--num-executors ${num_executors_val} \
--executor-memory ${executor_memory_val} \
--executor-cores ${executor_cores_val} \
--driver-memory ${driver_memory_val} \
--conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
--conf spark.driver.maxResultSize=100g \
./lib/kal-test_2.11-0.1.jar ${dataset_name}_${source_num}  ${compute_partition_val} ${is_raw}  | tee ./log/log
else
spark-submit \
--class com.bigdata.graph.MSSPRunner \
--master yarn \
--num-executors ${num_executors_val} \
--executor-memory ${executor_memory_val} \
--executor-cores ${executor_cores_val} \
--driver-memory ${driver_memory_val} \
--conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
--conf spark.driver.maxResultSize=100g \
--jars "lib/boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
--driver-class-path "lib/kal-test_2.11-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
--conf "spark.executor.extraClassPath=boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
./lib/kal-test_2.11-0.1.jar ${dataset_name}_${source_num}  ${compute_partition_val}  | tee ./log/log
fi
