#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <dataset name>"
  echo "argument: name of dataset: e.g. CP10M1K/CP2M5K/CP1M10K"
  exit 0
  ;;
esac

if [ $# -ne 1 ]; then
  echo "please input 1 arguments: <dataset name>"
  echo "argument: name of dataset: e.g. CP10M1K/CP2M5K/CP1M10K"
  exit 0
fi

source conf/ml/cov/cov_spark.properties

dataset_name=$1
platform_name=$(lscpu | grep Architecture | awk '{print $2}')

model_conf=${dataset_name}-"raw"

# concatnate strings as a new variable
num_executors="numExectuors_"${platform_name}
executor_cores="executorCores_"${platform_name}
executor_memory="executorMemory_"${platform_name}
executor_memory_overhead="executorMemOverhead_"${platform_name}
extra_java_options="extraJavaOptions_"${platform_name}
driver_max_result_size="driverMaxResultSize"
driver_cores="driverCores_"${platform_name}
driver_memory="driverMemory_"${platform_name}
master_="master"
deploy_mode="deployMode"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
executor_memory_overhead_val=${!executor_memory_overhead}
extra_java_options_val=${!extra_java_options}
driver_max_result_size_val=${!driver_max_result_size}
driver_cores_val=${!driver_cores}
driver_memory_val=${!driver_memory}
master_val=${!master_}
deploy_mode_val=${!deploy_mode}

echo ${platform_name}
echo "${master_} : ${master_val}"
echo "${deploy_mode} : ${deploy_mode_val}"
echo "${driver_cores} : ${driver_cores_val}"
echo "${driver_memory} : ${driver_memory_val}"
echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${executor_memory_overhead} : ${executor_memory_overhead_val}"
echo "${extra_java_options} : ${extra_java_options_val}"
echo "${driver_max_result_size} : ${driver_max_result_size_val}"


if [ ! ${num_executors_val} ] \
    || [ ! ${executor_cores_val} ] \
    || [ ! ${executor_memory_val} ] \
    || [ ! ${executor_memory_overhead_val} ] \
    || [ ! ${driver_max_result_size_val} ] \
    || [ ! ${extra_java_options_val} ] \
    || [ ! ${driver_cores_val} ] \
    || [ ! ${driver_memory_val} ] \
    || [ ! ${master_val} ]; then
  echo "Some values are NULL, please confirm with the property files"
  exit 0
fi

source conf/ml/ml_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}
data_path=${!dataset_name}

echo "${dataset_name} : ${data_path}"

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

mkdir -p log
echo "start to submit spark jobs --- Cov-${model_conf}"

spark-submit \
--driver-class-path "lib/fastutil-8.3.1.jar:lib/snakeyaml-1.19.jar" \
--class com.bigdata.ml.CovRunner \
--jars "lib/fastutil-8.3.1.jar" \
--conf "spark.executor.extraClassPath=fastutil-8.3.1.jar" \
--deploy-mode ${deploy_mode_val} \
--driver-cores ${driver_cores_val} \
--driver-memory ${driver_memory_val} \
--num-executors ${num_executors_val} \
--executor-cores ${executor_cores_val} \
--executor-memory ${executor_memory_val} \
--master ${master_val} \
--conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
--conf "spark.executor.memoryOverhead=${executor_memory_overhead_val}" \
--conf "spark.driver.maxResultSize=${driver_max_result_size_val}" \
./lib/kal-test_2.11-0.1.jar ${model_conf} ${data_path} | tee ./log/log
