#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <algorithm type> <data structure> <dataset name> <api name>"
  echo "1st argument: name of dataset: e.g. 10M4096"
  echo "2nd argument: name of API: e.g. fit"
  exit 0
  ;;
esac


source conf/ml/svm/svm_spark.properties

dataset_name=$1
api_name=$2

model_conf=${dataset_name}-${api_name}"-raw"

# concatnate strings as a new variable
num_executors="x86_64_numExectuors"
executor_cores="x86_64_executorCores"
executor_memory="x86_64_executorMemory"
extra_java_options="x86_64_extraJavaOptions"
driver_cores="x86_64_driverCores"
driver_memory="x86_64_driverMemory"
master_="x86_64_master"
deploy_mode="x86_64_deployMode"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
extra_java_options_val=${!extra_java_options}
driver_cores_val=${!driver_cores}
driver_memory_val=${!driver_memory}
master_val=${!master_}
deploy_mode_val=${!deploy_mode}

echo "${master_} : ${master_val}"
echo "${deploy_mode} : ${deploy_mode_val}"
echo "${driver_cores} : ${driver_cores_val}"
echo "${driver_memory} : ${driver_memory_val}"
echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${extra_java_options} : ${extra_java_options_val}"

if [ ! ${num_executors_val} ] \
    || [ ! ${executor_cores_val} ] \
    || [ ! ${executor_memory_val} ] \
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
echo "start to submit spark jobs"

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

mkdir -p log
spark-submit \
--driver-class-path "lib/snakeyaml-1.19.jar" \
--jars "lib/fastutil-8.3.1.jar" \
--conf "spark.executor.extraClassPath=fastutil-8.3.1.jar" \
--class com.bigdata.ml.SVMRunner \
--deploy-mode ${deploy_mode_val} \
--driver-cores ${driver_cores_val} \
--driver-memory ${driver_memory_val} \
--num-executors ${num_executors_val} \
--executor-cores ${executor_cores_val} \
--executor-memory ${executor_memory_val} \
--master ${master_val} \
--conf "spark.executor.extraJavaOptions=${extra_java_options_val} -XX:SurvivorRatio=4 -XX:ParallelGCThreads=6" \
./lib/kal-test_2.11-0.1.jar ${model_conf} ${data_path} | tee ./log/log
