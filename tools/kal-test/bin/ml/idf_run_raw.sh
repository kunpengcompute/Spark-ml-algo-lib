#!/bin/bash
set -e

ifRaw="pri"
case "$1" in
-h | --help | ?)
  echo "Usage: <dataset name>"
  echo "1rd argument: name of dataset: e.g. D10m200m"
  echo "This script will save the model in \${workspace}/output/models/\${datasetName}/${ifRaw}/"
  exit 0
esac

if [ $# -ne 1 ]; then
  echo "Usage: <dataset name>"
  echo "1rd argument: name of dataset: e.g. D10m200m"
  echo "This script will save the model in \${workspace}/output/models/\${datasetName}/${ifRaw}/"
  exit 0
fi


source conf/ml/idf/idf_spark.properties

dataset_name=$1
ifCheckModel="false"

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

# concatnate strings as a new variable
num_executors=${cpu_name}_${ifRaw}"_"${dataset_name}"_numExecutors"
executor_cores=${cpu_name}_${ifRaw}"_"${dataset_name}"_executorCores"
executor_memory=${cpu_name}_${ifRaw}"_"${dataset_name}"_executorMemory"
executor_extra_java_options=${cpu_name}_${ifRaw}"_"${dataset_name}"_extraJavaOptions"
driver_cores=${cpu_name}_${ifRaw}"_"${dataset_name}"_driverCores"
driver_memory=${cpu_name}_${ifRaw}"_"${dataset_name}"_driverMemory"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
executor_extra_java_options_val=${!executor_extra_java_options}
driver_cores_val=${!driver_cores}
driver_memory_val=${!driver_memory}


echo "master : ${master}"
echo "deploy_mode : ${deployMode}"
echo "${driver_cores} : ${driver_cores_val}"
echo "${driver_memory} : ${driver_memory_val}"
echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${executor_extra_java_options} : ${executor_extra_java_options_val}"
echo "cpu_name : ${cpu_name}"

if [ ! ${num_executors_val} ] \
    || [ ! ${executor_cores_val} ] \
    || [ ! ${executor_memory_val} ] \
    || [ ! ${executor_extra_java_options_val} ] \
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
data_path_val=${!dataset_name}
models_path=${dataset_name}_modelsPath
models_path_val=${!models_path}
echo "${dataset_name} : ${data_path_val}"
echo "spark_version : ${spark_version_val}"
echo "Is raw? -${ifRaw}"
echo "If checkoutModels? -${ifCheckModel}"
echo "models_path : ${models_path_val}"
echo "start to submit spark jobs"

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 3

mkdir -p log
spark-submit \
--class com.bigdata.ml.IDFRunner \
--master ${master} \
--deploy-mode ${deployMode} \
--driver-cores ${driver_cores_val} \
--driver-memory ${driver_memory_val} \
--num-executors ${num_executors_val} \
--executor-cores ${executor_cores_val} \
--executor-memory ${executor_memory_val} \
--conf spark.executor.extraJavaOptions=${executor_extra_java_options_val} \
--driver-java-options "-Xms15g" \
--jars "lib/json4s-ext_2.11-3.2.11.jar,lib/fastutil-8.3.1.jar" \
./lib/kal-test_2.11-0.1.jar ${ifRaw} ${dataset_name} ${data_path_val} ${models_path_val} ${ifCheckModel}| tee ./log/log
