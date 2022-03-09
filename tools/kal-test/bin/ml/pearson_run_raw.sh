#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <data structure> <dataset name>"
  echo "1st argument: type of data structure: [dataframe/rdd]"
  echo "2nd argument: name of dataset: e.g. CP10M1K"
  exit 0
  ;;
esac

if [ $# -ne 2 ]; then
  echo "please input 2 arguments: <data structure> <dataset name>"
  echo "1st argument: type of data structure: [dataframe/rdd]"
  echo "2nd argument: name of dataset: e.g. CP10M1K"
  exit 0
fi


source conf/ml/pearson/pearson_spark_raw.properties

data_structure=$1
dataset_name=$2
cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

model_conf=${data_structure}_${dataset_name}

# concatnate strings as a new variable
num_executors="numExectuors_"${cpu_name}
executor_cores="executorCores_"${cpu_name}
executor_memory="executorMemory_"${cpu_name}
extra_java_options="extraJavaOptions_"${cpu_name}
driver_cores="driverCores_"${cpu_name}
driver_memory="driverMemory_"${cpu_name}
memory_overhead="execMemOverhead_"${cpu_name}
master_="master"
deploy_mode="deployMode"
dataset_output_=${dataset_name}"_output"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
extra_java_options_val=${!extra_java_options}
driver_cores_val=${!driver_cores}
driver_memory_val=${!driver_memory}
memory_overhead_val=${!memory_overhead}
master_val=${!master_}
deploy_mode_val=${!deploy_mode}


echo "${master_} : ${master_val}"
echo "${deploy_mode} : ${deploy_mode_val}"
echo "${driver_cores} : ${driver_cores_val}"
echo "${driver_memory} : ${driver_memory_val}"
echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${extra_java_options} : ${extra_java_options_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${memory_overhead} : ${memory_overhead_val}"
echo "cpu_name : ${cpu_name}"

if [ ! ${num_executors_val} ] \
    || [ ! ${executor_cores_val} ] \
    || [ ! ${executor_memory_val} ] \
    || [ ! ${extra_java_options_val} ] \
    || [ ! ${driver_cores_val} ] \
    || [ ! ${driver_memory_val} ] \
    || [ ! ${memory_overhead_val} ] \
    || [ ! ${master_val} ] \
    || [ ! ${cpu_name} ]; then
  echo "Some values are NULL, please confirm with the property files"
  exit 0
fi


source conf/ml/ml_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}
data_path_input=${!dataset_name}
data_path_output=${!dataset_output_}
data_path_val=${data_path_input}","${data_path_output}
echo "${dataset_name} : ${data_path_val}"
is_raw="yes"
echo "-Is raw? -${is_raw}"
hdfs dfs -rm -r -f "${data_path_output}_${cpu_name}_${is_raw}"

spark_conf=${master_val}_${deploy_mode_val}_${num_executors_val}_${executor_cores_val}_${executor_memory_val}

mkdir -p log

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

echo "start to submit spark jobs --- Pearson_raw-${model_conf}"
spark-submit \
--driver-class-path "lib/fastutil-8.3.1.jar:lib/snakeyaml-1.19.jar" \
--jars "lib/snakeyaml-1.19.jar,lib/fastutil-8.3.1.jar" \
--class com.bigdata.ml.PearsonRunner \
--deploy-mode ${deploy_mode_val} \
--driver-cores ${driver_cores_val} \
--driver-memory ${driver_memory_val} \
--num-executors ${num_executors_val} \
--executor-cores ${executor_cores_val} \
--executor-memory ${executor_memory_val} \
--master ${master_val} \
--conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
--conf "spark.executor.instances=${num_executors_val}" \
--conf "spark.executor.memoryOverhead=${memory_overhead_val}" \
--conf "spark.driver.maxResultSize=256G" \
./lib/kal-test_2.11-0.1.jar ${model_conf} ${data_path_val} ${cpu_name} ${is_raw} ${spark_conf} | tee ./log/log
