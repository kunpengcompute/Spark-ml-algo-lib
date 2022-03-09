#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <dataset name> <algorithm type>"
  echo "1rd argument: name of dataset: e.g. higgs"
  echo "2st argument: type of algorithm: [classification/regression]"
  exit 0
  ;;
esac

if [ $# -ne 2 ]; then
  echo "please input 2 arguments: <dataset name> <algorithm type>"
  echo "1rd argument: name of dataset: e.g. higgs"
  echo "2st argument: type of algorithm: [classification/regression]"
  exit 0
fi


source conf/ml/xgbt/xgbt_spark_raw.properties

dataset_name=$1
algorithm_type=$2

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')



model_conf=${algorithm_type}_${dataset_name}

# concatnate strings as a new variable
num_executors=${cpu_name}_${algorithm_type}"_"${dataset_name}"_numExecutors"
executor_cores=${cpu_name}_${algorithm_type}"_"${dataset_name}"_executorCores"
executor_memory=${cpu_name}_${algorithm_type}"_"${dataset_name}"_executorMemory"
extra_java_options=${cpu_name}_${algorithm_type}"_"${dataset_name}"_extraJavaOptions"
driver_cores=${cpu_name}_${algorithm_type}"_"${dataset_name}"_driverCores"
driver_memory=${cpu_name}_${algorithm_type}"_"${dataset_name}"_driverMemory"
task_cpus=${cpu_name}_${algorithm_type}"_"${dataset_name}"_taskCpus"
num_partitions=${cpu_name}_${algorithm_type}"_"${dataset_name}"_numPartitions"
master_="master"
deploy_mode="deployMode"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
extra_java_options_val=${!extra_java_options}
driver_cores_val=${!driver_cores}
driver_memory_val=${!driver_memory}
task_cpus_val=${!task_cpus}
num_partitions_val=${!num_partitions}
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
echo "${task_cpus} : ${task_cpus_val}"
echo "${num_partitions} : ${num_partitions_val}"
echo "cpu_name : ${cpu_name}"

if [ ! ${num_executors_val} ] \
    || [ ! ${executor_cores_val} ] \
    || [ ! ${executor_memory_val} ] \
    || [ ! ${extra_java_options_val} ] \
    || [ ! ${driver_cores_val} ] \
    || [ ! ${driver_memory_val} ] \
    || [ ! ${master_val} ] \
    || [ ! ${task_cpus_val} ] \
    || [ ! ${num_partitions_val} ] \
    || [ ! ${deploy_mode_val} ] \
    || [ ! ${cpu_name} ]; then
  echo "Some values are NULL, please confirm with the property files"
  exit 0
fi


source conf/ml/ml_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}
data_path_val=${!dataset_name}
echo "${dataset_name} : ${data_path_val}"
is_raw="yes"
echo "-Is raw? -${is_raw}"
echo "start to submit spark jobs"

spark_conf=${master_val}_${deploy_mode_val}_${num_executors_val}_${executor_cores_val}_${executor_memory_val}_${extra_java_options_val}_${driver_cores_val}_${driver_memory_val}_${task_cpus_val}_${num_partitions_val}

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

mkdir -p log
spark-submit \
--class com.bigdata.ml.XGBTRunner \
--deploy-mode ${deploy_mode_val} \
--driver-cores ${driver_cores_val} \
--driver-memory ${driver_memory_val} \
--num-executors ${num_executors_val} \
--executor-cores ${executor_cores_val} \
--executor-memory ${executor_memory_val} \
--master ${master_val} \
--conf spark.task.cpus=${task_cpus_val} \
--conf spark.executor.extraJavaOptions=${extra_java_options_val} \
--jars "lib/xgboost4j_2.11-1.1.0.jar,lib/xgboost4j-spark_2.11-1.1.0.jar,lib/snakeyaml-1.19.jar,lib/kal-test_2.11-0.1.jar" \
--conf spark.executor.extraClassPath="xgboost4j_2.11-1.1.0.jar:xgboost4j-spark_2.11-1.1.0.jar:snakeyaml-1.19.jar" \
--conf spark.driver.extraClassPath="lib/kal-test_2.11-0.1.jar:lib/xgboost4j_2.11-1.1.0.jar:lib/xgboost4j-spark_2.11-1.1.0.jar:lib/snakeyaml-1.19.jar" \
./lib/kal-test_2.11-0.1.jar ${model_conf} ${data_path_val} ${cpu_name} ${is_raw} ${spark_conf} | tee ./log/log
