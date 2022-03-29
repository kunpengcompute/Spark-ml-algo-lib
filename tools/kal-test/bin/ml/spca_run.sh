#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <dataset name>"
  echo "1st argument: name of dataset: e.g. Kemelmacher"
  echo "If you want to save the results, plz change the configuration [saveRes] in <spca.yml>"
  exit 0
  ;;
esac

if [ $# -ne 1 ]; then
  echo "please input 1 argument: <dataset name>"
  echo "1st argument: name of dataset: e.g. Kemelmacher"
  echo "If you want to save the results, plz change the configuration <saveRes>"
  exit 0
fi


source conf/ml/spca/spca_spark.properties

dataset_name=$1
cpu_name=$(lscpu | grep Architecture | awk '{print $2}')
model_conf=${dataset_name}

# concatnate strings as a new variable
num_executors="numExectuors_"${dataset_name}_${cpu_name}
executor_cores="executorCores_"${dataset_name}_${cpu_name}
executor_memory="executorMemory_"${dataset_name}_${cpu_name}
extra_java_options="extraJavaOptions_"${dataset_name}_${cpu_name}
driver_cores="driverCores_"${dataset_name}_${cpu_name}
driver_memory="driverMemory_"${dataset_name}_${cpu_name}
memory_overhead="execMemOverhead_"${dataset_name}_${cpu_name}
master_="master"
deploy_mode="deployMode"
compress_="compress"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
extra_java_options_val=${!extra_java_options}
driver_cores_val=${!driver_cores}
driver_memory_val=${!driver_memory}
memory_overhead_val=${!memory_overhead}
master_val=${!master_}
deploy_mode_val=${!deploy_mode}
compress_val=${!compress_}


echo "${master_} : ${master_val}"
echo "${deploy_mode} : ${deploy_mode_val}"
echo "${driver_cores} : ${driver_cores_val}"
echo "${driver_memory} : ${driver_memory_val}"
echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${extra_java_options} : ${extra_java_options_val}"
echo "${memory_overhead} : ${memory_overhead_val}"
echo "${compress_} : ${compress_val}"
echo "cpu_name : ${cpu_name}"

if [ ! ${num_executors_val} ] \
    || [ ! ${executor_cores_val} ] \
    || [ ! ${executor_memory_val} ] \
    || [ ! ${extra_java_options_val} ] \
    || [ ! ${driver_cores_val} ] \
    || [ ! ${driver_memory_val} ] \
    || [ ! ${master_val} ] \
    || [ ! ${memory_overhead_val} ] \
    || [ ! ${compress_val} ] \
    || [ ! ${cpu_name} ]; then
  echo "Some values are NULL, please confirm with the property files"
  exit 0
fi


source conf/ml/ml_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}
data_path_input=${!dataset_name}
echo "${dataset_name} : ${data_path_input}"
is_raw="no"
echo "-Is raw? -${is_raw}"

spark_conf=${master_val}_${deploy_mode_val}_${num_executors_val}_${executor_cores_val}_${executor_memory_val}

mkdir -p log
echo "start to submit spark jobs --- SPCA-${model_conf}"

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

spark-submit \
--class com.bigdata.ml.SPCARunner \
--deploy-mode ${deploy_mode_val} \
--driver-cores ${driver_cores_val} \
--driver-memory ${driver_memory_val} \
--num-executors ${num_executors_val} \
--executor-cores ${executor_cores_val} \
--executor-memory ${executor_memory_val} \
--master ${master_val} \
--conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
--conf "spark.executor.instances=${num_executors_val}" \
--conf "spark.executor.memory_overhead=${memory_overhead_val}" \
--conf "spark.driver.maxResultSize=256G" \
--conf "spark.rdd.compress=${compress_val}" \
--jars "lib/fastutil-8.3.1.jar,lib/boostkit-ml-acc_2.11-2.1.0-${spark_version_val}.jar,lib/boostkit-ml-core_2.11-2.1.0-${spark_version_val}.jar,lib/boostkit-ml-kernel-2.11-2.1.0-${spark_version_val}-${cpu_name}.jar" \
--driver-class-path "lib/kal-test_2.11-0.1.jar:lib/fastutil-8.3.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-ml-acc_2.11-2.1.0-${spark_version_val}.jar:lib/boostkit-ml-core_2.11-2.1.0-${spark_version_val}.jar:lib/boostkit-ml-kernel-2.11-2.1.0-${spark_version_val}-${cpu_name}.jar" \
--conf "spark.executor.extraClassPath=fastutil-8.3.1.jar:boostkit-ml-acc_2.11-2.1.0-${spark_version_val}.jar:boostkit-ml-core_2.11-2.1.0-${spark_version_val}.jar:boostkit-ml-kernel-2.11-2.1.0-${spark_version_val}-${cpu_name}.jar" \
./lib/kal-test_2.11-0.1.jar ${model_conf} ${data_path_input} ${cpu_name} ${is_raw} ${spark_conf} | tee ./log/log
