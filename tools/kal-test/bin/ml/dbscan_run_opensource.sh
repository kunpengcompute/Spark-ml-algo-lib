#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <dataset name>"
  echo "argument: name of dataset: e.g. bremenSmall/farm/house"
  exit 0
  ;;
esac

if [ $# -ne 1 ]; then
  echo "please input 1 arguments: <dataset name>"
  echo "argument: name of dataset: e.g. bremenSmall/farm/house"
  exit 0
fi

source conf/ml/dbscan/dbscan_spark_opensource.properties

outputPath="hdfs:///tmp/ml/result/dbscan/alitoukaDBSCAN/output_${dataset_name}"
hdfsJarPath="hdfs:///tmp/ml/test/dbscan"

hdfs dfs -rm -r -f ${outputPath}
hdfs dfs -mkdir -p ${hdfsJarPath}
hdfs dfs -ls ${hdfsJarPath}
if [ $? -eq 0 ];then
  hdfs dfs -rm -r -f ${hdfsJarPath}/alitouka_dbscan_2.11-0.1.jar
  hdfs dfs -put ./lib/alitouka_dbscan_2.11-0.1.jar ${hdfsJarPath}
fi

dataset_name=$1
platform_name=$(lscpu | grep Architecture | awk '{print $2}')

model_conf=${dataset_name}-"opensource"

# concatnate strings as a new variable
num_executors="numExectuors_"${platform_name}
executor_cores="executorCores_"${platform_name}
executor_memory="executorMemory_"${platform_name}
extra_java_options="extraJavaOptions_"${platform_name}
driver_max_result_size="driverMaxResultSize"
driver_cores="driverCores_"${platform_name}
driver_memory="driverMemory_"${platform_name}
master_="master"
deploy_mode="deployMode"
epsilon="epsilon_"${dataset_name}
min_points="minPoints_"${dataset_name}

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
extra_java_options_val=${!extra_java_options}
driver_max_result_size_val=${!driver_max_result_size}
driver_cores_val=${!driver_cores}
driver_memory_val=${!driver_memory}
master_val=${!master_}
deploy_mode_val=${!deploy_mode}
epsilon_val=${!epsilon}
min_points_val=${!min_points}

echo ${platform_name}
echo "${master_} : ${master_val}"
echo "${deploy_mode} : ${deploy_mode_val}"
echo "${driver_cores} : ${driver_cores_val}"
echo "${driver_memory} : ${driver_memory_val}"
echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${extra_java_options} : ${extra_java_options_val}"
echo "${driver_max_result_size} : ${driver_max_result_size_val}"
echo "${epsilon} : ${epsilon_val}"
echo "${min_points} : ${min_points_val}"


if [ ! ${num_executors_val} ] \
    || [ ! ${executor_cores_val} ] \
    || [ ! ${executor_memory_val} ] \
    || [ ! ${driver_max_result_size_val} ] \
    || [ ! ${extra_java_options_val} ] \
    || [ ! ${driver_cores_val} ] \
    || [ ! ${driver_memory_val} ] \
    || [ ! ${epsilon_val} ] \
    || [ ! ${min_points_val} ] \
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
echo "start to submit spark jobs --- DBSCAN-${model_conf}"

spark-submit \
--jars "lib/scopt_2.11-3.5.0.jar" \
--class org.alitouka.spark.dbscan.DbscanDriver \
--deploy-mode ${deploy_mode_val} \
--name "alitouka_DBSCAN_${model_conf}" \
--driver-cores ${driver_cores_val} \
--driver-memory ${driver_memory_val} \
--num-executors ${num_executors_val} \
--executor-cores ${executor_cores_val} \
--executor-memory ${executor_memory_val} \
--master ${master_val} \
--conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
--conf "spark.driver.maxResultSize=${driver_max_result_size_val}" \
${hdfsJarPath}/alitouka_dbscan_2.11-0.1.jar --ds-master ${master_val} --ds-jar ${hdfsJarPath}/alitouka_dbscan_2.11-0.1.jar --ds-input ${data_path} --ds-output ${outputPath} --eps ${epsilon_val} --numPts ${min_points_val} >dbscan_tmp.log
CostTime=$(cat dbscan_tmp.log | grep "train total" | awk '{print $3}')
currentTime=$(date "+%Y%m%d_%H%M%S")
rm -rf dbscan_tmp.log
echo -e "algorithmName: DBSCAN\ncostTime: ${CostTime}\ndatasetName: ${dataset_name}\nisRaw: 'yes'\ntestcaseType: DBSCAN_opensource_${dataset_name}\n" > ./report/"DBSCAN_${currentTime}.yml"
echo "Exec Successful: costTime: ${CostTime}" > ./log/log
