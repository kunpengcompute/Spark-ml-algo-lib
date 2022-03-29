#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <dataset name>"
  echo "1st argument: name of dataset: e.g. cage14"
  echo "2nd argument: name of api: e.g. static"
  exit 0
  ;;
esac

if [ $# -ne 2 ];then
  echo "Usage:<dataset name><api name>"
 	echo "dataset name:GAP_road or cage14"
  echo "api name:static  or convergence"
	exit 0
fi

current_path=$(dirname $(readlink -f "$0"))
echo "current folder path: ${current_path}"
BIN_DIR="$(cd "$(dirname "$0")";pwd)"
CONF_DIR="${BIN_DIR}/../../conf/graph"
source conf/graph/wpr/wpr_spark.properties

dataset_name=$1
api_name=$2

if [ ${dataset_name} != "GAP_road" ] && [ ${dataset_name} != "cage14" ];then
  echo "invalid dataset name,dataset name:GAP_road,or cage14"
  exit 1
fi
if [ ${api_name} != "static" ] && [ ${api_name} != "convergence" ];then
  echo "invalid api name,api name: static or convergence"
  exit 1
fi

hdfs dfs -rm -r -f "/tmp/graphhData/wpr/output"

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

prefix="run"
if [ ${api_name} == "runUntilConvergence" ]
then
  prefix="convergence"
fi

# concatnate strings as a new variable
num_executors="numExectuors_"${cpu_name}
executor_cores="executorCores_"${cpu_name}
executor_memory="executorMemory_"${cpu_name}
extra_java_options="extraJavaOptions_"${cpu_name}
driver_cores="driverCores_"${cpu_name}
driver_memory="driverMemory_"${cpu_name}
executor_memory_overhead="execMemOverhead_"${cpu_name}
num_partitions="numPartitions_"${cpu_name}
master_="master"
deploy_mode="deployMode"
echo $num_executors
echo $executor_cores
echo $executor_memory
echo $extra_java_options
echo $num_partitions

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
extra_java_options_val=${!extra_java_options}
deploy_mode_val=${!deploy_mode}
num_partitions_val=${!num_partitions}

echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${extra_java_options} : ${extra_java_options_val}"
echo "${deploy_mode} : ${deploy_mode_val}"
echo "${num_partitions} : ${num_partitions_val}"

if [ ! ${num_executors_val} ] ||
  [ ! ${executor_cores_val} ] ||
  [ ! ${executor_memory_val} ] ||
  [ ! ${extra_java_options_val} ] ||
  [ ! ${num_partitions_val} ]; then
  echo "Some values are NULL, please confirm with the property files"
  exit 0
fi

source conf/graph/graph_datasets.properties
kal_version=kalVersion
kal_version_val=${!kal_version}

spark_version=sparkVersion
spark_version_val=${!spark_version}
data_path_val=${!dataset_name}
echo "${dataset_name} : ${data_path_val}"


echo "start to submit spark jobs"
echo "start to clean cache and sleep 3s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 3

mkdir -p log
spark-submit \
--class com.bigdata.graph.WeightedPageRankRunner \
--master yarn \
--deploy-mode ${deploy_mode_val} \
--num-executors ${num_executors_val} \
--executor-memory ${executor_memory_val} \
--executor-cores ${executor_cores_val} \
--driver-memory 100g \
--conf spark.driver.maxResultSize=200g \
--conf spark.driver.extraJavaOptions="-Xms100G" \
--conf spark.locality.wait.node=0 \
--conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
--conf spark.shuffle.blockTransferService=nio \
--jars "lib/fastutil-8.3.1.jar,lib/boostkit-graph-kernel-2.11-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
--driver-class-path "lib/kal-test_2.11-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-2.11-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
--conf "spark.executor.extraClassPath=fastutil-8.3.1.jar:boostkit-graph-kernel-2.11-${kal_version_val}-{$spark_version_val}-${cpu_name}.jar" \
./lib/kal-test_2.11-0.1.jar ${dataset_name} ${api_name} "no" | tee ./log/log

