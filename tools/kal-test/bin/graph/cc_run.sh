#!/bin/bash
set -e

case "$1" in 
-h | --help | ?)
 echo "Usage: <dataset name>"
 echo "dataset name: graph500_25 or graph500_26 or liveJournal"
 exit 0
 ;;
esac

if [ $# -ne 1 ];then
	echo "please input dataset name."
  echo "dataset name: graph500_25 or graph500_26 or liveJournal"
	exit 0
fi

dataset_name=$1
if [ $dataset_name != 'graph500_25' ] && [ $dataset_name != 'graph500_26' ] && [ $dataset_name != 'liveJournal' ];
then
  echo 'invalid dataset'
  echo "dataset name: graph500_25 or graph500_26 or liveJournal"
  exit 0
fi

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

BIN_DIR="$(cd "$(dirname "$0")";pwd)"
CONF_DIR="${BIN_DIR}/../../conf/graph"
source ${CONF_DIR}/cc/cc_spark.properties
num_executors_val="numExecutors_${cpu_name}"
executor_cores_val="executorCores_${cpu_name}"
executor_memory_val="executorMemory_${cpu_name}"
executor_extra_javaopts_val="executorExtraJavaopts_${cpu_name}"
default_parallelism_val="defaultParallelism_${dataset_name}_${cpu_name}"

master_val="master"
deploy_mode_val="deployMode"
driver_memory_val="driverMemory"
num_executors=${!num_executors_val}
executor_cores=${!executor_cores_val}
executor_memory=${!executor_memory_val}
master=${!master_val}
driver_memory=${!driver_memory_val}
deploy_mode=${!deploy_mode_val}
executor_extra_javaopts=${!executor_extra_javaopts_val}
default_parallelism=${!default_parallelism_val}
if [ ! ${num_executors} ] \
	|| [ ! ${executor_cores} ] \
  || [ ! ${executor_memory} ] \
	|| [ ! ${master} ]; then
   echo "Some values are NUll, please confirm with the property files"
   exit 0
fi
echo "${master_val}:${master}"
echo "${deploy_mode_val}:${deploy_mode}"
echo "${num_executors_val}:${num_executors}"
echo "${executor_cores_val}:${executor_cores}"
echo "${executor_memory_val}:${executor_memory}"
echo "${executor_extra_javaopts_val}:${executor_extra_javaopts}"
echo "${default_parallelism_val}:${default_parallelism}"

source ${CONF_DIR}/graph_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}
input_path=${!dataset_name}
output_path="${output_path_prefix}/cc/${dataset_name}"
echo "${dataset_name}: ${input_path},${output_path}"

echo "start to clean exist output"
hdfs dfs -rm -r -f -skipTrash ${output_path}

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

mkdir -p log

echo "start to submit spark jobs"

spark-submit \
--class com.bigdata.graph.ConnectedComponentsRunner \
--deploy-mode ${deploy_mode} \
--driver-memory ${driver_memory} \
--num-executors ${num_executors} \
--executor-cores ${executor_cores} \
--executor-memory ${executor_memory} \
--conf "spark.executor.extraJavaOptions=${executor_extra_javaopts}" \
--conf spark.rpc.message.maxSize=1000 \
--conf spark.driver.maxResultSize=100g \
--conf spark.locality.wait.node=0 \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.kryoserializer.buffer=48m \
--conf spark.default.parallelism=${default_parallelism} \
--driver-class-path "lib/kal-test_2.11-0.1.jar:lib/fastutil-8.3.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
--jars "lib/fastutil-8.3.1.jar,lib/boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
--conf spark.executor.extraClassPath="fastutil-8.3.1.jar:boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
./lib/kal-test_2.11-0.1.jar ${dataset_name} ${input_path} ${output_path} "no" ${cpu_name} | tee ./log/log
