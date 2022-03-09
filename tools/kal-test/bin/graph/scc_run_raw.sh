#!/bin/bash
set -e

case "$1" in 
-h | --help | ?)
 echo "Usage:<dataset name>"
 echo "dataset name: enwiki_2018 or arabic_2005 or cit_patents"
 exit 0
 ;;
esac

if [ $# -ne 1 ];then
	echo "please input dataset name: enwiki_2018 or arabic_2005 or cit_patents"
	exit 0
fi
dataset_name=$1
cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

BIN_DIR="$(cd "$(dirname "$0")";pwd)"
CONF_DIR="${BIN_DIR}/../../conf/graph"

num_executors_val="numExecutors_${cpu_name}"
executor_cores_val="executorCores_${cpu_name}"
executor_memory_val="executorMemory_${cpu_name}"
master_val="master"
deploy_mode_val="deployMode"
driver_memory_val="driverMemory"
source ${CONF_DIR}/scc/scc_spark.properties
num_executors=${!num_executors_val}
executor_cores=${!executor_cores_val}
executor_memory=${!executor_memory_val}
master=${!master_val}
driver_memory=${!driver_memory_val}
deploy_mode=${!deploy_mode_val}
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
echo "executor_extra_javaopts:${executor_extra_javaopts}"

source ${CONF_DIR}/graph_datasets.properties
input_path=${!dataset_name}
output_path="${output_path_prefix}/scc/${dataset_name}"
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
--class com.bigdata.graph.StronglyConnectedComponentsRunner \
--deploy-mode ${deploy_mode} \
--driver-memory ${driver_memory} \
--num-executors ${num_executors} \
--executor-cores ${executor_cores} \
--executor-memory ${executor_memory} \
--conf spark.locality.wait.node=0 \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.driver.maxResultSize=100g \
--conf spark.ui.showConsoleProgress=false \
--conf spark.driver.extraJavaOptions="-Xms12g -XX:hashCode=0" \
--conf spark.executor.extraJavaOptions="${executor_extra_javaopts}" \
--conf spark.rpc.askTimeout=1000000s \
--conf spark.network.timeout=1000000s \
--conf spark.executor.heartbeatInterval=100000s \
--conf spark.rpc.message.maxSize=1000 \
--conf spark.memory.fraction=0.24939583270092516 \
--conf spark.memory.storageFraction=0.5849745294783253 \
--conf spark.broadcast.blockSize=1m \
--conf spark.reducer.maxSizeInFlight=59mb \
--conf spark.shuffle.file.buffer=17k \
--conf spark.io.compression.codec=lzf \
--conf spark.shuffle.compress=true \
--conf spark.rdd.compress=false \
--conf spark.shuffle.io.preferDirectBufs=true \
--conf spark.shuffle.spill.compress=true \
--driver-class-path "lib/kal-test_2.11-0.1.jar:lib/snakeyaml-1.19.jar" \
./lib/kal-test_2.11-0.1.jar ${dataset_name} ${input_path} ${output_path} "run" "yes" ${cpu_name} | tee ./log/log