#!/bin/bash
set -e

case "$1" in 
-h | --help | ?)
 echo "Usage:<dataset name>"
 echo "dataset name:graph500_22,or graph500_23,or graph500_24,or graph500_25,or graph500_26"
 echo "api name:run or preCanonical"
 exit 0
 ;;
esac

if [ $# -ne 2 ];then
  echo "Usage:<dataset name><api name>"
 	echo "dataset name:graph500_22,or graph500_23,or graph500_24,or graph500_25,or graph500_26"
  echo "api name:run or preCanonical"
	exit 0
fi
dataset_name=$1
api_name=$2
cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

BIN_DIR="$(cd "$(dirname "$0")";pwd)"
CONF_DIR="${BIN_DIR}/../../conf/graph"
source ${CONF_DIR}/tc/tc_spark.properties
num_executors_val="numExecutors_${cpu_name}"
executor_cores_val="executorCores"
executor_memory_val="executorMemory_${cpu_name}"
extra_java_options_val="extraJavaOptions_${cpu_name}"
master_val="master"
deploy_mode_val="deployMode"
driver_cores_val="driverCores"
driver_memory_val="driverMemory"
num_executors=${!num_executors_val}
executor_cores=${!executor_cores_val}
executor_memory=${!executor_memory_val}
extra_java_options=${!extra_java_options_val}
master=${!master_val}
deploy_mode=${!deploy_mode_val}
driver_cores=${!driver_cores_val}
driver_memory=${!driver_memory_val}
if [ ! ${num_executors} ] \
	|| [ ! ${executor_cores} ] \
  || [ ! ${executor_memory} ] \
	|| [ ! ${deploy_mode} ] \
	|| [ ! ${driver_cores} ] \
	|| [ ! ${driver_memory} ] \
	|| [ ! ${master} ]; then
   echo "Some values are NUll,please confirm with the property files"
   exit 0
fi
echo "${master_val}:${master}"
echo "${deploy_mode_val}:${deploy_mode}"
echo "${num_executors_val}:${num_executors}"
echo "${executor_cores_val}:${executor_cores}"
echo "${executor_memory_val}:${executor_memory}"
echo "${extra_java_options_val}:${extra_java_options}"
echo "${driver_memory_val}:${driver_memory}"
echo "${driver_cores_val}:${driver_cores}"

source ${CONF_DIR}/graph_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}
input_path=${!dataset_name}
output_path_val="${dataset_name}_tc"
output_path=${!output_path_val}
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
--class com.bigdata.graph.TriangleCountRunner \
--deploy-mode ${deploy_mode} \
--driver-cores ${driver_cores} \
--driver-memory ${driver_memory} \
--num-executors ${num_executors} \
--executor-cores ${executor_cores} \
--executor-memory ${executor_memory} \
--conf "spark.executor.extraJavaOptions=${extra_java_options} -XX:SurvivorRatio=4 -XX:ParallelGCThreads=6" \
--conf spark.rpc.askTimeout=36000 \
--conf spark.akka.timeout=3600 \
--conf spark.scheduler.maxRegisteredResourcesWaitingTime=3600000 \
--conf spark.worker.timeout=3600 \
--conf spark.network.timeout=6000s \
--conf spark.storage.blockManagerSlaveTimeoutMs=600000 \
--conf spark.shuffle.blockTransferService=nio \
--conf spark.driver.maxResultSize=100g \
--conf spark.shuffle.manager=SORT \
--conf spark.broadcast.blockSize=25g \
--conf spark.akka.frameSize=2046 \
--conf spark.core.connection.ack.wait.timeout=60000s \
--conf spark.storage.memoryFraction=0.2 \
--conf spark.shuffle.memoryFraction=0.6 \
--conf spark.rdd.compress=true \
--jars "lib/boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
--driver-class-path "lib/kal-test_2.11-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
--conf "spark.executor.extraClassPath=boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
./lib/kal-test_2.11-0.1.jar ${dataset_name} ${input_path} ${output_path} ${api_name} | tee ./log/log





