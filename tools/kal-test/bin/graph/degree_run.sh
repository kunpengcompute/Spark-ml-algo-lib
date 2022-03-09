#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <dataset name>"
  echo "1st argument: name of dataset: e.g. it_2004"
  echo "2nd argument: name of api: e.g. degrees,inDegrees,outDegrees"
  exit 0
  ;;
esac

if [ $# -ne 2 ];then
  echo "Usage:<dataset name><api name>"
 	echo "dataset name:it_2004,twitter7,uk_2007_05,mycielskian20,gap_kron,com_friendster"
  echo "api name:degrees,inDegrees,outDegrees"
	exit 0
fi

current_path=$(dirname $(readlink -f "$0"))
echo "current folder path: ${current_path}"

source conf/graph/degree/degree_spark.properties

dataset_name=$1
api_name=$2

if [ ${dataset_name} != "it_2004" ] &&
   [ ${dataset_name} != "twitter7" ] &&
   [ ${dataset_name} != "uk_2007_05" ] &&
   [ ${dataset_name} != "mycielskian20" ] &&
   [ ${dataset_name} != "gap_kron" ] &&
   [ ${dataset_name} != "com_friendster" ] ;then
  echo "invalid dataset name,dataset name:it_2004,twitter7,uk_2007_05,mycielskian20,gap_kron,com_friendster"
  exit 1
fi

if [ ${api_name} != "degrees" ] &&
   [ ${api_name} != "inDegrees" ] &&
   [ ${api_name} != "outDegrees" ];then
  echo "invalid api name,api name: degrees,inDegrees,outDegrees"
  exit 1
fi

hdfs dfs -rm -r -f "/tmp/graph/result/degree/${dataset_name}_${api_name}"

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

# concatnate strings as a new variable
num_executors="${api_name}_${dataset_name}_numExecutors_${cpu_name}"
executor_cores="${api_name}_${dataset_name}_executorCores_${cpu_name}"
executor_memory="${api_name}_${dataset_name}_executorMemory_${cpu_name}"
num_partitions="${api_name}_${dataset_name}_numPartitions_${cpu_name}"
deploy_mode="deployMode"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
deploy_mode_val=${!deploy_mode}
num_partitions_val=${!num_partitions}
extra_java_options_val="-Xms${executor_memory_val}"

echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${deploy_mode} : ${deploy_mode_val}"
echo "${num_partitions} : ${num_partitions_val}"
echo "extra_java_options_val : ${extra_java_options_val}"

if [ ! ${num_executors_val} ] ||
  [ ! ${executor_cores_val} ] ||
  [ ! ${executor_memory_val} ] ||
  [ ! ${extra_java_options_val} ] ||
  [ ! ${num_partitions_val} ]; then
  echo "Some values are NULL, please confirm with the property files"
  exit 0
fi

source conf/graph/graph_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}
data_path_val=${!dataset_name}
echo "${dataset_name} : ${data_path_val}"


echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

mkdir -p log

echo "start to submit spark jobs"
spark-submit \
--class com.bigdata.graph.DegreeRunner \
--master yarn \
--deploy-mode ${deploy_mode_val} \
--num-executors ${num_executors_val} \
--executor-memory ${executor_memory_val} \
--executor-cores ${executor_cores_val} \
--driver-memory 200g \
--conf spark.locality.wait.node=0 \
--conf spark.driver.maxResultSize=0 \
--conf spark.rpc.askTimeout=1000000s \
--conf spark.network.timeout=1000000s \
--conf spark.executor.heartbeatInterval=100000s \
--conf spark.rpc.message.maxSize=1000 \
--conf spark.broadcast.blockSize=1m \
--conf spark.reducer.maxSizeInFlight=59mb \
--conf spark.shuffle.file.buffer=17k \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.io.compression.codec=lzf \
--conf spark.shuffle.compress=true \
--conf spark.rdd.compress=false \
--conf spark.shuffle.io.preferDirectBufs=true \
--conf spark.shuffle.spill.compress=true \
--conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
--jars "lib/boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar,lib/boostkit-graph-acc_2.11-1.3.0-${spark_version_val}.jar,lib/fastutil-8.3.1.jar" \
--driver-class-path "lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar:lib/boostkit-graph-acc_2.11-1.3.0-${spark_version_val}.jar:lib/fastutil-8.3.1.jar" \
--conf "spark.executor.extraClassPath=boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar:boostkit-graph-acc_2.11-1.3.0-${spark_version_val}.jar:fastutil-8.3.1.jar" \
./lib/kal-test_2.11-0.1.jar ${dataset_name} ${api_name} ${num_partitions_val} "no" ${data_path_val} | tee ./log/log
