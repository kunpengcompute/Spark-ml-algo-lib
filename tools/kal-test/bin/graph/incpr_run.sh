#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <dataset name>"
  echo "1st argument: name of dataset: e.g. cit_patents"
  echo "2nd argument: name of api: e.g. run"
  exit 0
  ;;
esac

if [ $# -ne 2 ];then
  echo "Usage:<dataset name><api name>"
 	echo "dataset name: twitter-2010"
  echo "api name:run or runUntilConvergence"
	exit 0
fi

current_path=$(dirname $(readlink -f "$0"))
echo "current folder path: ${current_path}"

source conf/graph/incpr/incpr_spark.properties

dataset_name=$1
api_name=$2

input_path=${basic_input_path}
output_path=${basic_output_path}
echo ${input_path}
echo ${output_path}
exit 0
result=$(echo ${dataset_name} | grep "twitter_2010")
if [[ "$result" != "" ]]
then
    echo "correct dataset name ${dataset_name}, continue kal test."
else
    echo "invalid dataset name ${dataset_name}, expect twitter-2010*."
    exit 1
fi

if [ ${api_name} != "run" ] && [ ${api_name} != "runUntilConvergence" ];then
  echo "invalid api name ${api_name}, expect run or runUntilConvergence"
  exit 1
fi

hdfs dfs -rm -r -f "/tmp/graph/result/incpr/${dataset_name}_${api_name}"

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

prefix="run"
if [ ${api_name} == "runUntilConvergence" ]
then
  prefix="convergence"
fi

# concatnate strings as a new variable
# concatnate strings as a new variable
num_executors="numExectuors_"${cpu_name}
executor_cores="executorCores_"${cpu_name}
executor_memory="executorMemory_"${cpu_name}
extra_java_options="extraJavaOptions_"${cpu_name}
driver_cores="driverCores_"${cpu_name}
driver_memory="driverMemory_"${cpu_name}
executor_memory_overhead="execMemOverhead_"${cpu_name}
master_="master"
deploy_mode="deployMode"
num_partitions="numPartitions_"${cpu_name}
echo $num_executors
echo $executor_cores
echo $executor_memory
echo $extra_java_options
echo $num_partitions
deploy_mode="deployMode"

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

echo "JuTaoBB 94"
source conf/graph/graph_datasets.properties
echo "JuTaoBB 96"
spark_version=sparkVersion
spark_version_val=${!spark_version}
echo "JuTaoBB 99"
data_path_val=${!dataset_name}
echo "JuTaoBB 101"
kal_version=kalVersion
kal_version_val=${!kal_version}
#echo "${dataset_name} : ${data_path_val}"


echo "start to submit spark jobs"
echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

mkdir -p log
spark-submit \
--class com.bigdata.graph.IncPageRankRunner \
--master yarn \
--deploy-mode ${deploy_mode_val} \
--num-executors ${num_executors_val} \
--executor-memory ${executor_memory_val} \
--executor-cores ${executor_cores_val} \
--driver-memory 100g \
--conf spark.driver.maxResultSize=200g \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.kryoserializer.buffer.max=2040m \
--conf spark.driver.extraJavaOptions="-Xms100G" \
--conf spark.rpc.askTimeout=1000000s \
--conf spark.locality.wait.node=0 \
--conf spark.network.timeout=1000000s \
--conf spark.executor.heartbeatInterval=100000s \
--conf spark.rpc.message.maxSize=1000 \
--conf spark.memory.fraction=0.5 \
--conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
--jars "lib/fastutil-8.3.1.jar,lib/boostkit-graph-kernel-2.11-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
--driver-class-path "lib/kal-test_2.11-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-2.11-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
--conf "spark.executor.extraClassPath=fastutil-8.3.1.jar:boostkit-graph-kernel-2.11-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
./lib/kal-test_2.11-0.1.jar ${dataset_name} ${api_name} "no" | tee ./log/log

