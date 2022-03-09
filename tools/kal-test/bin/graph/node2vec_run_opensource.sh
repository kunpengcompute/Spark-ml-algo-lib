#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <dataset name>"
  echo "1st argument: name of dataset: name of dataset: e.g. cit_patents"
  exit 0
  ;;
esac

if [ $# -ne 1 ];then
  echo "Usage:<dataset name>"
 	echo "dataset name:cit_patents,soc_liveJournal,uk_2002"
	exit 0
fi

current_path=$(dirname $(readlink -f "$0"))
echo "current folder path: ${current_path}"

source conf/graph/node2vec/node2vec_spark_opensource.properties

dataset_name=$1

if [ ${dataset_name} != "cit_patents" ] &&
   [ ${dataset_name} != "soc_liveJournal" ] &&
   [ ${dataset_name} != "uk_2002" ] ;then
  echo "invalid dataset name,dataset name:cit_patents,soc_liveJournal,uk_2002"
  exit 1
fi

outputPath=/tmp/graph/result/node2vec/${dataset_name}_"opensource"/${dataset_name}-"test.txt"
hdfs dfs -rm -r -f ${outputPath}

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')
model_conf=${dataset_name}-"opensource"
# concatnate strings as a new variable

master_="master"
deploy_mode="deployMode"
driver_memory="driverMemory"
driver_cores="driverCores"
executor_cores="executorCores"
executor_memory="executorMemory"
num_executors="numExecutors"

master_val=${!master_}
deploy_mode_val=${!deploy_mode}
num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
driver_memory_val=${!driver_memory}
driver_cores_val=${!driver_cores}

echo "${cpu_name}"
echo "${master_} : ${master_val}"
echo "${deploy_mode} : ${deploy_mode_val}"
echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${driver_memory}:${driver_memory_val}"
echo "${driver_cores}:${driver_cores_val}"


if [ ! ${num_executors_val} ] ||
  [ ! ${executor_cores_val} ] ||
  [ ! ${executor_memory_val} ] ||
  [ ! ${driver_memory_val} ] ||
  [ ! ${driver_cores_val} ] ; then
  echo "Some values are NULL, please confirm with the property files"
  exit 0
fi

source conf/graph/graph_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}
negEdge_path="${dataset_name}_negEdge"
data_path_val=${!dataset_name}
negEdge_path_val=${!negEdge_path}

echo "${dataset_name} : ${data_path_val}"

mkdir -p log

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

echo "start to submit spark jobs--node2vec_${model_conf}"
spark-submit \
--class vn.five9.Main \
--master ${master_val} \
--name "Node2Vec_${model_conf}" \
--deploy-mode ${deploy_mode_val} \
--num-executors ${num_executors_val} \
--executor-memory ${executor_memory_val} \
--executor-cores ${executor_cores_val} \
--driver-memory ${driver_memory_val} \
--driver-cores ${driver_cores_val} \
--conf spark.kryoserializer.buffer.max=2047m \
--conf spark.ui.showConsoleProgress=true \
--conf spark.driver.maxResultSize=0 \
--conf spark.driver.extraJavaOptions="-Xms300G -XX:hashCode=0" \
--conf spark.executor.extraJavaOption="-Xms315G -XX:hashCode=0" \
--conf spark.rpc.askTimeout=1000000s \
--conf spark.network.timeout=1000000s \
--conf spark.executor.heartbeatInterval=100000s \
--conf spark.rpc.message.maxSize=1000 \
--jars "lib/fastutil-8.3.1.jar,lib/spark-mllib_2.11-2.3.2.jar,lib/smile-core-2.5.3.jar,lib/scopt_2.11-3.5.0.jar,lib/smile-math-2.5.3.jar" \
./lib/node2vec-baseline.jar \
--cmd node2vec --indexed true --directed true --degree 1000000000 \
--p 1.0 --q 1.0 --walkLength 5 --numWalks 10 \
--input ${data_path_val} --output ${outputPath} > node2vec_tmp.log

CostTime=$(cat node2vec_tmp.log |grep "total time" | awk '{print $7}')
currentTime=$(date "+%Y%m%d_H%M%S")
rm -rf node2vec_tmp.log
echo -e "algorithmName: Node2vec\ncostTime: $CostTime\ndatasetName: ${dataset_name}\nisRaw: 'yes'\ntestcaseType: Node2vec_opensource_${dataset_name}\n" > ./report/"Node2vec_${currentTime}.yml"
if [ $? -eq 0 ];then
  echo "Exec Successful: end." > ./log/log
else
  echo "Exec Failure: please check the code" > ./log/log
fi
