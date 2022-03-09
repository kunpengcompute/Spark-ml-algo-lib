#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <dataset name>"
  echo "1st argument: name of dataset: e.g. cit_patents"
  exit 0
  ;;
esac

if [ $# -ne 1 ];then
  echo "Usage:<dataset name>"
 	echo "dataset name:cit_patents,enwiki_2018,arabic_2005,graph500_22,graph500_23,graph500_25"
	exit 0
fi

current_path=$(dirname $(readlink -f "$0"))
echo "current folder path: ${current_path}"

source conf/graph/bfs/bfs_spark.properties
source conf/graph/bfs/bfs_source_id.properties

dataset_name=$1

if [ ${dataset_name} != "cit_patents" ] &&
   [ ${dataset_name} != "enwiki_2018" ] &&
   [ ${dataset_name} != "arabic_2005" ] &&
   [ ${dataset_name} != "graph500_22" ] &&
   [ ${dataset_name} != "graph500_23" ] &&
   [ ${dataset_name} != "graph500_25" ] ;then
  echo "invalid dataset name,dataset name:cit_patents,enwiki_2018,arabic_2005,graph500_22,graph500_23,graph500_25"
  exit 1
fi

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

# concatnate strings as a new variable
num_executors="${dataset_name}_numExecutors_${cpu_name}"
executor_cores="${dataset_name}_executorCores_${cpu_name}"
executor_memory="${dataset_name}_executorMemory_${cpu_name}"
num_partitions="${dataset_name}_numPartitions_${cpu_name}"
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

source_ids="${dataset_name}_SourceID"
source_ids_val=${!source_ids}
IFS=,
source_ids_arr=($source_ids_val)

echo "${source_ids}:${source_ids_val}"
for source_id in ${source_ids_arr[@]}
do
  hdfs dfs -rm -r -f "/tmp/graph/result/bfs/${dataset_name}_${source_id}"

  echo "start to clean cache and sleep 30s"
  ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
  ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
  ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
  ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
  sleep 30

  mkdir -p log

  echo "start to submit spark jobs--${dataset_name}_${source_id}"
  spark-submit \
  --class com.bigdata.graph.BFSRunner \
  --master yarn \
  --deploy-mode ${deploy_mode_val} \
  --num-executors ${num_executors_val} \
  --executor-memory ${executor_memory_val} \
  --executor-cores ${executor_cores_val} \
  --driver-memory 200g \
  --conf "spark.driver.maxResultSize=200g" \
  --conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
  --conf "spark.driver.extraJavaOptions=-Xms200g" \
  --conf spark.locality.wait.node=0 \
  --jars "lib/boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
  --driver-class-path "lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
  --conf "spark.executor.extraClassPath=boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
  ./lib/kal-test_2.11-0.1.jar ${dataset_name} ${source_id} ${num_partitions_val} "no" ${data_path_val} | tee ./log/log
done
