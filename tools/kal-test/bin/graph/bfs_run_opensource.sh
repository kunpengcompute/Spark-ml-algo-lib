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

source conf/graph/bfs/bfs_spark_opensource.properties
source conf/graph/bfs/bfs_source_id.properties
source conf/graph/bfs/bfs_opensource.properties

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

# concatnate strings as a new variable
num_executors="${dataset_name}_numExecutors"
executor_cores="${dataset_name}_executorCores"
executor_memory="${dataset_name}_executorMemory"
num_partitions="${dataset_name}_numPartitions"
deploy_mode="deployMode"
split="${dataset_name}_split"
q="${dataset_name}_q"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
deploy_mode_val=${!deploy_mode}
num_partitions_val=${!num_partitions}
extra_java_options_val="-Xms${executor_memory_val}"
split_val=${!split}
q_val=${!q}

echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${deploy_mode} : ${deploy_mode_val}"
echo "${num_partitions} : ${num_partitions_val}"
echo "extra_java_options_val : ${extra_java_options_val}"
echo "${split}: ${split_val}"
echo "${q}: ${q_val}"

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
  hdfs dfs -rm -r -f "/tmp/graph/result/bfs/${dataset_name}_opensource_${source_id}"

  echo "start to clean cache and sleep 30s"
  ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
  ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
  ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
  ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
  sleep 30

  mkdir -p log

  echo "start to submit spark jobs--${dataset_name}_${source_id}"
  spark-submit \
  --class Driver \
  --master yarn \
  --deploy-mode ${deploy_mode_val} \
  --num-executors ${num_executors_val} \
  --executor-memory ${executor_memory_val} \
  --executor-cores ${executor_cores_val} \
  --driver-memory 80g \
  --conf "spark.driver.maxResultSize=80g" \
  --conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
  --conf "spark.driver.extraJavaOptions=-Xms80g" \
  --conf spark.locality.wait.node=0 \
  --jars "lib/scopt_2.10-3.5.0.jar" \
  --driver-class-path "lib/scopt_2.10-3.5.0.jar" \
  --conf "spark.executor.extraClassPath=scopt_2.10-3.5.0.jar" \
  ./lib/bfs_2.10-0.1.2.jar \
  -g 'EdgeList' \
  -p 'EdgePartition2D' \
  -n ${num_partitions_val} \
  -i ${source_id} \
  -d 10 \
  -l 'yarn' \
  -f "${data_path_val}" \
  -t "${split_val}" \
  -o "/tmp/graph/result/bfs/${dataset_name}_opensource_${source_id}" \
  -z ${num_partitions_val} \
  -q ${q_val} > bfs_temp_1.log

  cat bfs_temp_1.log | grep "BFS: Time:" > bfs_temp_2.log
  if [ -s bfs_temp_2.log ];then
    echo "start to report result"
  else
    exit 1
  fi

  while read line
  do
    echo $line
    IFS=" "
    line_arr=($line)
    time_temp=${line_arr[${#line_arr[@]}-1]}
    bfs_time=$(echo -e "scale=4;${time_temp}/1000" | bc)
    echo "CostTime(s): ${bfs_time}"
    if [ ! -d "./report" ]; then
      mkdir report
    fi
    currentTime=$(date "+%Y%m%d_%H%M%S")
    echo -e "algorithmName: BFS\ncostTime: $bfs_time\ndatasetName: ${dataset_name}\nisRaw: 'yes'\nsourceID: ${source_id}\ntestcaseType: BFS_opensource_${dataset_name}_${source_id}\n" > ./report/"BFS_${currentTime}.yml"
  done < bfs_temp_2.log
  rm -rf "bfs_temp_1.log"
  rm -rf "bfs_temp_2.log"
done
echo "Exec Successful: end." > ./log/log
