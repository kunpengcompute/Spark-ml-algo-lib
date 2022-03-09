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
 	echo "dataset name:cit_patents,enwiki_2018,uk_2002"
	exit 0
fi

current_path=$(dirname $(readlink -f "$0"))
echo "current folder path: ${current_path}"

source conf/graph/betweenness/betweenness_spark.properties

dataset_name=$1

if [ ${dataset_name} != "cit_patents" ] &&
   [ ${dataset_name} != "enwiki_2018" ] &&
   [ ${dataset_name} != "uk_2002" ] ;then
  echo "invalid dataset name,dataset name:cit_patents,enwiki_2018,uk_2002"
  exit 1
fi
outputPath=/tmp/graph/result/betweenness/${dataset_name}
hdfs dfs -rm -r -f ${outputPath}

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')


# concatnate strings as a new variable
num_executors="${dataset_name}_numExecutors_${cpu_name}"
executor_cores="${dataset_name}_executorCores_${cpu_name}"
executor_memory="${dataset_name}_executorMemory_${cpu_name}"
num_partitions="${dataset_name}_numPartitions_${cpu_name}"
spark_task_cpus="${dataset_name}_SparkTaskCpus_${cpu_name}"
thread_num="${dataset_name}_ThreadNum_${cpu_name}"
betweenness_part_num="${dataset_name}_BetweennessPartNum_${cpu_name}"
deploy_mode="deployMode"
driver_memory="driverMemory"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
deploy_mode_val=${!deploy_mode}
num_partitions_val=${!num_partitions}
driver_memory_val=${!driver_memory}
spark_task_cpus_val=${!spark_task_cpus}
thread_num_val=${!thread_num}
betweenness_part_num_val=${!betweenness_part_num}

echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${deploy_mode} : ${deploy_mode_val}"
echo "${num_partitions} : ${num_partitions_val}"
echo "${driver_memory}:${driver_memory_val}"
echo "${spark_task_cpus}:${spark_task_cpus_val}"
echo "${thread_num}:${thread_num_val}"
echo "${betweenness_part_num}:${betweenness_part_num_val}"

if [ ! ${num_executors_val} ] ||
  [ ! ${executor_cores_val} ] ||
  [ ! ${executor_memory_val} ] ||
  [ ! ${spark_task_cpus_val} ] ||
  [ ! ${thread_num_val} ] ||
  [ ! ${betweenness_part_num_val} ] ||
  [ ! ${num_partitions_val} ] ; then
  echo "Some values are NULL, please confirm with the property files"
  exit 0
fi

source conf/graph/graph_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}
gt_path="${dataset_name}_gt"
data_path_val=${!dataset_name}
gt_path_val=${!gt_path}
echo "${dataset_name} : ${data_path_val}"
echo "${gt_path} : ${gt_path_val}"

mkdir -p log

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

echo "start to submit spark jobs--betweenness_${dataset_name}"
spark-submit \
--class com.bigdata.graph.BetweennessRunner \
--master yarn \
--deploy-mode ${deploy_mode_val} \
--num-executors ${num_executors_val} \
--executor-memory ${executor_memory_val} \
--executor-cores ${executor_cores_val} \
--driver-memory ${driver_memory_val} \
--conf spark.rpc.message.maxSize=2046 \
--conf spark.worker.timeout=3600 \
--conf spark.driver.maxResultSize=200g \
--conf spark.rpc.askTimeout=36000 \
--conf spark.network.timeout=6000s \
--conf spark.broadcast.blockSize=4m \
--conf spark.shuffle.manager=SORT \
--conf spark.shuffle.blockTransferService=nio \
--conf spark.locality.wait.node=0 \
--conf spark.boostkit.bigdata.graph.betweenness.partnum=${betweenness_part_num_val} \
--conf spark.boostkit.bigdata.graph.betweenness.threadnum=${thread_num_val} \
--conf spark.task.cpus=${spark_task_cpus_val} \
--jars "lib/fastutil-8.3.1.jar,lib/boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
--driver-class-path "lib/kal-test_2.11-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
--conf "spark.executor.extraClassPath=fastutil-8.3.1.jar:boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
./lib/kal-test_2.11-0.1.jar ${dataset_name} "no" ${num_partitions_val} ${data_path_val} "no" ${outputPath} ${gt_path_val}  | tee ./log/log
