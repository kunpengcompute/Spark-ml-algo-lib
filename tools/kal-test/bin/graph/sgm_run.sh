#!/bin/bash
set -e

function alg_usage() {
  echo "Usage: <dataset name> <queryGraph name> <identical or not> <isRaw>"
  echo "1st argument: name of dataset: name of dataset: graph500_19,liveJournal,com_orkut"
  echo "2nd argument: name of queryGraph: for Identical: 4dgn/4sqr/5tree/6star; for unIdentical: 4dgn/4clique/5clique/6clique"
  echo "3rd argument: match mode:Identical,unIdentical"
  echo "4th argument: optimization algorithm or raw: no/yes"
}

case "$1" in
-h | --help | ?)
  alg_usage
  exit 0
  ;;
esac

if [ $# -ne 4 ];then
  alg_usage
	exit 0
fi

dataset_name=$1
queryGraph=$2
match_mode=$3
is_raw=$4

if [ ${dataset_name} != "graph500_19" ] &&
   [ ${dataset_name} != "liveJournal" ] &&
   [ ${dataset_name} != "com_orkut" ] ;then
  echo "invalid dataset name,dataset name:graph500_19,liveJournal,com_orkut"
  exit 1
fi
if [ ${match_mode} != "Identical" ] &&
   [ ${match_mode} != "unIdentical" ] ;then
  echo "invalid argument value,match mode:identical or unidentical"
  exit 1
fi
if [ ${match_mode} == "Identical" ] ; then
  if [ ${queryGraph} != "4dgn" ] &&
     [ ${queryGraph} != "4sqr" ] &&
     [ ${queryGraph} != "5tree" ] &&
     [ ${queryGraph} != "6star" ] ; then
    echo "invalid queryGraph,queryGraph name:4dgn,4sqr,5tree,6star"
    exit 1
  fi
elif [ ${match_mode} == "unIdentical" ]; then
  if [ ${queryGraph} != "4dgn" ] &&
     [ ${queryGraph} != "4clique" ] &&
     [ ${queryGraph} != "5clique" ] &&
     [ ${queryGraph} != "6clique" ] ; then
    echo "invalid queryGraph,queryGraph name:4dgn,4clique,5clique,6clique"
    exit 1
  fi
fi

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

# concatnate strings as a new variable
source conf/graph/sgm/sgm_spark.properties
num_executors="${dataset_name}_${queryGraph}_${match_mode}_numExecutors_${cpu_name}"
executor_cores="${dataset_name}_${queryGraph}_${match_mode}_executorCores_${cpu_name}"
executor_memory="${dataset_name}_${queryGraph}_${match_mode}_executorMemory_${cpu_name}"
num_partitions="${dataset_name}_${queryGraph}_${match_mode}_numPartitions_${cpu_name}"
extra_Java_Options="${dataset_name}_${queryGraph}_${match_mode}_executorExtraJavaOptions_${cpu_name}"
num_Task="${dataset_name}_${queryGraph}_${match_mode}_numberTask_${cpu_name}"
deploy_mode="deployMode"
driver_memory="driverMemory"
rpc_askTime="rpcAskTime"
scheduler_maxRegisteredResourcesWaitingTime="schedulerMaxRegisteredResourcesWaitingTime"
worker_timeout="workerTimeout"
network_timeout="networkTimeout"
storage_blockManagerSlaveTimeoutMs="storageBlockManagerSlaveTimeoutMs"
shuffle_blockTransferService="shuffleBlockTransferService"
driver_maxResultSize="driverMaxResultSize"
shuffle_manager="shuffleManager"
broadcast_blockSize="broadcastBlockSize"
rpc_message_maxSize="rpcMessageMaxSize"
core_connection_ack_wait_timeout="coreConnectionAckWaitTimeout"
storage_memoryFraction="storageMemoryFraction"
shuffle_memoryFraction="shuffleMemoryFraction"
rdd_compress="rddCompress"
memory_useLegacyMode="memoryUseLegacyMode"
num_Colors="numberColors"
graph_Split="${dataset_name}_split"

if [ ${is_raw} == "yes" ]; then
  num_executors="numExecutors"
  executor_cores="executorCores"
  executor_memory="executorMemory"
  num_partitions="numPartitions"
  extra_Java_Options="executorExtraJavaOptions"
fi

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
num_task_val=${!num_Task}
deploy_mode_val=${!deploy_mode}
num_partitions_val=${!num_partitions}
extra_Java_Options_val=${!extra_Java_Options}
driver_memory_val=${!driver_memory}
rpc_askTime_val=${!rpc_askTime}
scheduler_maxRegisteredResourcesWaitingTime_val=${!scheduler_maxRegisteredResourcesWaitingTime}
worker_timeout_val=${!worker_timeout}
network_timeout_val=${!network_timeout}
storage_blockManagerSlaveTimeoutMs_val=${!storage_blockManagerSlaveTimeoutMs}
shuffle_blockTransferService_val=${!shuffle_blockTransferService}
driver_maxResultSize_val=${!driver_maxResultSize}
shuffle_manager_val=${!shuffle_manager}
broadcast_blockSize_val=${!broadcast_blockSize}
rpc_message_maxSize_val=${!rpc_message_maxSize}
core_connection_ack_wait_timeout_val=${!core_connection_ack_wait_timeout}
storage_memoryFraction_val=${!storage_memoryFraction}
shuffle_memoryFraction_val=${!shuffle_memoryFraction}
rdd_compress_val=${!rdd_compress}
memory_useLegacyMode_val=${!memory_useLegacyMode}
num_colors_val=${!num_Colors}
graph_split_val=${!graph_Split}

echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${deploy_mode} : ${deploy_mode_val}"
echo "${num_partitions} : ${num_partitions_val}"
echo "${driver_memory}:${driver_memory_val}"
echo "${extra_Java_Options}:${extra_Java_Options_val}"
echo "${num_Task}:${num_task_val}"
echo "${num_Colors}:${num_colors_val}"
echo "${graph_Split}:${graph_split_val}"

if [ ! ${num_executors_val} ] ||
   [ ! ${executor_cores_val} ] ||
   [ ! ${executor_memory_val} ] ||
   [ ! ${num_partitions_val} ] ||
   [ ! ${num_task_val} ] ||
   [ ! ${num_colors_val} ] ||
   [ ! ${graph_split_val} ] ||
   [ ! ${extra_Java_Options_val} ]; then
  echo "Some values are NULL, please confirm with the property files"
  exit 0
fi

source conf/graph/graph_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}
kal_version=kalVersion
kal_version_val=${!kal_version}
scala_version=scalaVersion
scala_version_val=${!scala_version}

data_path_val=${!dataset_name}
queryGraph_path="query_${queryGraph}"
queryGraph_path_val=${!queryGraph_path}
echo "${dataset_name} : ${data_path_val}"
echo "${queryGraph_path} : ${queryGraph_path_val}"

output_path="${output_path_prefix}/sgm/${is_raw}/${dataset_name}_${queryGraph}_${match_mode}"
hdfs dfs -rm -r -f ${output_path}

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

echo "start to submit spark jobs--SGM_${dataset_name}_${queryGraph}_${match_mode}"
if [ ${is_raw} == "no" ]; then
  scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent1:/opt/graph_classpath/
  scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent2:/opt/graph_classpath/
  scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent3:/opt/graph_classpath/

  spark-submit \
  --class com.bigdata.graph.SubgraphMatchingRunner \
  --master yarn \
  --deploy-mode ${deploy_mode_val} \
  --num-executors ${num_executors_val} \
  --executor-memory ${executor_memory_val} \
  --executor-cores ${executor_cores_val} \
  --driver-memory ${driver_memory_val} \
  --conf spark.executor.extraJavaOptions=${extra_Java_Options_val} \
  --conf spark.rpc.askTime=${rpc_askTime_val} \
  --conf spark.scheduler.maxRegisteredResourcesWaitingTime=${scheduler_maxRegisteredResourcesWaitingTime_val} \
  --conf spark.worker.timeout=${worker_timeout_val} \
  --conf spark.network.timeout=${network_timeout_val} \
  --conf spark.storage.blockManagerSlaveTimeoutMs=${storage_blockManagerSlaveTimeoutMs_val} \
  --conf spark.shuffle.blockTransferService=${shuffle_blockTransferService_val} \
  --conf spark.driver.maxResultSize=${driver_maxResultSize_val} \
  --conf spark.shuffle.manager=${shuffle_manager_val} \
  --conf spark.broadcast.blockSize=${broadcast_blockSize_val} \
  --conf spark.rpc.message.maxSize=${rpc_message_maxSize_val} \
  --conf spark.core.connection.ack.wait.timeout=${core_connection_ack_wait_timeout_val} \
  --conf spark.storage.memoryFraction=${storage_memoryFraction_val} \
  --conf spark.shuffle.memoryFraction=${shuffle_memoryFraction_val} \
  --conf spark.rdd.compress=${rdd_compress_val} \
  --conf spark.memory.useLegacyMode=${memory_useLegacyMode_val} \
  --conf spark.executor.memoryOverhead=5g \
  --jars "lib/fastutil-8.3.1.jar,lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --conf "spark.executor.extraClassPath=/opt/graph_classpath/fastutil-8.3.1.jar:/opt/graph_classpath/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${dataset_name} ${queryGraph} ${is_raw} ${match_mode} ${output_path} ${data_path_val} ${num_partitions_val} ${num_task_val} ${queryGraph_path_val} | tee ./log/log
else
  spark-submit \
  --class pegasus.spark.subgraph.TestOriginal \
  --name "SGM_${dataset_name}_${queryGraph}_opensource" \
  --master yarn \
  --deploy-mode ${deploy_mode_val} \
  --num-executors ${num_executors_val} \
  --executor-memory ${executor_memory_val} \
  --executor-cores ${executor_cores_val} \
  --driver-memory ${driver_memory_val} \
  --conf spark.executor.extraJavaOptions=${extra_Java_Options_val} \
  --conf spark.rpc.askTime=${rpc_askTime_val} \
  --conf spark.scheduler.maxRegisteredResourcesWaitingTime=${scheduler_maxRegisteredResourcesWaitingTime_val} \
  --conf spark.worker.timeout=${worker_timeout_val} \
  --conf spark.network.timeout=${network_timeout_val} \
  --conf spark.storage.blockManagerSlaveTimeoutMs=${storage_blockManagerSlaveTimeoutMs_val} \
  --conf spark.shuffle.blockTransferService=${shuffle_blockTransferService_val} \
  --conf spark.driver.maxResultSize=${driver_maxResultSize_val} \
  --conf spark.shuffle.manager=${shuffle_manager_val} \
  --conf spark.broadcast.blockSize=${broadcast_blockSize_val} \
  --conf spark.rpc.message.maxSize=${rpc_message_maxSize_val} \
  --conf spark.core.connection.ack.wait.timeout=${core_connection_ack_wait_timeout_val} \
  --conf spark.storage.memoryFraction=${storage_memoryFraction_val} \
  --conf spark.shuffle.memoryFraction=${shuffle_memoryFraction_val} \
  --conf spark.rdd.compress=${rdd_compress_val} \
  --conf spark.memory.useLegacyMode=${memory_useLegacyMode_val} \
  ./lib/pegasus-spark_2.11-0.1.0-SNAPSHOT_openSource.jar yarn ${data_path_val} ${output_path} ${queryGraph_path_val} ${num_colors_val} 232 "," ${graph_split_val} 10000 > sgm_temp.log
  num_subgraphs=$(cat sgm_temp.log | grep "number of matched subgraphs" | awk -F '[\t]' '{print $2}')
  costTime=$(cat sgm_temp.log | grep "cost time" | awk -F '[\t]' '{print $2}')
  currentTime=$(date "+%Y%m%d_H%M%S")
  rm -rf sgm_temp.log
  echo -e "algorithmName: SGM\ncostTime: $costTime\ndatasetName: ${dataset_name}\nisRaw: 'yes'\nnum_subgraphs: $num_subgraphs\ntestcaseType: SGM_opensource_${1}_${2}_opensource\n" > ./report/"SGM_${currentTime}.yml"
  echo "Exec Successful: End." > ./log/log
fi