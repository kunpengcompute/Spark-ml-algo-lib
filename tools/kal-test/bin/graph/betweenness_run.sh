#!/bin/bash
set -e

function alg_usage() {
  echo "Usage: <dataset name> <is raw> <is check>"
  echo "1st argument: name of dataset: cit_patents, enwiki_2018, uk_2002"
  echo "2nd argument: optimization algorithm or raw: no/yes"
  echo "3rd argument: verify result: no/yes"
}

case "$1" in
-h | --help | ?)
  alg_usage
  exit 0
  ;;
esac

if [ $# -ne 3 ];then
  alg_usage
	exit 0
fi

source conf/graph/betweenness/betweenness_spark.properties

dataset_name=$1
is_raw=$2
is_check=$3

if [ ${dataset_name} != "cit_patents" ] &&
   [ ${dataset_name} != "enwiki_2018" ] &&
   [ ${dataset_name} != "uk_2002" ] ;then
  echo "invalid dataset name,dataset name:cit_patents, enwiki_2018, uk_2002"
  exit 1
fi

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

# concatnate strings as a new variable
num_executors="${dataset_name}_numExecutors_${cpu_name}"
executor_cores="${dataset_name}_executorCores_${cpu_name}"
executor_memory="${dataset_name}_executorMemory_${cpu_name}"
num_partitions="${dataset_name}_numPartitions_${cpu_name}"
spark_task_cpus="${dataset_name}_SparkTaskCpus_${cpu_name}"
thread_num="${dataset_name}_ThreadNum_${cpu_name}"
betweenness_part_num="${dataset_name}_BetweennessPartNum_${cpu_name}"
pivots="${dataset_name}_pivots"
iteration="${dataset_name}_iteration"
graph_split="${dataset_name}_graphSplit"
deploy_mode="deployMode"
driver_memory="driverMemory"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
deploy_mode_val=${!deploy_mode}
num_partitions_val=${!num_partitions}
driver_memory_val=${!driver_memory}

echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${deploy_mode} : ${deploy_mode_val}"
echo "${num_partitions} : ${num_partitions_val}"
echo "${driver_memory}:${driver_memory_val}"

if [ ! ${num_executors_val} ] ||
  [ ! ${executor_cores_val} ] ||
  [ ! ${executor_memory_val} ] ||
  [ ! ${num_partitions_val} ] ; then
  echo "Some values are NULL, please confirm with the property files"
  exit 0
fi

if [ ${is_raw} == "no" ]; then
  spark_task_cpus_val=${!spark_task_cpus}
  thread_num_val=${!thread_num}
  betweenness_part_num_val=${!betweenness_part_num}

  echo "${spark_task_cpus}:${spark_task_cpus_val}"
  echo "${thread_num}:${thread_num_val}"
  echo "${betweenness_part_num}:${betweenness_part_num_val}"

  if [ ! ${spark_task_cpus_val} ] ||
    [ ! ${thread_num_val} ] ||
    [ ! ${betweenness_part_num_val} ] ; then
    echo "Some values are NULL, please confirm with the property files"
    exit 0
  fi
else
  pivots_val=${!pivots}
  iteration_val=${!iteration}
  graph_split_val=${!graph_split}

  echo "${pivots}:${pivots_val}"
  echo "${iteration}:${iteration_val}"
  echo "${graph_split}:${graph_split_val}"

  if [ ! ${pivots_val} ] ||
    [ ! ${iteration_val} ] ||
    [ ! ${graph_split_val} ]; then
    echo "Some values are NULL, please confirm with the property files"
    exit 0
  fi
fi

source conf/graph/graph_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}
kal_version=kalVersion
kal_version_val=${!kal_version}
scala_version=scalaVersion
scala_version_val=${!scala_version}

gt_path="${dataset_name}_gt"
data_path_val=${!dataset_name}
gt_path_val=${!gt_path}
output_path="${output_path_prefix}/betweenness/${is_raw}/${dataset_name}"
hdfs dfs -rm -r -f ${output_path}

echo "${dataset_name} : ${data_path_val}"
echo "${gt_path} : ${gt_path_val}"
echo "output_path : ${output_path}"

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

echo "start to submit spark jobs--betweenness_${dataset_name}"
if [ ${is_raw} == "no" ]; then
  scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent1:/opt/graph_classpath/
  scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent2:/opt/graph_classpath/
  scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent3:/opt/graph_classpath/

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
  --jars "lib/fastutil-8.3.1.jar,lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --conf "spark.executor.extraClassPath=/opt/graph_classpath/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${dataset_name} ${is_raw} ${num_partitions_val} ${data_path_val} ${is_check} ${output_path} ${gt_path_val}  | tee ./log/log
else
  spark-submit \
  --master yarn \
  --deploy-mode ${deploy_mode_val} \
  --name "Betweenness_${dataset_name}_opensource" \
  --num-executors ${num_executors_val} \
  --executor-memory ${executor_memory_val} \
  --executor-cores ${executor_cores_val} \
  --driver-memory ${driver_memory_val} \
  --conf spark.kryoserializer.buffer.max=2047m \
  --conf spark.driver.maxResultSize=0 \
  --conf spark.ui.showConsoleProgress=true \
  --conf spark.driver.extraJavaOptions="-Xms${driver_memory_val} -XX:hashCode=0" \
  --conf spark.executor.extraJavaOptions="-Xms${executor_memory_val} -XX:hashCode=0" \
  --conf spark.rpc.askTimeout=1000000s \
  --conf spark.network.timeout=1000000s \
  --conf spark.executor.heartbeatInterval=100000s \
  --conf spark.rpc.message.maxSize=1000 \
  --jars "./lib/scopt_2.11-3.2.0.jar" \
  ./lib/hbse_2.11-0.1.jar \
  -m yarn \
  -s ${graph_split_val} \
  -n ${num_partitions_val} \
  -i ${data_path_val} \
  -o ${output_path} \
  -g ${gt_path_val} \
  -p ${pivots_val} \
  -b ${iteration_val} > betweenness_temp.log
  CostTime=$(cat betweenness_temp.log |grep "CostTime of Top-K" | awk '{print $6}')
  Accuracy=$(cat betweenness_temp.log |grep "Accuracy of Top-K" | awk '{print $6}')
  currentTime=$(date "+%Y%m%d_H%M%S")
  rm -rf betweenness_temp.log
  echo -e "algorithmName: Betweenness\ncostTime: $CostTime\ndatasetName: ${dataset_name}\nisRaw: 'yes'\nAccuracy: ${Accuracy}\ntestcaseType: Betweenness_opensource_${dataset_name}\n" > ./report/"Betweenness_${currentTime}.yml"
  echo "Exec Successful: end." > ./log/log
fi