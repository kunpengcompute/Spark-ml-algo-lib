#!/bin/bash
set -e

function alg_usage() {
  echo "please input 1 argument: <dataset name>"
  echo "1st argument: name of dataset: graph500_23, graph500_25, graph500_26, uk_2002, arabic_2005, twitter"
  echo "2nd argument: optimization algorithm or raw: no/yes"
}

case "$1" in
-h | --help | ?)
  alg_usage
  exit 0
  ;;
esac

if [ $# -ne 2 ];then
  alg_usage
	exit 0
fi

dataset_name=$1
is_raw=$2

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')
source conf/graph/modularity/modularity_spark.properties
# concatnate strings as a new variable
num_executors="${dataset_name}_numExectuors_${cpu_name}"
executor_cores="${dataset_name}_executorCores_${cpu_name}"
executor_memory="${dataset_name}_executorMemory_${cpu_name}"
extra_java_options="${dataset_name}_extraJavaOptions_${cpu_name}"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
extra_java_options_val=${!extra_java_options}

echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${extra_java_options} : ${extra_java_options_val}"

if [ ! ${num_executors_val} ] ||
  [ ! ${executor_cores_val} ] ||
  [ ! ${executor_memory_val} ] ||
  [ ! ${extra_java_options_val} ]; then
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
input_community="${dataset_name}_community"
input_community_val=${!input_community}
echo "${dataset_name} : ${data_path_val}"
echo "input_community : ${input_community_val}"

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

echo "start to submit spark jobs -- modularity-${dataset_name}"

if [ ${is_raw} == "no" ]; then
  scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent1:/opt/graph_classpath/
  scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent2:/opt/graph_classpath/
  scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent3:/opt/graph_classpath/

  spark-submit \
  --class com.bigdata.graph.ModularityRunner \
  --driver-memory 80g \
  --master yarn \
  --num-executors ${num_executors_val} \
  --executor-cores ${executor_cores_val} \
  --executor-memory ${executor_memory_val} \
  --conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
  --jars "lib/fastutil-8.3.1.jar,lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --conf "spark.executor.extraClassPath=/opt/graph_classpath/fastutil-8.3.1.jar:/opt/graph_classpath/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${dataset_name} ${data_path_val} ${input_community_val} | tee ./log/log
else
  spark-submit \
  --class com.huawei.graph.algorithms.ModularityComputeByNovel \
  --master yarn \
  --deploy-mode client \
  --num-executors ${num_executors_val} \
  --executor-memory ${executor_memory_val} \
  --executor-cores ${executor_cores_val} \
  --driver-memory 200g \
  --conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
  --conf spark.scheduler.maxRegisteredResourcesWaitingTime=3600000 \
  --conf spark.rpc.message.maxSize=2046 \
  --conf spark.worker.timeout=3600 \
  --conf spark.driver.maxResultSize=200g \
  --conf spark.shuffle.manager=SORT \
  --conf spark.broadcast.blockSize=4m \
  --conf spark.locality.wait.node=0 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.kryoserializer.buffer=48m \
  --conf spark.core.connection.ack.wait.timeout=60000s \
  --conf spark.storage.memoryFraction=0.2 \
  --conf spark.shuffle.memoryFraction=0.6 \
  --conf spark.rdd.compress=true \
  ./lib/modularity_2.11-0.1.0_open.jar yarn ${data_path_val} ${input_community_val} " " "," 500 false false > modularity_temp.log
  costTime=$(cat modularity_temp.log |grep "cost_time:" | awk '{print $2}')
  modularity=$(cat modularity_temp.log |grep "modularity:" | awk '{print $2}')
  currentTime=$(date "+%Y%m%d_H%M%S")
  rm -rf modularity_temp.log
  echo -e "algorithmName: Modularity\ncostTime: $costTime\ndatasetName: ${dataset_name}\nisRaw: 'yes'\nModularity: ${modularity}\ntestcaseType: Modularity_opensource_${dataset_name}\n" > ./report/"Modularity_${currentTime}.yml"
fi