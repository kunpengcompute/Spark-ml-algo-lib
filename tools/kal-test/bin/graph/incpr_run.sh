#!/bin/bash
set -e

function alg_usage() {
  echo "Usage: <dataset name> <rate> <batch> <isRaw>"
  echo "1st argument: name of dataset: twitter_2010"
  echo "2nd argument: rate: e.g. 0.001,0.01,0.05"
  echo "3nd argument: batch: e.g. 1,2,3,4,5"
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
rate=$2
batch=$3
is_raw=$4

source conf/graph/incpr/incpr_spark.properties
cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

# concatnate strings as a new variable
num_executors="numExectuors"
executor_cores="executorCores"
executor_memory="executorMemory"
extra_java_options="extraJavaOptions"
driver_cores="driverCores"
driver_memory="driverMemory"
executor_memory_overhead="execMemOverhead"
master_="master"
deploy_mode="deployMode"
echo $num_executors
echo $executor_cores
echo $executor_memory
echo $extra_java_options
deploy_mode="deployMode"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
extra_java_options_val=${!extra_java_options}
deploy_mode_val=${!deploy_mode}

echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${extra_java_options} : ${extra_java_options_val}"
echo "${deploy_mode} : ${deploy_mode_val}"

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

data_path_val=${!dataset_name}_${rate}_batch_${batch}
output_path="${output_path_prefix}/incpr/${is_raw}/${dataset_name}_${rate}_batch_${batch}"
echo "${dataset_name} : ${data_path_val}"
echo "output_path : ${output_path}"
hdfs dfs -rm -r -f ${output_path}

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

echo "start to submit spark jobs -- incpr-${dataset_name}_${rate}_batch_${batch}"
if [ ${is_raw} == "no" ]; then
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
  --jars "lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --driver-class-path "lib/snakeyaml-1.19.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${dataset_name} ${data_path_val} ${output_path} ${is_raw} | tee ./log/log
else
  scp lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent1:/opt/graph_classpath/
  scp lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent2:/opt/graph_classpath/
  scp lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent3:/opt/graph_classpath/

  spark-submit \
  --class com.bigdata.graph.TrillionPageRankRunner \
  --master yarn \
  --deploy-mode ${deploy_mode_val} \
  --num-executors ${num_executors_val} \
  --executor-memory ${executor_memory_val} \
  --executor-cores ${executor_cores_val} \
  --driver-memory 80g \
  --conf spark.driver.maxResultSize=80g \
  --conf spark.locality.wait.node=0 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.kryoserializer.buffer.max=2040m \
  --conf spark.rdd.compress=true \
  --conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
  --jars "lib/fastutil-8.3.1.jar,lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --conf "spark.executor.extraClassPath=/opt/graph_classpath/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${dataset_name} ${data_path_val} ${output_path} no | tee ./log/log
fi