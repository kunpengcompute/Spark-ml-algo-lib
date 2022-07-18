#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <dataset name> <isRaw>"
  echo "1st argument: name of dataset: twitter_tpr"
  echo "2nd argument: optimization algorithm or raw: no/yes"
  exit 0
  ;;
esac

if [ $# -ne 2 ]; then
  echo "please input 2 arguments: <dataset name> <isRaw>"
  echo "1st argument: name of dataset: twitter_tpr"
  echo "2nd argument: optimization algorithm or raw: no/yes"
  exit 0
fi

source conf/graph/tpr/tpr_spark.properties

dataset_name=$1
is_raw=$2

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

# concatnate strings as a new variable
num_executors="numExectuors_"${dataset_name}_${cpu_name}
executor_cores="executorCores_"${dataset_name}_${cpu_name}
executor_memory="executorMemory_"${dataset_name}_${cpu_name}
extra_java_options="extraJavaOptions_"${dataset_name}_${cpu_name}
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

data_path=${dataset_name}
data_path_val=${!data_path}
echo "${dataset_name} : ${data_path_val}"

outputPath="/tmp/graph/result/tpr/${dataset_name}/${is_raw}"
hdfs dfs -rm -r -f ${outputPath}

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

echo "start to submit spark jobs --- TrillionPageRank"
if [ ${is_raw} == "no" ]; then
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
  ./lib/kal-test_${scala_version_val}-0.1.jar ${dataset_name} ${data_path_val} ${outputPath} ${is_raw} | tee ./log/log
else
  scp lib/kal-test_${scala_version_val}-0.1.jar root@agent1:/opt/graph_classpath/
  scp lib/kal-test_${scala_version_val}-0.1.jar root@agent2:/opt/graph_classpath/
  scp lib/kal-test_${scala_version_val}-0.1.jar root@agent3:/opt/graph_classpath/

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
  --driver-class-path "lib/snakeyaml-1.19.jar" \
  --conf "spark.executor.extraClassPath=/opt/graph_classpath/kal-test_${scala_version_val}-0.1.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${dataset_name} ${data_path_val} ${outputPath} ${is_raw} | tee ./log/log
fi