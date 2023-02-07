#!/bin/bash
set -e

function usage() {
    echo "Usage: <dataset name> <api name> <isRaw>"
    echo "1st argument: name of dataset: cage14, GAP_road, GAP_twitter"
    echo "2nd argument: name of api: static, convergence"
    echo "3rd argument: optimization algorithm or raw: no, yes"
}
case "$1" in
-h | --help | ?)
  usage
  exit 0
  ;;
esac

if [ $# -ne 3 ];then
  usage
	exit 0
fi

dataset_name=$1
api_name=$2
is_raw=$3

if [ ${dataset_name} != "GAP_road" ] && [ ${dataset_name} != "cage14" ] && [ ${dataset_name} != "GAP_twitter" ];then
  echo "invalid dataset name, dataset name:GAP_road, cage14, GAP_twitter"
  exit 1
fi
if [ ${api_name} != "static" ] && [ ${api_name} != "convergence" ];then
  echo "invalid api name,api name: static or convergence"
  exit 1
fi

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

prefix="run"
if [ ${api_name} == "runUntilConvergence" ]
then
  prefix="convergence"
fi

source conf/graph/wpr/wpr_spark.properties
# concatnate strings as a new variable
deploy_mode="deployMode"
num_executors="numExecutors_"${cpu_name}
executor_cores="executorCores_"${cpu_name}
executor_memory="executorMemory_"${cpu_name}
extra_java_options="extraJavaOptions_"${cpu_name}
split="split_graph"

if [ ${is_raw} != "no" ]; then
  num_executors=${api_name}_${dataset_name}_numExecutors
  executor_cores=${api_name}_${dataset_name}_executorCores
  executor_memory=${api_name}_${dataset_name}_executorMemory
  extra_java_options=${api_name}_${dataset_name}_extraJavaOptions
  partition=${api_name}_${dataset_name}_partition
  iter=${api_name}_iter
  tolerance=${api_name}_tolerance

  iter_val=${!iter}
  tolerance_val=${!tolerance}
  partition_val=${!partition}
fi

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
extra_java_options_val=${!extra_java_options}
deploy_mode_val=${!deploy_mode}
split_val=${!split}

echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${extra_java_options} : ${extra_java_options_val}"
echo "${deploy_mode} : ${deploy_mode_val}"
echo "split : ${split_val}"

if [ ! ${num_executors_val} ] ||
  [ ! ${executor_cores_val} ] ||
  [ ! ${executor_memory_val} ] ||
  [ ! ${extra_java_options_val} ] ||
  [ ! ${split_val} ]; then
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
output_path_val=${output_path_prefix}/wpr/${is_raw}/${dataset_name}_${api_name}
echo "${dataset_name} : ${data_path_val}"
echo "output_path : ${output_path_val}"
hdfs dfs -rm -r -f ${output_path_val}

echo "start to clean cache and sleep 3s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 3

echo "start to submit spark jobs -- wpr-${dataset_name}-${api_name}"
if [ ${is_raw} == "no" ]; then
  scp lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent1:/opt/graph_classpath/
  scp lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent2:/opt/graph_classpath/
  scp lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent3:/opt/graph_classpath/

  spark-submit \
  --class com.bigdata.graph.WeightedPageRankRunner \
  --master yarn \
  --deploy-mode ${deploy_mode_val} \
  --num-executors ${num_executors_val} \
  --executor-memory ${executor_memory_val} \
  --executor-cores ${executor_cores_val} \
  --driver-memory 100g \
  --conf spark.driver.maxResultSize=200g \
  --conf spark.driver.extraJavaOptions="-Xms100G" \
  --conf spark.locality.wait.node=0 \
  --conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
  --conf spark.shuffle.blockTransferService=nio \
  --jars "lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --conf "spark.executor.extraClassPath=/opt/graph_classpath/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${dataset_name} ${data_path_val} ${output_path_val} ${api_name} ${is_raw} ${split_val} | tee ./log/log
else
  spark-submit \
  --class com.soundcloud.spark.pagerank.SparkPageRankTest \
  --master yarn \
  --deploy-mode ${deploy_mode_val} \
  --num-executors ${num_executors_val} \
  --executor-memory ${executor_memory_val} \
  --executor-cores ${executor_cores_val} \
  --driver-memory 100g \
  --conf spark.driver.maxResultSize=200g \
  --conf spark.driver.extraJavaOptions="-Xms100G" \
  --conf spark.locality.wait.node=0 \
  --conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
  --conf spark.shuffle.blockTransferService=nio \
  ./lib/spark-pagerank-1.0-SNAPSHOT.jar ${data_path_val} ${split_val} ${partition_val} ${output_path_val}  0.15 ${iter_val} ${tolerance_val}| tee ./log/log
fi