#!/bin/bash
set -e

function usage() {
    echo "Usage: <dataset name> <api name> <isRaw>"
    echo "1st argument: name of dataset: cit_patents,uk_2002,arabic_2005"
    echo "2nd argument: name of api: run,runUntilConvergence"
    echo "3rd argument: optimization algorithm or raw: no/yes"
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

source conf/graph/pr/pr_spark.properties

dataset_name=$1
api_name=$2
is_raw=$3

if [ ${dataset_name} != "cit_patents" ] && [ ${dataset_name} != "uk_2002" ] && [ ${dataset_name} != "arabic_2005" ];then
  echo "invalid dataset name,dataset name:cit_patents,or uk_2002,or arabic_2005"
  exit 1
fi
if [ ${api_name} != "run" ] && [ ${api_name} != "runUntilConvergence" ];then
  echo "invalid api name,api name: run or runUntilConvergence"
  exit 1
fi

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

prefix="run"
if [ ${api_name} == "runUntilConvergence" ]
then
  prefix="convergence"
fi

# concatnate strings as a new variable
num_executors="${prefix}_${dataset_name}_numExecutors_${cpu_name}"
executor_cores="${prefix}_${dataset_name}_executorCores_${cpu_name}"
executor_memory="${prefix}_${dataset_name}_executorMemory_${cpu_name}"
extra_java_options="${prefix}_${dataset_name}_extraJavaOptions_${cpu_name}"
num_partitions="${prefix}_${dataset_name}_numPartitions_${cpu_name}"
deploy_mode="deployMode"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
extra_java_options_val=${!extra_java_options}
deploy_mode_val=${!deploy_mode}
num_partitions_val=${!num_partitions}

echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${extra_java_options} : ${extra_java_options_val}"
echo "${deploy_mode} : ${deploy_mode_val}"
echo "${num_partitions} : ${num_partitions_val}"

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
kal_version=kalVersion
kal_version_val=${!kal_version}
scala_version=scalaVersion
scala_version_val=${!scala_version}

data_path_val=${!dataset_name}
output_path="${output_path_prefix}/pr/${is_raw}/${dataset_name}_${api_name}"
echo "${dataset_name} : ${data_path_val}"
echo "output_path : ${output_path}"
hdfs dfs -rm -r -f ${output_path}

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

echo "start to submit spark jobs -- pr-${dataset_name}-${api_name}"
if [ ${is_raw} == "no" ]; then
  scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent1:/opt/graph_classpath/
  scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent2:/opt/graph_classpath/
  scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent3:/opt/graph_classpath/

  spark-submit \
  --class com.bigdata.graph.PageRankRunner \
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
  --jars "lib/fastutil-8.3.1.jar,lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --conf "spark.executor.extraClassPath=/opt/graph_classpath/fastutil-8.3.1.jar:/opt/graph_classpath/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${dataset_name} ${api_name} ${num_partitions_val} ${is_raw} ${data_path_val} ${output_path} | tee ./log/log
else
  spark-submit \
  --class com.bigdata.graph.PageRankRunner \
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
  --driver-class-path "lib/snakeyaml-1.19.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${dataset_name} ${api_name} ${num_partitions_val} ${is_raw} ${data_path_val} ${output_path} | tee ./log/log
fi