#!/bin/bash
set -e

function alg_usage() {
  echo "please input 1 argument: <dataset name>"
  echo "1st argument: name of dataset: graph500_24, graph500_25, graph500_26"
}

case "$1" in
-h | --help | ?)
  alg_usage
  exit 0
  ;;
esac

if [ $# -ne 1 ];then
  alg_usage
	exit 0
fi

dataset_name=$1
is_raw=no

source conf/graph/wce/wce_spark.properties

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

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
output_path="/tmp/graph/result/wce/${is_raw}/${dataset_name}"
echo "${dataset_name} : ${data_path_val}"
echo "output_path : ${output_path}"
hdfs dfs -rm -r -f ${output_path}

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

echo "start to submit spark jobs -- wce-${dataset_name}"

scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent1:/opt/graph_classpath/
scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent2:/opt/graph_classpath/
scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent3:/opt/graph_classpath/

spark-submit \
--class com.bigdata.graph.WCERunner \
--driver-memory 80g \
--master yarn \
--num-executors ${num_executors_val} \
--executor-cores ${executor_cores_val} \
--executor-memory ${executor_memory_val} \
--conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
--jars "lib/fastutil-8.3.1.jar,lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
--driver-class-path "lib/fastutil-8.3.1.jar:lib/kal-test_${scala_version_val}-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
--conf "spark.executor.extraClassPath=/opt/graph_classpath/fastutil-8.3.1.jar:/opt/graph_classpath/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
./lib/kal-test_${scala_version_val}-0.1.jar ${dataset_name} ${data_path_val} ${output_path} | tee ./log/log
