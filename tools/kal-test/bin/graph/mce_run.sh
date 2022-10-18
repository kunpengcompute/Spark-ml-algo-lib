#!/bin/bash
set -e

function alg_usage() {
  echo "Usage:<dataset name>"
  echo "dataset name: graph500_23, graph500_24, graph500_25"
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
cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

source conf/graph/mce/mce_spark.properties
num_executors_val="numExecutors_${cpu_name}"
executor_cores_val="executorCores"
executor_memory_val="executorMemory_${cpu_name}"
extra_java_options_val="extraJavaOptions_${cpu_name}"
deploy_mode_val="deployMode"
driver_memory_val="driverMemory"
num_executors=${!num_executors_val}
executor_cores=${!executor_cores_val}
executor_memory=${!executor_memory_val}
extra_java_options=${!extra_java_options_val}
driver_memory=${!driver_memory_val}
deploy_mode=${!deploy_mode_val}

echo "${deploy_mode_val}:${deploy_mode}"
echo "${driver_memory_val}:${driver_memory}"
echo "${num_executors_val}:${num_executors}"
echo "${executor_cores_val}:${executor_cores}"
echo "${executor_memory_val}:${executor_memory}"
echo "${extra_java_options_val}:${extra_java_options}"
if [ ! ${num_executors} ] \
	|| [ ! ${executor_cores} ] \
  || [ ! ${executor_memory} ] \
  || [ ! ${driver_memory} ] \
  || [ ! ${extra_java_options} ] \
	|| [ ! ${deploy_mode} ]; then
   echo "Some values are NUll, please confirm with the property files"
   exit 0
fi

source conf/graph/graph_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}
kal_version=kalVersion
kal_version_val=${!kal_version}
scala_version=scalaVersion
scala_version_val=${!scala_version}

input_path=${!dataset_name}
output_path="${output_path_prefix}/mce/${dataset_name}"
echo "${dataset_name} : ${input_path}"
echo "outputPath : ${output_path}"
hdfs dfs -rm -r -f ${output_path}

echo "start to clean exist output"
hdfs dfs -rm -r -f -skipTrash ${output_path}

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent1:/opt/graph_classpath/
scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent2:/opt/graph_classpath/
scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent3:/opt/graph_classpath/

echo "start to submit spark jobs -- mce-${dataset_name}"
spark-submit \
--class com.bigdata.graph.MaximalCliqueEnumerationRunner \
--deploy-mode ${deploy_mode} \
--driver-memory ${driver_memory} \
--num-executors ${num_executors} \
--executor-cores ${executor_cores} \
--executor-memory ${executor_memory} \
--conf "spark.executor.extraJavaOptions=${extra_java_options}" \
--conf spark.locality.wait=10 \
--conf spark.rdd.compress=false \
--conf spark.io.compression.codec=lz4 \
--jars "lib/fastutil-8.3.1.jar,lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
--driver-class-path "lib/fastutil-8.3.1.jar:lib/kal-test_${scala_version_val}-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
--conf "spark.executor.extraClassPath=/opt/graph_classpath/fastutil-8.3.1.jar:/opt/graph_classpath/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
./lib/kal-test_${scala_version_val}-0.1.jar ${dataset_name} ${input_path} ${output_path} | tee ./log/log





