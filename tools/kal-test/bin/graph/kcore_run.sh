#!/bin/bash
set -e

function alg_usage() {
  echo "Usage: <dataset name> <isRaw>"
  echo "1st argument: dataset name: graph500_22, graph500_23, graph500_25, graph500_26"
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

if [ $dataset_name != 'graph500_22' ] && [ $dataset_name != 'graph500_23' ] && [ $dataset_name != 'graph500_25' ] && [ $dataset_name != 'graph500_26' ];
then
  echo 'invalid dataset'
  echo 'dataset name: graph500_22 or graph500_23 or graph500_25 or graph500_26'
  exit 0
fi

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

source conf/graph/kcore/kcore_spark.properties
num_executors_val="numExecutors_${dataset_name}_${cpu_name}"
executor_cores_val="executorCores_${dataset_name}_${cpu_name}"
executor_memory_val="executorMemory_${dataset_name}_${cpu_name}"
executor_extra_javaopts_val="executorExtraJavaopts_${dataset_name}_${cpu_name}"
executor_emoryOverhead_val="executorMemoryOverhead_${dataset_name}_${cpu_name}"

master_val="master"
deploy_mode_val="deployMode"
driver_memory_val="driverMemory"
num_executors=${!num_executors_val}
executor_cores=${!executor_cores_val}
executor_memory=${!executor_memory_val}
master=${!master_val}
driver_memory=${!driver_memory_val}
deploy_mode=${!deploy_mode_val}
executor_extra_javaopts=${!executor_extra_javaopts_val}
executor_emoryOverhead=${!executor_emoryOverhead_val}
if [ ! ${num_executors} ] \
	|| [ ! ${executor_cores} ] \
  || [ ! ${executor_memory} ] \
	|| [ ! ${master} ]; then
   echo "Some values are NUll, please confirm with the property files"
   exit 0
fi
echo "${master_val}:${master}"
echo "${deploy_mode_val}:${deploy_mode}"
echo "${num_executors_val}:${num_executors}"
echo "${executor_cores_val}:${executor_cores}"
echo "${executor_memory_val}:${executor_memory}"
echo "${executor_extra_javaopts_val}:${executor_extra_javaopts}"
echo "${executor_emoryOverhead_val}:${executor_emoryOverhead}"

source conf/graph/graph_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}
kal_version=kalVersion
kal_version_val=${!kal_version}
scala_version=scalaVersion
scala_version_val=${!scala_version}

input_path=${!dataset_name}
output_path="${output_path_prefix}/kcore/${is_raw}/${dataset_name}"
echo "${dataset_name}: ${input_path},${output_path}"

echo "start to clean exist output"
hdfs dfs -rm -r -f -skipTrash ${output_path}

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

echo "start to submit spark jobs -- kcore-${dataset_name}"
if [ ${is_raw} == "no" ]; then
  scp lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent1:/opt/graph_classpath/
  scp lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent2:/opt/graph_classpath/
  scp lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent3:/opt/graph_classpath/

  spark-submit \
  --class com.bigdata.graph.KCoreDecompositionRunner \
  --deploy-mode ${deploy_mode} \
  --driver-memory ${driver_memory} \
  --num-executors ${num_executors} \
  --executor-cores ${executor_cores} \
  --executor-memory ${executor_memory} \
  --conf "spark.executor.extraJavaOptions=${executor_extra_javaopts}" \
  --conf spark.driver.maxResultSize=200g \
  --conf spark.locality.wait.node=0 \
  --conf spark.executor.memoryOverhead=${executor_emoryOverhead} \
  --jars "lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --conf "spark.executor.extraClassPath=/opt/graph_classpath/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${dataset_name} ${input_path} ${output_path} ${is_raw} ${cpu_name} | tee ./log/log
else
  spark-submit \
  --class com.bigdata.graph.KCore \
  --deploy-mode ${deploy_mode} \
  --driver-memory ${driver_memory} \
  --num-executors ${num_executors} \
  --executor-cores ${executor_cores} \
  --executor-memory ${executor_memory} \
  --conf "spark.executor.extraJavaOptions=${executor_extra_javaopts}" \
  --conf spark.driver.maxResultSize=200g \
  --conf spark.locality.wait.node=0 \
  --conf spark.executor.memoryOverhead=${executor_emoryOverhead} \
  --driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/snakeyaml-1.19.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${dataset_name} ${input_path} ${output_path} ${is_raw} ${cpu_name} | tee ./log/log
fi