#!/bin/bash
set -e

function alg_usage() {
  echo "Usage: <dataset name> <api name> <isRaw>"
  echo "1st argument: name of dataset: graph500_22,graph500_24,graph500_25"
  echo "2nd argument: api: run,runConvergence"
  echo "3rd argument: optimization algorithm or raw: no/yes"
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

dataset_name=$1
api=$2
is_raw=$3

if [ $api != "run" ] && [ $api != "runConvergence" ];
then
  echo "invalid api."
  echo "api: run or runConvergence"
  exit 0
fi

if [ $dataset_name != 'graph500_22' ] && [ $dataset_name != 'graph500_24' ] && [ $dataset_name != 'graph500_25' ];
then
  echo 'invalid dataset'
  echo "dataset name: graph500_22 or graph500_24 or graph500_25"
  exit 0
fi

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

source conf/graph/lpa/lpa_spark.properties
num_executors_val="numExecutors_${dataset_name}_${cpu_name}"
executor_cores_val="executorCores_${dataset_name}_${cpu_name}"
executor_memory_val="executorMemory_${dataset_name}_${cpu_name}"
executor_extra_javaopts_val="executorExtraJavaopts_${dataset_name}_${cpu_name}"

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

source conf/graph/graph_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}
kal_version=kalVersion
kal_version_val=${!kal_version}
scala_version=scalaVersion
scala_version_val=${!scala_version}

input_path=${!dataset_name}
output_path="${output_path_prefix}/lpa/${is_raw}/${dataset_name}/${api}"
echo "${dataset_name}: ${input_path},${output_path}"

echo "start to clean exist output"
hdfs dfs -rm -r -f -skipTrash ${output_path}

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

echo "start to submit spark jobs -- lpa-${api}-${dataset_name}"

if [ ${is_raw} == "no" ]; then
  scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent1:/opt/graph_classpath/
  scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent2:/opt/graph_classpath/
  scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent3:/opt/graph_classpath/

  spark-submit \
  --class com.bigdata.graph.LabelPropagationRunner \
  --deploy-mode ${deploy_mode} \
  --driver-memory ${driver_memory} \
  --num-executors ${num_executors} \
  --executor-cores ${executor_cores} \
  --executor-memory ${executor_memory} \
  --conf "spark.executor.extraJavaOptions=${executor_extra_javaopts}" \
  --jars "lib/fastutil-8.3.1.jar,lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --conf "spark.executor.extraClassPath=/opt/graph_classpath/fastutil-8.3.1.jar:/opt/graph_classpath/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${dataset_name} ${input_path} ${output_path} ${api} ${is_raw} ${cpu_name} | tee ./log/log
else
  spark-submit \
  --class com.bigdata.graph.LabelPropagationRunner \
  --deploy-mode ${deploy_mode} \
  --driver-memory ${driver_memory} \
  --num-executors ${num_executors} \
  --executor-cores ${executor_cores} \
  --executor-memory ${executor_memory} \
  --conf "spark.executor.extraJavaOptions=${executor_extra_javaopts}" \
  --driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/snakeyaml-1.19.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${dataset_name} ${input_path} ${output_path} "run" ${is_raw} ${cpu_name} | tee ./log/log
fi
