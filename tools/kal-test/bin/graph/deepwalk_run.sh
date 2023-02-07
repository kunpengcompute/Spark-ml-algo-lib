#!/bin/bash
set -e

function usage() {
  echo "Usage: <dataset name> <isRaw>"
  echo "1st argument: name of dataset: cit_patents_deepwalk"
  echo "2nd argument: optimization algorithm or raw: no/yes"
}

case "$1" in
-h | --help | ?)
  usage
  exit 0
  ;;
esac

if [ $# -ne 2 ]; then
  usage
  exit 0
fi

source conf/graph/deepwalk/deepwalk_spark.properties

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

model_conf=${dataset_name}-${cpu_name}

outputPath="/tmp/graph/result/deepwalk/${dataset_name}/${is_raw}"
hdfs dfs -rm -r -f ${outputPath}

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

echo "start to submit spark jobs --- DeepWalk"
if [ ${is_raw} == "no" ]; then
  scp lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent1:/opt/graph_classpath/
  scp lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent2:/opt/graph_classpath/
  scp lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent3:/opt/graph_classpath/

spark-submit \
  --class com.bigdata.graph.DeepWalkRunner \
  --master yarn \
  --deploy-mode ${deploy_mode_val} \
  --num-executors ${num_executors_val} \
  --executor-memory ${executor_memory_val} \
  --executor-cores ${executor_cores_val} \
  --driver-memory 300g \
  --conf spark.kryoserializer.buffer.max=2047m \
  --conf spark.ui.showConsoleProgress=true \
  --conf spark.driver.maxResultSize=0 \
  --conf spark.driver.extraJavaOptions="-Xms300G -XX:hashCode=0" \
  --conf spark.executor.extraJavaOptions="-Xms315G -XX:hashCode=0" \
  --conf spark.rpc.askTimeout=1000000s \
  --conf spark.network.timeout=1000000s \
  --conf spark.executor.heartbeatInterval=100000s \
  --conf spark.rpc.message.maxSize=1000 \
  --conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
  --jars "lib/fastutil-8.3.1.jar,lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --conf "spark.executor.extraClassPath=/opt/graph_classpath/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${model_conf} ${data_path_val} ${outputPath} ${is_raw} | tee ./log/log

else

  walkLength="walkLength_"${dataset_name}_${cpu_name}
  numWalks="numWalks_"${dataset_name}_${cpu_name}
  dimension="dimension_"${dataset_name}_${cpu_name}
  partitions="partitions_"${dataset_name}_${cpu_name}
  iteration="iteration_"${dataset_name}_${cpu_name}
  windowSize="windowSize_"${dataset_name}_${cpu_name}
  splitGraph="splitGraph_"${dataset_name}_${cpu_name}

  walkLength_val=${!walkLength}
  numWalks_val=${!numWalks}
  dimension_val=${!dimension}
  partitions_val=${!partitions}
  iteration_val=${!iteration}
  windowSize_val=${!windowSize}
  splitGraph_val=${!splitGraph}

spark-submit \
  --class  com.nrl.SparkedDeepWalkApp \
  --master yarn \
  --num-executors 6 \
  --executor-memory 95g \
  --driver-memory 300g \
  --executor-cores 38 \
  --driver-cores 80 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.kryoserializer.buffer=48m \
  --conf spark.driver.extraJavaOptions="-Xms300g -XX:hashCode=0" \
  --conf spark.executor.extraJavaOptions="-Xms95g -XX:hashCode=0" \
  --conf spark.driver.maxResultSize=0 \
  --conf spark.rpc.askTimeout=1000000s \
  --conf spark.network.timeout=1000000s \
  --conf spark.executor.heartbeatInterval=100000s \
  --conf spark.rpc.message.maxSize=1000 \
  ./lib/sparked-deepwalk_2.11-1.0.jar "" ${data_path_val}  "" "" "" "" ${outputPath} ${walkLength_val} ${numWalks_val} ${dimension_val} ${partitions_val} ${iteration_val} ${windowSize_val} ${splitGraph_val} | tee ./log/log
fi