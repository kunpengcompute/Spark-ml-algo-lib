#!/bin/bash
set -e

function alg_usage() {
  echo "Usage: <dataset name> <isRaw>"
  echo "1st argument: name of dataset: cit_patents,soc_liveJournal,uk_2002"
  echo "2nd argument: optimization algorithm or raw: no/yes"
  echo "3rd argument: verify result: no/yes"
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

source conf/graph/node2vec/node2vec_spark.properties

dataset_name=$1
is_raw=$2
is_check=$3

if [ ${dataset_name} != "cit_patents" ] &&
   [ ${dataset_name} != "soc_liveJournal" ] &&
   [ ${dataset_name} != "uk_2002" ] ;then
  echo "invalid dataset name,dataset name:cit_patents,soc_liveJournal,uk_2002"
  exit 1
fi

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')
model_conf=${dataset_name}-${cpu_name}

# concatnate strings as a new variable
master_="master"
deploy_mode="deployMode"
driver_memory="driverMemory"
driver_cores="driverCores_${cpu_name}"
executor_cores="executorCores_${cpu_name}"
executor_memory="executorMemory_${cpu_name}"
num_executors="numExecutors_${cpu_name}"

master_val=${!master_}
deploy_mode_val=${!deploy_mode}
num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
driver_memory_val=${!driver_memory}
driver_cores_val=${!driver_cores}

echo "${cpu_name}"
echo "${master_} : ${master_val}"
echo "${deploy_mode} : ${deploy_mode_val}"
echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${driver_memory}:${driver_memory_val}"
echo "${driver_cores}:${driver_cores_val}"

if [ ! ${num_executors_val} ] ||
  [ ! ${executor_cores_val} ] ||
  [ ! ${executor_memory_val} ] ||
  [ ! ${driver_memory_val} ] ||
  [ ! ${driver_cores_val} ] ; then
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

gt_path="${dataset_name}_negEdge"
data_path_val=${!dataset_name}
gt_path_val=${!gt_path}
output_path="${output_path_prefix}/node2vec/${is_raw}/${dataset_name}"
echo "${dataset_name} : ${data_path_val}"
echo "output_path : ${output_path}"
echo "gt_path : ${gt_path_val}"
hdfs dfs -rm -r -f ${output_path}

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

echo "start to submit spark jobs--node2vec_${model_conf}"
if [ ${is_raw} == "no" ]; then
  scp lib/fastutil-8.3.1.jar lib/boostkit-graph-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent1:/opt/graph_classpath/
  scp lib/fastutil-8.3.1.jar lib/boostkit-graph-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent2:/opt/graph_classpath/
  scp lib/fastutil-8.3.1.jar lib/boostkit-graph-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent3:/opt/graph_classpath/

  spark-submit \
  --class com.bigdata.graph.Node2VecRunner \
  --master ${master_val} \
  --deploy-mode ${deploy_mode_val} \
  --num-executors ${num_executors_val} \
  --executor-memory ${executor_memory_val} \
  --executor-cores ${executor_cores_val} \
  --driver-memory ${driver_memory_val} \
  --driver-cores ${driver_cores_val} \
  --conf spark.kryoserializer.buffer.max=2047m \
  --conf spark.ui.showConsoleProgress=true \
  --conf spark.driver.maxResultSize=0 \
  --conf spark.driver.extraJavaOptions="-Xms300G -XX:hashCode=0" \
  --conf spark.executor.extraJavaOption="-Xms315G -XX:hashCode=0" \
  --conf spark.rpc.askTimeout=1000000s \
  --conf spark.network.timeout=1000000s \
  --conf spark.executor.heartbeatInterval=100000s \
  --conf spark.rpc.message.maxSize=1000 \
  --jars "lib/smile-core-2.5.3.jar,lib/smile-math-2.5.3.jar,lib/fastutil-8.3.1.jar,lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar,lib/boostkit-graph-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar" \
  --driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar:lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --conf "spark.executor.extraClassPath=/opt/graph_classpath/fastutil-8.3.1.jar:/opt/graph_classpath/boostkit-graph-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar:/opt/graph_classpath/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${model_conf} ${data_path_val} ${output_path} ${gt_path_val} ${is_check} | tee ./log/log
else
  spark-submit \
  --class vn.five9.Main \
  --master ${master_val} \
  --name "Node2Vec_${model_conf}" \
  --deploy-mode ${deploy_mode_val} \
  --num-executors ${num_executors_val} \
  --executor-memory ${executor_memory_val} \
  --executor-cores ${executor_cores_val} \
  --driver-memory ${driver_memory_val} \
  --driver-cores ${driver_cores_val} \
  --conf spark.kryoserializer.buffer.max=2047m \
  --conf spark.ui.showConsoleProgress=true \
  --conf spark.driver.maxResultSize=0 \
  --conf spark.driver.extraJavaOptions="-Xms300G -XX:hashCode=0" \
  --conf spark.executor.extraJavaOption="-Xms315G -XX:hashCode=0" \
  --conf spark.rpc.askTimeout=1000000s \
  --conf spark.network.timeout=1000000s \
  --conf spark.executor.heartbeatInterval=100000s \
  --conf spark.rpc.message.maxSize=1000 \
  --jars "lib/fastutil-8.3.1.jar,lib/spark-mllib_2.11-2.3.2.jar,lib/smile-core-2.5.3.jar,lib/scopt_2.11-3.5.0.jar,lib/smile-math-2.5.3.jar" \
  ./lib/node2vec-baseline.jar \
  --cmd node2vec --indexed true --directed true --degree 1000000000 \
  --p 1.0 --q 1.0 --walkLength 5 --numWalks 10 \
  --input ${data_path_val} --output ${output_path} > node2vec_tmp.log

  CostTime=$(cat node2vec_tmp.log |grep "total time" | awk '{print $7}')
  currentTime=$(date "+%Y%m%d_H%M%S")
  rm -rf node2vec_tmp.log
  echo -e "algorithmName: Node2vec\ncostTime: $CostTime\ndatasetName: ${dataset_name}\nisRaw: 'yes'\ntestcaseType: Node2vec_opensource_${dataset_name}\n" > ./report/"Node2vec_${currentTime}.yml"
  if [ $? -eq 0 ];then
    echo "Exec Successful: end." > ./log/log
  else
    echo "Exec Failure: please check the code" > ./log/log
  fi
fi