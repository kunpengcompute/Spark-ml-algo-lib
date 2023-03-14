#!/bin/bash
set -e

function alg_usage() {
  echo "Usage: <dataset name> <api name> <weight or not> <isRaw>"
  echo "1st argument: name of dataset: name of dataset: cit_patents,uk_2002,arabic_2005,graph500_22,graph500_23,graph500_24,graph500_25"
  echo "2nd argument: name of api: lcc,avgcc,globalcc"
  echo "3nd argument: weight or not: weighted,unweighted"
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

source conf/graph/clusteringcoefficient/clusteringcoefficient_spark.properties

dataset_name=$1
api_name=$2
weight=$3
is_raw=$4

if [ ${dataset_name} != "cit_patents" ] &&
   [ ${dataset_name} != "uk_2002" ] &&
   [ ${dataset_name} != "arabic_2005" ] &&
   [ ${dataset_name} != "graph500_22" ] &&
   [ ${dataset_name} != "graph500_23" ] &&
   [ ${dataset_name} != "graph500_24" ] &&
   [ ${dataset_name} != "graph500_25" ] ;then
  echo "invalid dataset name,dataset name:cit_patents,uk_2002,arabic_2005,graph500_22,graph500_23,graph500_24,graph500_25"
  exit 1
fi
if [ ${api_name} != "lcc" ] &&
   [ ${api_name} != "avgcc" ] &&
   [ ${api_name} != "globalcc" ] ;then
  echo "invalid argument value,api name: lcc,avgcc,globalcc"
  exit 1
fi
if [ ${weight} != "weighted" ] && [ ${weight} != "unweighted" ];then
  echo "invalid argument value,must be: weighted or unweighted"
  exit 1
fi

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

# concatnate strings as a new variable
num_executors="${dataset_name}_numExecutors_${cpu_name}"
executor_cores="${dataset_name}_executorCores_${cpu_name}"
executor_memory="${dataset_name}_executorMemory_${cpu_name}"
num_partitions="${dataset_name}_numPartitions_${cpu_name}"
deploy_mode="deployMode"
driver_memory="driverMemory"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
deploy_mode_val=${!deploy_mode}
num_partitions_val=${!num_partitions}
driver_memory_val=${!driver_memory}

echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${deploy_mode} : ${deploy_mode_val}"
echo "${num_partitions} : ${num_partitions_val}"
echo "${driver_memory}:${driver_memory_val}"

if [ ! ${num_executors_val} ] ||
  [ ! ${executor_cores_val} ] ||
  [ ! ${executor_memory_val} ] ||
  [ ! ${num_partitions_val} ] ; then
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
output_path="${output_path_prefix}/clusteringcoefficient/${is_raw}/${api_name}/${dataset_name}_${weight}"
echo "${dataset_name} : ${data_path_val}"
echo "output_path : ${output_path}"
hdfs dfs -rm -r -f ${output_path}

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

echo "start to submit spark jobs -- clusteringcoefficient-${api_name}-${weight}-${dataset_name}"
if [ ${is_raw} == "no" ]; then
  scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent1:/opt/graph_classpath/
  scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent2:/opt/graph_classpath/
  scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent3:/opt/graph_classpath/

  spark-submit \
  --class com.bigdata.graph.ClusteringCoefficientRunner \
  --master yarn \
  --deploy-mode ${deploy_mode_val} \
  --num-executors ${num_executors_val} \
  --executor-memory ${executor_memory_val} \
  --executor-cores ${executor_cores_val} \
  --driver-memory ${driver_memory_val} \
  --conf spark.executor.memoryOverhead=2048 \
  --conf spark.executor.extraJavaOptions="-Xms12g" \
  --jars "lib/fastutil-8.3.1.jar,lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --conf "spark.executor.extraClassPath=/opt/graph_classpath/fastutil-8.3.1.jar:/opt/graph_classpath/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${dataset_name} ${num_partitions_val} ${weight} ${is_raw} ${data_path_val} ${api_name} ${output_path} | tee ./log/log
else
  scp lib/lcc_kaiyuan.jar root@agent1:/opt/graph_classpath/
  scp lib/lcc_kaiyuan.jar root@agent2:/opt/graph_classpath/
  scp lib/lcc_kaiyuan.jar root@agent3:/opt/graph_classpath/

  spark-submit \
  --class com.bigdata.graph.ClusteringCoefficientRawRunner \
  --master yarn \
  --deploy-mode ${deploy_mode_val} \
  --name "clusteringcoefficient_${dataset_name}_${api_name}" \
  --num-executors ${num_executors_val} \
  --executor-memory ${executor_memory_val} \
  --executor-cores ${executor_cores_val} \
  --driver-memory ${driver_memory_val} \
  --conf spark.rpc.askTimeout=36000 \
  --conf spark.scheduler.maxRegisteredResourcesWaitingTime=3600000 \
  --conf spark.worker.timeout=3600 \
  --conf spark.network.timeout=6000s \
  --conf spark.storage.blockManagerSlaveTimeoutMs=600000 \
  --conf spark.shuffle.blockTransferService=nio \
  --conf spark.driver.maxResultSize=100g \
  --conf spark.shuffle.manager=SORT \
  --conf spark.broadcast.blockSize=25g \
  --conf spark.network.timeout=1200s \
  --conf spark.rpc.message.maxSize=2046 \
  --conf spark.core.connection.ack.wait.timeout=60000s \
  --conf spark.executor.extraJavaOptions="-Xms35g" \
  --conf spark.rdd.compress=true \
  --driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/snakeyaml-1.19.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${dataset_name} ${num_partitions_val} ${weight} ${is_raw} ${data_path_val} ${api_name} ${output_path} | tee ./log/log
fi