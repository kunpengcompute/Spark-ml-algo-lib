#!/bin/bash
set -e

function alg_usage() {
 echo "Usage:<dataset name>"
 echo "dataset name: simulate1,simulate2,usaRoad"
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
is_raw="no"
if [ $dataset_name != 'simulate1' ] && [ $dataset_name != 'simulate2' ] && [ $dataset_name != 'usaRoad' ];
then
  echo 'invalid dataset'
  echo "please input dataset name: simulate1 or simulate2 or usaRoad"
  exit 0
fi

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

source conf/graph/cd/cd_spark.properties
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
output_path="/tmp/graph/result/cd/${is_raw}/${dataset_name}"
echo "${dataset_name}: ${input_path},${output_path}"

echo "start to clean exist output"
hdfs dfs -rm -r -f -skipTrash ${output_path}

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

echo "start to submit spark jobs -- cd-${dataset_name}"

scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent1:/opt/graph_classpath/
scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent2:/opt/graph_classpath/
scp lib/fastutil-8.3.1.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent3:/opt/graph_classpath/

spark-submit \
--class com.bigdata.graph.CycleDetectionWithConstrainsRunner \
--deploy-mode ${deploy_mode} \
--driver-memory ${driver_memory} \
--num-executors ${num_executors} \
--executor-cores ${executor_cores} \
--executor-memory ${executor_memory} \
--conf spark.rpc.askTimeout=3600 \
--conf spark.scheduler.maxRegisteredResourcesWaitingTime=3600000 \
--conf spark.worker.timeout=3600 \
--conf spark.network.timeout=6000s \
--conf spark.storage.blockManagerSlaveTimeoutMs=600000 \
--conf spark.shuffle.blockTransferService=nio \
--conf spark.driver.maxResultSize=100g \
--conf spark.shuffle.manager=SORT \
--conf spark.broadcast.blockSize=25g \
--conf spark.core.connection.ack.wait.timeout=60000s \
--conf spark.shuffle.memoryFraction=0.6 \
--conf spark.storage.memoryFraction=0.2 \
--conf spark.rdd.compress=true \
--conf spark.executor.memoryOverhead=5g \
--conf "spark.executor.extraJavaOptions=${executor_extra_javaopts}" \
--jars "lib/fastutil-8.3.1.jar,lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
--driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
--conf "spark.executor.extraClassPath=/opt/graph_classpath/fastutil-8.3.1.jar:/opt/graph_classpath/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
./lib/kal-test_${scala_version_val}-0.1.jar ${dataset_name} ${input_path} ${output_path} "run" "no" ${cpu_name} | tee ./log/log
