#!/bin/bash
set -e

function usage() {
  echo "Usage:  <dataset name> <isRaw> <isCheck>"
  echo "1rd argument: name of dataset:Hibench1m_100,Hibench1m_200 "
  echo "2th argument: optimization algorithm or raw: [no/yes]"
  echo "3th argument: Whether to Compare Results [no/yes]"
}

case "$1" in
-h | --help | ?)
  usage
  exit 0
  ;;
esac

if [ $# -ne 3 ]; then
  usage
  exit 0
fi

source conf/ml/hdb/hdb_spark.properties
dataset_name=$1
is_raw=$2
if_check=$3
cpu_name=$(lscpu | grep Architecture | awk '{print $2}')
model_conf=${dataset_name}-${is_raw}-${if_check}

# concatnate strings as a new variable
num_executors=${cpu_name}_${dataset_name}"_numExecutors"
executor_cores=${cpu_name}_${dataset_name}"_executorCores"
executor_memory=${cpu_name}_${dataset_name}"_executorMemory"
executor_extra_java_options=${cpu_name}_${dataset_name}"_extraJavaOptions"
driver_cores=${cpu_name}_${dataset_name}"_driverCores"
driver_memory=${cpu_name}_${dataset_name}"_driverMemory"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
executor_extra_java_options_val=${!executor_extra_java_options}
driver_cores_val=${!driver_cores}
driver_memory_val=${!driver_memory}

echo "master : ${master}"
echo "deployMode : ${deployMode}"
echo "${driver_cores} : ${driver_cores_val}"
echo "${driver_memory} : ${driver_memory_val}"
echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${executor_extra_java_options} : ${executor_extra_java_options_val}"
echo "cpu_name : ${cpu_name}"

if [ ! ${num_executors_val} ] \
    || [ ! ${executor_cores_val} ] \
    || [ ! ${executor_memory_val} ] \
    || [ ! ${executor_extra_java_options_val} ] \
    || [ ! ${driver_cores_val} ] \
    || [ ! ${driver_memory_val} ] \
    || [ ! ${master} ] \
    || [ ! ${deployMode} ] \
    || [ ! ${cpu_name} ]; then
  echo "Some values are NULL, please confirm with the property files"
  exit 0
fi

source conf/ml/ml_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}
kal_version=kalVersion
kal_version_val=${!kal_version}
scala_version=scalaVersion
scala_version_val=${!scala_version}
save_resultPath=saveResultPath
save_resultPath_val=${!save_resultPath}
data_path=${!dataset_name}

echo "${dataset_name} : ${data_path}"

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

echo "start to submit spark jobs --- hdb-${model_conf}"
if [ ${is_raw} == "no" ]; then
  scp lib/fastutil-8.3.1.jar lib/boostkit-ml-acc_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-ml-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent1:/opt/ml_classpath/
  scp lib/fastutil-8.3.1.jar lib/boostkit-ml-acc_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-ml-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent2:/opt/ml_classpath/
  scp lib/fastutil-8.3.1.jar lib/boostkit-ml-acc_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-ml-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent3:/opt/ml_classpath/

  spark-submit \
  --class com.bigdata.ml.HDBRunner \
  --deploy-mode ${deployMode} \
  --driver-cores ${driver_cores_val} \
  --driver-memory ${driver_memory_val} \
  --num-executors ${num_executors_val} \
  --executor-cores ${executor_cores_val} \
  --executor-memory ${executor_memory_val} \
  --master ${master} \
  --conf "spark.rpc.message.maxSize=1024" \
  --conf "spark.driver.maxResultSize=4g" \
  --conf "spark.task.maxFailures=100" \
  --conf "spark.executor.extraJavaOptions=${executor_extra_java_options_val}" \
  --driver-java-options "-Dlog4j.configuration=file:./log4j.properties" \
  --driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/fastutil-8.3.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-ml-acc_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar:lib/boostkit-ml-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar:lib/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar:lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --conf "spark.executor.extraClassPath=/opt/ml_classpath/fastutil-8.3.1.jar:/opt/ml_classpath/boostkit-ml-acc_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar:/opt/ml_classpath/boostkit-ml-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar:/opt/ml_classpath/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar:/opt/ml_classpath/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${model_conf} ${data_path} ${cpu_name} ${save_resultPath_val} | tee ./log/log
else
  spark-submit \
  --class com.bigdata.ml.HDBRunner \
  --deploy-mode ${deployMode} \
  --driver-cores ${driver_cores_val} \
  --driver-memory ${driver_memory_val} \
  --num-executors ${num_executors_val} \
  --executor-cores ${executor_cores_val} \
  --executor-memory ${executor_memory_val} \
  --master ${master} \
  --conf "spark.executor.extraJavaOptions=${executor_extra_java_options_val}" \
  --driver-class-path "lib/snakeyaml-1.19.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${model_conf} ${data_path} ${cpu_name} ${save_resultPath_val} | tee ./log/log
fi