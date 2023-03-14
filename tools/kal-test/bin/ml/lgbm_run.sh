#!/bin/bash
set -e

function usage() {
  echo "Usage:  <dataset name> <isRaw> <isCheck>"
  echo "1st argument: type of algorithm: [classification/regression]"
  echo "2nd argument: name of dataset:mnist8m, higgs "
  echo "3rd argument: optimization algorithm or raw: [no/yes]"
  echo "4th argument: Whether to Compare Results [no/yes]"
}

case "$1" in
-h | --help | ?)
  usage
  exit 0
  ;;
esac

if [ $# -ne 4 ]; then
  usage
  exit 0
fi

source conf/ml/lgbm/lgbm_spark.properties
algorithm_type=$1
dataset_name=$2
is_raw=$3
if_check=$4
cpu_name=$(lscpu | grep Architecture | awk '{print $2}')
model_conf=${algorithm_type}-${dataset_name}-${is_raw}-${if_check}

# concatnate strings as a new variable
num_executors=${cpu_name}_${dataset_name}"_numExecutors"
executor_cores=${cpu_name}_${dataset_name}"_executorCores"
executor_memory=${cpu_name}_${dataset_name}"_executorMemory"
executor_extra_java_options=${cpu_name}_${dataset_name}"_extraJavaOptions"
executor_memory_overhead=${cpu_name}_${dataset_name}"_executorMemOverhead"
driver_cores=${cpu_name}_${dataset_name}"_driverCores"
driver_memory=${cpu_name}_${dataset_name}"_driverMemory"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
executor_extra_java_options_val=${!executor_extra_java_options}
executor_memory_overhead_val=${!executor_memory_overhead}
driver_cores_val=${!driver_cores}
driver_memory_val=${!driver_memory}

echo "master : ${master}"
echo "deployMode : ${deployMode}"
echo "${driver_cores} : ${driver_cores_val}"
echo "${driver_memory} : ${driver_memory_val}"
echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${executor_memory_overhead} : ${executor_memory_overhead_val}"
echo "${executor_extra_java_options} : ${executor_extra_java_options_val}"
echo "cpu_name : ${cpu_name}"

if [ ! ${num_executors_val} ] \
    || [ ! ${executor_cores_val} ] \
    || [ ! ${executor_memory_val} ] \
    || [ ! ${executor_memory_overhead_val} ] \
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

echo "start to submit spark jobs --- lgbm-${model_conf}"
if [ ${is_raw} == "no" ]; then
  scp lib/boostkit-lightgbmlib-${kal_version_val}.jar lib/boostkit-mmlspark-${scala_version_val}-${spark_version_val}-${kal_version_val}.jar lib/boostkit-lightgbm-kernel-${scala_version_val}-${spark_version_val}-${kal_version_val}.jar lib/fastutil-8.3.1.jar root@agent1:/opt/ml_classpath/
  scp lib/boostkit-lightgbmlib-${kal_version_val}.jar lib/boostkit-mmlspark-${scala_version_val}-${spark_version_val}-${kal_version_val}.jar lib/boostkit-lightgbm-kernel-${scala_version_val}-${spark_version_val}-${kal_version_val}.jar lib/fastutil-8.3.1.jar root@agent2:/opt/ml_classpath/
  scp lib/boostkit-lightgbmlib-${kal_version_val}.jar lib/boostkit-mmlspark-${scala_version_val}-${spark_version_val}-${kal_version_val}.jar lib/boostkit-lightgbm-kernel-${scala_version_val}-${spark_version_val}-${kal_version_val}.jar lib/fastutil-8.3.1.jar root@agent3:/opt/ml_classpath/

  spark-submit \
  --class com.bigdata.ml.LightGBMRunner \
  --deploy-mode ${deployMode} \
  --driver-cores ${driver_cores_val} \
  --driver-memory ${driver_memory_val} \
  --num-executors ${num_executors_val} \
  --executor-cores ${executor_cores_val} \
  --executor-memory ${executor_memory_val} \
  --conf "spark.executor.memoryOverhead=${executor_memory_overhead_val}" \
  --master ${master} \
  --files=lib/libboostkit_lightgbm_close.so  \
  --conf "spark.executor.extraJavaOptions=${executor_extra_java_options_val}" \
  --jars "lib/boostkit-lightgbmlib-${kal_version_val}.jar,lib/fastutil-8.3.1.jar,lib/boostkit-mmlspark-${scala_version_val}-${spark_version_val}-${kal_version_val}.jar,lib/boostkit-lightgbm-kernel-${scala_version_val}-${spark_version_val}-${kal_version_val}.jar" \
  --driver-class-path "lib/boostkit-lightgbmlib-${kal_version_val}.jar:lib/boostkit-mmlspark-${scala_version_val}-${spark_version_val}-${kal_version_val}.jar:lib/boostkit-lightgbm-kernel-${scala_version_val}-${spark_version_val}-${kal_version_val}.jar:lib/kal-test_${scala_version_val}-0.1.jar:lib/fastutil-8.3.1.jar:lib/snakeyaml-1.19.jar" \
  --conf "spark.executor.extraClassPath=/opt/ml_classpath/boostkit-lightgbmlib-${kal_version_val}.jar:/opt/ml_classpath/boostkit-mmlspark-${scala_version_val}-${spark_version_val}-${kal_version_val}.jar:/opt/ml_classpath/boostkit-lightgbm-kernel-${scala_version_val}-${spark_version_val}-${kal_version_val}.jar:/opt/ml_classpath/fastutil-8.3.1.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${model_conf} ${data_path} ${cpu_name} ${save_resultPath_val} | tee ./log/log
else
  scp lib/boostkit-lightgbmlib-${kal_version_val}.jar lib/mmlspark_2.12_spark3.1.2-0.0.0+79-09152193.jar lib/fastutil-8.3.1.jar root@agent1:/opt/ml_classpath/
  scp lib/boostkit-lightgbmlib-${kal_version_val}.jar lib/mmlspark_2.12_spark3.1.2-0.0.0+79-09152193.jar lib/fastutil-8.3.1.jar root@agent2:/opt/ml_classpath/
  scp lib/boostkit-lightgbmlib-${kal_version_val}.jar lib/mmlspark_2.12_spark3.1.2-0.0.0+79-09152193.jar lib/fastutil-8.3.1.jar root@agent3:/opt/ml_classpath/

  spark-submit \
  --class com.bigdata.ml.LightGBMRawRunner \
  --deploy-mode ${deployMode} \
  --driver-cores ${driver_cores_val} \
  --driver-memory ${driver_memory_val} \
  --num-executors ${num_executors_val} \
  --executor-cores ${executor_cores_val} \
  --executor-memory ${executor_memory_val} \
  --master ${master} \
  --jars "lib/lightgbmlib.jar,lib/snakeyaml-1.19.jar,lib/fastutil-8.3.1.jar,lib/mmlspark_2.12_spark3.1.2-0.0.0+79-09152193.jar" \
  --conf "spark.executor.extraJavaOptions=${executor_extra_java_options_val}" \
  --driver-class-path "lib/lightgbmlib.jar,lib/snakeyaml-1.19.jar,lib/mmlspark_2.12_spark3.1.2-0.0.0+79-09152193.jar" \
  --conf "spark.executor.extraClassPath=/opt/ml_classpath/lightgbmlib.jar:/opt/ml_classpath/mmlspark_2.12_spark3.1.2-0.0.0+79-09152193.jar:/opt/ml_classpath/fastutil-8.3.1.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${model_conf} ${data_path} ${cpu_name} ${save_resultPath_val} | tee ./log/log
fi