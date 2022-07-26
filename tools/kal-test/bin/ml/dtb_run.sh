#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <dataset name> <api name> <save or verify> <isRaw>"
  echo "1st argument: name of dataset: higgs/mnist8m"
  echo "2nd argument: name of API: fit/fit1/fit2/fit3"
  echo "3rd argument: save or verify result: save/verify"
  echo "4th argument: optimization algorithm or raw: no/yes"
  exit 0
  ;;
esac

if [ $# -ne 4 ]; then
  echo "please input 4 arguments: <dataset name> <api name> <save or verify> <isRaw>"
  echo "1st argument: name of dataset: higgs/mnist8m"
  echo "2nd argument: name of API: fit/fit1/fit2/fit3"
  echo "3rd argument: save or verify result: save/verify"
  echo "4th argument: optimization algorithm or raw: no/yes"
  exit 0
fi

dataset_name=$1
api_name=$2
verify=$3
is_raw=$4
cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

source conf/ml/dtb/dtb_spark.properties
# concatnate strings as a new variable
num_executors="numExectuors_"${dataset_name}_${cpu_name}
executor_cores="executorCores_"${dataset_name}_${cpu_name}
executor_memory="executorMemory_"${dataset_name}_${cpu_name}
extra_java_options="extraJavaOptions_"${dataset_name}_${cpu_name}
driver_cores="driverCores"
driver_memory="driverMemory"
master_="master"
deploy_mode="deployMode"
max_failures="maxFailures"
compress_="compress"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
extra_java_options_val=${!extra_java_options}
driver_cores_val=${!driver_cores}
driver_memory_val=${!driver_memory}
master_val=${!master_}
deploy_mode_val=${!deploy_mode}
max_failures_val=${!max_failures}
compress_val=${!compress_}

echo "${master_} : ${master_val}"
echo "${deploy_mode} : ${deploy_mode_val}"
echo "${driver_cores} : ${driver_cores_val}"
echo "${driver_memory} : ${driver_memory_val}"
echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${extra_java_options} : ${extra_java_options_val}"
echo "${max_failures} : ${max_failures_val}"
echo "${compress_} : ${compress_val}"
echo "cpu_name : ${cpu_name}"

if [ ! ${num_executors_val} ] \
    || [ ! ${executor_cores_val} ] \
    || [ ! ${executor_memory_val} ] \
    || [ ! ${extra_java_options_val} ] \
    || [ ! ${driver_cores_val} ] \
    || [ ! ${driver_memory_val} ] \
    || [ ! ${master_val} ] \
    || [ ! ${max_failures_val} ] \
    || [ ! ${compress_val} ] \
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

data_path_val=${!dataset_name}
echo "${dataset_name} : ${data_path_val}"

bucketedResPath="/tmp/ml/res/DTB_ref_bucketedRes/${spark_version_val}/${dataset_name}"
hdfs dfs -mkdir -p ${bucketedResPath}

spark_conf=${master_val}_${deploy_mode_val}_${num_executors_val}_${executor_cores_val}_${executor_memory_val}

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

model_conf=${dataset_name}-${api_name}-${verify}-${bucketedResPath}
echo "start to submit spark jobs --- dtb-${model_conf}"
if [ ${is_raw} == "no" ]; then
  scp lib/fastutil-8.3.1.jar lib/boostkit-ml-acc_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-ml-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent1:/opt/ml_classpath/
  scp lib/fastutil-8.3.1.jar lib/boostkit-ml-acc_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-ml-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent2:/opt/ml_classpath/
  scp lib/fastutil-8.3.1.jar lib/boostkit-ml-acc_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-ml-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent3:/opt/ml_classpath/

  spark-submit \
  --class com.bigdata.ml.DTBRunner \
  --driver-java-options "-Xms15g" \
  --deploy-mode ${deploy_mode_val} \
  --driver-cores ${driver_cores_val} \
  --driver-memory ${driver_memory_val} \
  --num-executors ${num_executors_val} \
  --executor-cores ${executor_cores_val} \
  --executor-memory ${executor_memory_val} \
  --master ${master_val} \
  --conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
  --conf "spark.executor.instances=${num_executors_val}" \
  --conf "spark.taskmaxFailures=${max_failures_val}" \
  --jars "lib/fastutil-8.3.1.jar,lib/boostkit-ml-acc_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar,lib/boostkit-ml-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar,lib/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/fastutil-8.3.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-ml-acc_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar:lib/boostkit-ml-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar:lib/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --conf "spark.executor.extraClassPath=/opt/ml_classpath/fastutil-8.3.1.jar:/opt/ml_classpath/boostkit-ml-acc_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar:/opt/ml_classpath/boostkit-ml-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar:/opt/ml_classpath/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${model_conf} ${data_path_val} ${cpu_name} ${is_raw} ${spark_conf} | tee ./log/log
else
  scp lib/boostkit-ml-kernel-client_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar root@agent1:/opt/ml_classpath/
  scp lib/boostkit-ml-kernel-client_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar root@agent2:/opt/ml_classpath/
  scp lib/boostkit-ml-kernel-client_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar root@agent3:/opt/ml_classpath/

  spark-submit \
  --class com.bigdata.ml.DTBRunner \
  --driver-java-options "-Xms15g" \
  --deploy-mode ${deploy_mode_val} \
  --driver-cores ${driver_cores_val} \
  --driver-memory ${driver_memory_val} \
  --num-executors ${num_executors_val} \
  --executor-cores ${executor_cores_val} \
  --executor-memory ${executor_memory_val} \
  --master ${master_val} \
  --conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
  --conf "spark.executor.instances=${num_executors_val}" \
  --conf "spark.taskmaxFailures=${max_failures_val}" \
  --driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/fastutil-8.3.1.jar:lib/snakeyaml-1.19.jar" \
  --conf "spark.executor.extraClassPath=/opt/ml_classpath/boostkit-ml-kernel-client_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${model_conf} ${data_path_val} ${cpu_name} ${is_raw} ${spark_conf} | tee ./log/log
fi
