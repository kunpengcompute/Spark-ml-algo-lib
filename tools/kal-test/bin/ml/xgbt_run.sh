#!/bin/bash
set -e

function usage() {
  echo "Usage: <dataset name> <algorithm type> <isRaw> <isCheck>"
  echo "1rd argument: name of dataset: e.g. higgs/mnist8m"
  echo "2st argument: type of algorithm: [classification/regression]"
  echo "3th argument: optimization algorithm or raw: [no/yes]"
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

source conf/ml/xgbt/xgbt_spark.properties
dataset_name=$1
algorithm_type=$2
is_raw=$3
if_check=$4
cpu_name=$(lscpu | grep Architecture | awk '{print $2}')
model_conf=${algorithm_type}-${dataset_name}-${is_raw}-${if_check}

# concatnate strings as a new variable
num_executors=${cpu_name}_${algorithm_type}"_"${dataset_name}"_numExecutors"
executor_cores=${cpu_name}_${algorithm_type}"_"${dataset_name}"_executorCores"
executor_memory=${cpu_name}_${algorithm_type}"_"${dataset_name}"_executorMemory"
extra_java_options=${cpu_name}_${algorithm_type}"_"${dataset_name}"_extraJavaOptions"
driver_cores=${cpu_name}_${algorithm_type}"_"${dataset_name}"_driverCores"
driver_memory=${cpu_name}_${algorithm_type}"_"${dataset_name}"_driverMemory"
task_cpus=${cpu_name}_${algorithm_type}"_"${dataset_name}"_taskCpus"
num_partitions=${cpu_name}_${algorithm_type}"_"${dataset_name}"_numPartitions"
master_="master"
deploy_mode="deployMode"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
extra_java_options_val=${!extra_java_options}
driver_cores_val=${!driver_cores}
driver_memory_val=${!driver_memory}
task_cpus_val=${!task_cpus}
num_partitions_val=${!num_partitions}
master_val=${!master_}
deploy_mode_val=${!deploy_mode}


echo "${master_} : ${master_val}"
echo "${deploy_mode} : ${deploy_mode_val}"
echo "${driver_cores} : ${driver_cores_val}"
echo "${driver_memory} : ${driver_memory_val}"
echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${extra_java_options} : ${extra_java_options_val}"
echo "${task_cpus} : ${task_cpus_val}"
echo "${num_partitions} : ${num_partitions_val}"
echo "cpu_name : ${cpu_name}"

if [ ! ${num_executors_val} ] \
    || [ ! ${executor_cores_val} ] \
    || [ ! ${executor_memory_val} ] \
    || [ ! ${extra_java_options_val} ] \
    || [ ! ${driver_cores_val} ] \
    || [ ! ${driver_memory_val} ] \
    || [ ! ${master_val} ] \
    || [ ! ${task_cpus_val} ] \
    || [ ! ${num_partitions_val} ] \
    || [ ! ${deploy_mode_val} ] \
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
data_path_val=${!dataset_name}
echo "${dataset_name} : ${data_path_val}"


echo "start to submit spark jobs"

spark_conf=${master_val}_${deploy_mode_val}_${num_executors_val}_${executor_cores_val}_${executor_memory_val}_${extra_java_options_val}_${driver_cores_val}_${driver_memory_val}_${task_cpus_val}_${num_partitions_val}

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

echo "start to submit spark jobs --- XGBT-${model_conf}"
if [ ${is_raw} == "no" ]; then
  scp lib/snakeyaml-1.19.jar lib/boostkit-xgboost4j-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar lib/boostkit-xgboost4j_${scala_version_val}-${kal_version_val}-${cpu_name}.jar lib/boostkit-xgboost4j-${spark_version_val}_${scala_version_val}-${kal_version_val}-${cpu_name}.jar root@agent1:/opt/ml_classpath/
  scp lib/snakeyaml-1.19.jar lib/boostkit-xgboost4j-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar lib/boostkit-xgboost4j_${scala_version_val}-${kal_version_val}-${cpu_name}.jar lib/boostkit-xgboost4j-${spark_version_val}_${scala_version_val}-${kal_version_val}-${cpu_name}.jar root@agent2:/opt/ml_classpath/
  scp lib/snakeyaml-1.19.jar lib/boostkit-xgboost4j-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar lib/boostkit-xgboost4j_${scala_version_val}-${kal_version_val}-${cpu_name}.jar lib/boostkit-xgboost4j-${spark_version_val}_${scala_version_val}-${kal_version_val}-${cpu_name}.jar root@agent3:/opt/ml_classpath/

  spark-submit \
  --class com.bigdata.ml.XGBTRunner \
  --deploy-mode ${deploy_mode_val} \
  --driver-cores ${driver_cores_val} \
  --driver-memory ${driver_memory_val} \
  --num-executors ${num_executors_val} \
  --executor-cores ${executor_cores_val} \
  --executor-memory ${executor_memory_val} \
  --master ${master_val} \
  --conf spark.task.cpus=${task_cpus_val} \
  --conf spark.executor.extraJavaOptions=${extra_java_options_val} \
  --conf spark.executorEnv.LD_LIBRARY_PATH="./lib/:${LD_LIBRARY_PATH}" \
  --conf spark.executor.extraLibraryPath="./lib" \
  --conf spark.driver.extraLibraryPath="./lib" \
  --files=lib/libboostkit_xgboost_kernel.so  \
  --jars "lib/boostkit-xgboost4j-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar,lib/boostkit-xgboost4j_${scala_version_val}-${kal_version_val}-${cpu_name}.jar,lib/boostkit-xgboost4j-${spark_version_val}_${scala_version_val}-${kal_version_val}-${cpu_name}.jar,lib/snakeyaml-1.19.jar,lib/kal-test_${scala_version_val}-0.1.jar" \
  --driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/boostkit-xgboost4j-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar:lib/boostkit-xgboost4j_${scala_version_val}-${kal_version_val}-${cpu_name}.jar:lib/boostkit-xgboost4j-${spark_version_val}_${scala_version_val}-${kal_version_val}-${cpu_name}.jar:lib/snakeyaml-1.19.jar" \
  --conf "spark.executor.extraClassPath=/opt/ml_classpath/boostkit-xgboost4j-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar:/opt/ml_classpath/boostkit-xgboost4j_${scala_version_val}-${kal_version_val}-${cpu_name}.jar:/opt/ml_classpath/boostkit-xgboost4j-${spark_version_val}_${scala_version_val}-${kal_version_val}-${cpu_name}.jar:/opt/ml_classpath/snakeyaml-1.19.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${model_conf} ${data_path_val} ${cpu_name} ${spark_conf} ${save_resultPath_val} | tee ./log/log
else
  scp lib/snakeyaml-1.19.jar lib/xgboost4j_${scala_version_val}-1.1.0.jar lib/xgboost4j-spark_${scala_version_val}-1.1.0.jar root@agent1:/opt/ml_classpath/
  scp lib/snakeyaml-1.19.jar lib/xgboost4j_${scala_version_val}-1.1.0.jar lib/xgboost4j-spark_${scala_version_val}-1.1.0.jar root@agent2:/opt/ml_classpath/
  scp lib/snakeyaml-1.19.jar lib/xgboost4j_${scala_version_val}-1.1.0.jar lib/xgboost4j-spark_${scala_version_val}-1.1.0.jar root@agent3:/opt/ml_classpath/

  spark-submit \
  --class com.bigdata.ml.XGBTRunner \
  --deploy-mode ${deploy_mode_val} \
  --driver-cores ${driver_cores_val} \
  --driver-memory ${driver_memory_val} \
  --num-executors ${num_executors_val} \
  --executor-cores ${executor_cores_val} \
  --executor-memory ${executor_memory_val} \
  --master ${master_val} \
  --conf spark.task.cpus=${task_cpus_val} \
  --conf spark.executor.extraJavaOptions=${extra_java_options_val} \
  --jars "lib/xgboost4j_${scala_version_val}-1.1.0.jar,lib/xgboost4j-spark_${scala_version_val}-1.1.0.jar,lib/snakeyaml-1.19.jar,lib/kal-test_${scala_version_val}-0.1.jar" \
  --driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/xgboost4j_${scala_version_val}-1.1.0.jar:lib/xgboost4j-spark_${scala_version_val}-1.1.0.jar:lib/snakeyaml-1.19.jar" \
  --conf "spark.executor.extraClassPath=/opt/ml_classpath/xgboost4j_${scala_version_val}-1.1.0.jar:/opt/ml_classpath/xgboost4j-spark_${scala_version_val}-1.1.0.jar:/opt/ml_classpath/snakeyaml-1.19.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${model_conf} ${data_path_val} ${cpu_name} ${spark_conf} ${save_resultPath_val} | tee ./log/log
fi