#!/bin/bash
set -e

function usage() {
  echo "Usage: <dataset name> <isRaw>"
  echo "1st argument: name of dataset: e.g. bremenSmall/farm/house"
  echo "2nd argument: optimization algorithm or raw: [no/yes]"
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

source conf/ml/dbscan/dbscan_spark.properties
dataset_name=$1
is_raw=$2
cpu_name=$(lscpu | grep Architecture | awk '{print $2}')
model_conf=${dataset_name}-${is_raw}
type=opt
if [ $is_raw == "yes" ]; then
  type=raw
fi

# concatnate strings as a new variable
num_executors="numExectuors_"${type}
executor_cores="executorCores_"${type}
executor_memory="executorMemory_"${type}
extra_java_options="extraJavaOptions_"${type}
driver_cores="driverCores_"${type}
driver_memory="driverMemory_"${type}
master_="master"
deploy_mode="deployMode"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
extra_java_options_val=${!extra_java_options}
driver_cores_val=${!driver_cores}
driver_memory_val=${!driver_memory}
master_val=${!master_}
deploy_mode_val=${!deploy_mode}

echo ${cpu_name}
echo "${master_} : ${master_val}"
echo "${deploy_mode} : ${deploy_mode_val}"
echo "${driver_cores} : ${driver_cores_val}"
echo "${driver_memory} : ${driver_memory_val}"
echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${extra_java_options} : ${extra_java_options_val}"

if [ ! ${num_executors_val} ] \
    || [ ! ${executor_cores_val} ] \
    || [ ! ${executor_memory_val} ] \
    || [ ! ${extra_java_options_val} ] \
    || [ ! ${driver_cores_val} ] \
    || [ ! ${driver_memory_val} ] \
    || [ ! ${master_val} ]; then
  echo "Some values are NULL, please confirm with the property files"
  exit 0
fi



if [ ${is_raw} == "yes" ]; then
  driver_max_result_size="driverMaxResultSize_"${type}
  epsilon="epsilon_"${dataset_name}_${type}
  min_points="minPoints_"${dataset_name}_${type}

  driver_max_result_size_val=${!driver_max_result_size}
  epsilon_val=${!epsilon}
  min_points_val=${!min_points}

  echo "${driver_max_result_size} : ${driver_max_result_size_val}"
  echo "${epsilon} : ${epsilon_val}"
  echo "${min_points} : ${min_points_val}"


  if [ ! ${driver_max_result_size_val} ] \
      || [ ! ${epsilon_val} ] \
      || [ ! ${min_points_val} ]; then
    echo "Some values are NULL, please confirm with the property files"
    exit 0
  fi
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

outputPath="${save_resultPath_val}/dbscan/alitoukaDBSCAN/output_${dataset_name}"
hdfsJarPath="hdfs:///tmp/ml/test/dbscan"

echo "start to clean cache and sleep 30s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 30

mkdir -p log
echo "start to submit spark jobs --- DBSCAN-${model_conf}"
if [ ${is_raw} == "no" ]; then
  scp lib/fastutil-8.3.1.jar lib/boostkit-ml-acc_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-ml-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent1:/opt/ml_classpath/
  scp lib/fastutil-8.3.1.jar lib/boostkit-ml-acc_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-ml-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent2:/opt/ml_classpath/
  scp lib/fastutil-8.3.1.jar lib/boostkit-ml-acc_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-ml-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar lib/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent3:/opt/ml_classpath/

  spark-submit \
  --class org.apache.spark.ml.clustering.DBSCANRunner \
  --deploy-mode ${deploy_mode_val} \
  --driver-cores ${driver_cores_val} \
  --driver-memory ${driver_memory_val} \
  --num-executors ${num_executors_val} \
  --executor-cores ${executor_cores_val} \
  --executor-memory ${executor_memory_val} \
  --master ${master_val} \
  --conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
  --conf "spark.task.maxFailures=1" \
  --jars "lib/fastutil-8.3.1.jar,lib/boostkit-ml-acc_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar,lib/boostkit-ml-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar,lib/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --driver-class-path "lib/fastutil-8.3.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-ml-acc_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar:lib/boostkit-ml-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar:lib/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  --conf "spark.executor.extraClassPath=/opt/ml_classpath/fastutil-8.3.1.jar:/opt/ml_classpath/boostkit-ml-acc_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar:/opt/ml_classpath/boostkit-ml-core_${scala_version_val}-${kal_version_val}-${spark_version_val}.jar:/opt/ml_classpath/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar ${model_conf} ${data_path_val} | tee ./log/log
else
  hdfs dfs -rm -r -f ${outputPath}
  hdfs dfs -mkdir -p ${hdfsJarPath}
  hdfs dfs -ls ${hdfsJarPath}
  if [ $? -eq 0 ];then
    hdfs dfs -rm -r -f ${hdfsJarPath}/alitouka_dbscan_2.11-0.1.jar
    hdfs dfs -put ./lib/alitouka_dbscan_2.11-0.1.jar ${hdfsJarPath}
  fi

  spark-submit \
  --jars "lib/scopt_2.11-3.5.0.jar" \
  --class org.alitouka.spark.dbscan.DbscanDriver \
  --deploy-mode ${deploy_mode_val} \
  --name "alitouka_DBSCAN_${model_conf}" \
  --driver-cores ${driver_cores_val} \
  --driver-memory ${driver_memory_val} \
  --num-executors ${num_executors_val} \
  --executor-cores ${executor_cores_val} \
  --executor-memory ${executor_memory_val} \
  --master ${master_val} \
  --conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
  --conf "spark.driver.maxResultSize=${driver_max_result_size_val}" \
  ${hdfsJarPath}/alitouka_dbscan_2.11-0.1.jar --ds-master ${master_val} --ds-jar ${hdfsJarPath}/alitouka_dbscan_${scala_version_val}-0.1.jar --ds-input ${data_path_val} --ds-output ${outputPath} --eps ${epsilon_val} --numPts ${min_points_val} >dbscan_tmp.log
  CostTime=$(cat dbscan_tmp.log | grep "train total" | awk '{print $3}')
  currentTime=$(date "+%Y%m%d_%H%M%S")
  rm -rf dbscan_tmp.log
  echo -e "algorithmName: DBSCAN\ncostTime: ${CostTime}\ndatasetName: ${dataset_name}\nisRaw: 'yes'\ntestcaseType: DBSCAN_opensource_${dataset_name}\n" > ./report/"DBSCAN_${dataset_name}_raw_${currentTime}.yml"
  echo "Exec Successful: costTime: ${CostTime}" > ./log/log
fi

