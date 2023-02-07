#!/bin/bash
set -e

function alg_usage() {
  echo "Usage: <dataset name> <api name> <isRaw> <sourceCnt>"
  echo "1st argument: name of dataset: cit_patents,uk_2002,arabic_2005"
  echo "2nd argument: name of api: fixMS,fixSS,conSS"
  echo "3rd argument: optimization algorithm or raw: no/yes"
  echo "4th argument: sourceCnt or null: 1,5,10,50,100"
}

case "$1" in
-h | --help | ?)
  alg_usage
  exit 0
  ;;
esac

if [ $# -ne 3 ] && [ $# -ne 4 ];then
  alg_usage
	exit 0
fi

source conf/graph/ppr/ppr_spark.properties

dataset_name=$1
api_name=$2
is_raw=$3

if [ ${dataset_name} != "cit_patents" ] &&
   [ ${dataset_name} != "uk_2002" ] &&
   [ ${dataset_name} != "arabic_2005" ] ;then
  echo "invalid dataset name,dataset name:cit_patents,uk_2002,arabic_2005"
  exit 1
fi
if [ ${api_name} != "fixMS" ] &&
   [ ${api_name} != "fixSS" ] &&
   [ ${api_name} != "conSS" ] ;then
  echo "invalid argument value,api name: fixMS,fixSS,conSS"
  exit 1
fi

if [ $# -eq 4 ]; then
    src=$4
    if [ ${src} != "1" ] &&
       [ ${src} != "5" ] &&
       [ ${src} != "10" ] &&
       [ ${src} != "50" ] &&
       [ ${src} != "100" ] ;then
      echo "invalid argument value,must be: 1, 5, 10, 50 or 100"
      exit 1
    fi
fi

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

# concatnate strings as a new variable
num_executors="${api_name}_${dataset_name}_numExecutors_${cpu_name}"
executor_cores="${api_name}_${dataset_name}_executorCores_${cpu_name}"
executor_memory="${api_name}_${dataset_name}_executorMemory_${cpu_name}"
num_partitions="${api_name}_${dataset_name}_numPartitions_${cpu_name}"
extra_Java_Options="${api_name}_${dataset_name}_extraJavaOptions_${cpu_name}"
deploy_mode="deployMode"
driver_memory="driverMemory"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
deploy_mode_val=${!deploy_mode}
num_partitions_val=${!num_partitions}
driver_memory_val=${!driver_memory}
extra_Java_Options_val=${!extra_Java_Options}

echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${deploy_mode} : ${deploy_mode_val}"
echo "${num_partitions} : ${num_partitions_val}"
echo "${driver_memory}:${driver_memory_val}"

if [ ! ${num_executors_val} ] ||
  [ ! ${executor_cores_val} ] ||
  [ ! ${executor_memory_val} ] ||
  [ ! ${extra_Java_Options_val} ] ||
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
echo "${dataset_name} : ${data_path_val}"

function clean_cache() {
  echo "start to clean cache and sleep 30s"
  ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
  ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
  ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
  ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
  sleep 30
}

if [ ${is_raw} == "no" ]; then
  scp lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent1:/opt/graph_classpath/
  scp lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent2:/opt/graph_classpath/
  scp lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent3:/opt/graph_classpath/
fi

if [ ${api_name} == "fixMS" ]; then
  output_path="${output_path_prefix}/ppr/${is_raw}/${dataset_name}/${api_name}_${src}"
  hdfs dfs -rm -r -f ${output_path}
  clean_cache
  echo "start to submit spark jobs -- ppr-${api_name}_${dataset_name}_${src}"
  if [ ${is_raw} == "no" ]; then
    spark-submit \
    --class com.bigdata.graph.PersonalizedPageRankRunner \
    --master yarn \
    --deploy-mode ${deploy_mode_val} \
    --num-executors ${num_executors_val} \
    --executor-memory ${executor_memory_val} \
    --executor-cores ${executor_cores_val} \
    --driver-memory ${driver_memory_val} \
    --conf spark.executor.extraJavaOptions=${extra_Java_Options_val} \
    --conf spark.driver.maxResultSize=80g \
    --conf spark.driver.extraJavaOptions="-Xms80G" \
    --conf spark.locality.wait.node=0 \
    --jars "lib/fastutil-8.3.1.jar,lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
    --driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
    --conf "spark.executor.extraClassPath=/opt/graph_classpath/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
    ./lib/kal-test_${scala_version_val}-0.1.jar ${dataset_name} ${api_name} ${num_partitions_val} ${data_path_val} ${is_raw} ${src} ${output_path} | tee ./log/log
  else
    spark-submit \
    --class com.bigdata.graph.PersonalizedPageRankRunner \
    --master yarn \
    --deploy-mode ${deploy_mode_val} \
    --num-executors ${num_executors_val} \
    --executor-memory ${executor_memory_val} \
    --executor-cores ${executor_cores_val} \
    --driver-memory ${driver_memory_val} \
    --conf spark.executor.extraJavaOptions=${extra_Java_Options_val} \
    --conf spark.driver.maxResultSize=80g \
    --conf spark.driver.extraJavaOptions="-Xms80G" \
    --conf spark.locality.wait.node=0 \
    --jars "lib/fastutil-8.3.1.jar" \
    --driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/snakeyaml-1.19.jar" \
    --conf "spark.executor.extraClassPath=fastutil-8.3.1.jar" \
    ./lib/kal-test_${scala_version_val}-0.1.jar ${dataset_name} ${api_name} ${num_partitions_val} ${data_path_val} ${is_raw} ${src} ${output_path} | tee ./log/log
  fi
else
  source conf/graph/ppr/ppr_source_id.properties
  IFS=,
  source_ids="${dataset_name}_SourceID"
  source_ids_val=${!source_ids}
  source_ids_arr=($source_ids_val)

  echo "${source_ids}:${source_ids_val}"
  for source_id in ${source_ids_arr[@]}
  do
    output_path="${output_path_prefix}/ppr/${is_raw}/${dataset_name}/${api_name}_${source_id}"
    hadoop fs -rm -r -f ${output_path}
    clean_cache
    echo "start to submit spark jobs -- ppr-${api_name}_${dataset_name}_${source_id}"
    if [ ${is_raw} == "no" ]; then
      spark-submit \
      --class com.bigdata.graph.PersonalizedPageRankRunner \
      --master yarn \
      --deploy-mode ${deploy_mode_val} \
      --num-executors ${num_executors_val} \
      --executor-memory ${executor_memory_val} \
      --executor-cores ${executor_cores_val} \
      --driver-memory ${driver_memory_val} \
      --conf spark.executor.extraJavaOptions=${extra_Java_Options_val} \
      --conf spark.driver.maxResultSize=80g \
      --conf spark.driver.extraJavaOptions="-Xms80G" \
      --conf spark.locality.wait.node=0 \
      --jars "lib/fastutil-8.3.1.jar,lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
      --driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
      --conf "spark.executor.extraClassPath=/opt/graph_classpath/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
      ./lib/kal-test_${scala_version_val}-0.1.jar ${dataset_name} ${api_name} ${num_partitions_val} ${data_path_val} ${is_raw} ${source_id} ${output_path} | tee ./log/log
    else
      spark-submit \
      --class com.bigdata.graph.PersonalizedPageRankRunner \
      --master yarn \
      --deploy-mode ${deploy_mode_val} \
      --num-executors ${num_executors_val} \
      --executor-memory ${executor_memory_val} \
      --executor-cores ${executor_cores_val} \
      --driver-memory ${driver_memory_val} \
      --conf spark.executor.extraJavaOptions=${extra_Java_Options_val} \
      --conf spark.driver.maxResultSize=80g \
      --conf spark.driver.extraJavaOptions="-Xms80G" \
      --conf spark.locality.wait.node=0 \
      --jars "lib/fastutil-8.3.1.jar" \
      --driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/snakeyaml-1.19.jar" \
      --conf "spark.executor.extraClassPath=fastutil-8.3.1.jar" \
      ./lib/kal-test_${scala_version_val}-0.1.jar ${dataset_name} ${api_name} ${num_partitions_val} ${data_path_val} ${is_raw} ${source_id} ${output_path} | tee ./log/log
    fi
  done
fi
