#!/bin/bash
set -e

function alg_usage() {
  echo "Usage:<dataset name>"
 	echo "1st argument: name of dataset: cit_patents,enwiki_2018,arabic_2005,graph500_22,graph500_23,graph500_25"
  echo "2nd argument: optimization algorithm or raw: no/yes"
}

case "$1" in
-h | --help | ?)
  alg_usage
  exit 0
  ;;
esac

if [ $# -ne 2 ];then
  alg_usage
	exit 0
fi

source conf/graph/bfs/bfs_spark.properties
source conf/graph/bfs/bfs_source_id.properties

dataset_name=$1
is_raw=$2

if [ ${dataset_name} != "cit_patents" ] &&
   [ ${dataset_name} != "enwiki_2018" ] &&
   [ ${dataset_name} != "arabic_2005" ] &&
   [ ${dataset_name} != "graph500_22" ] &&
   [ ${dataset_name} != "graph500_23" ] &&
   [ ${dataset_name} != "graph500_25" ] ;then
  echo "invalid dataset name,dataset name:cit_patents,enwiki_2018,arabic_2005,graph500_22,graph500_23,graph500_25"
  exit 1
fi

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

# concatnate strings as a new variable
num_executors="${dataset_name}_numExecutors_${cpu_name}"
executor_cores="${dataset_name}_executorCores_${cpu_name}"
executor_memory="${dataset_name}_executorMemory_${cpu_name}"
num_partitions="${dataset_name}_numPartitions_${cpu_name}"
deploy_mode="deployMode"

num_executors_val=${!num_executors}
executor_cores_val=${!executor_cores}
executor_memory_val=${!executor_memory}
deploy_mode_val=${!deploy_mode}
num_partitions_val=${!num_partitions}
extra_java_options_val="-Xms${executor_memory_val}"

echo "${num_executors} : ${num_executors_val}"
echo "${executor_cores}: ${executor_cores_val}"
echo "${executor_memory} : ${executor_memory_val}"
echo "${deploy_mode} : ${deploy_mode_val}"
echo "${num_partitions} : ${num_partitions_val}"
echo "extra_java_options_val : ${extra_java_options_val}"

if [ ! ${num_executors_val} ] ||
  [ ! ${executor_cores_val} ] ||
  [ ! ${executor_memory_val} ] ||
  [ ! ${extra_java_options_val} ] ||
  [ ! ${num_partitions_val} ]; then
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
output_path="${output_path_prefix}/bfs/${is_raw}/${dataset_name}"

data_path_val=${!dataset_name}
echo "${dataset_name} : ${data_path_val}"
echo "output_path : ${output_path}"

source_ids="${dataset_name}_SourceID"
source_ids_val=${!source_ids}
source_ids_arr=($source_ids_val)
echo "${source_ids}:${source_ids_val}"

if [ ${is_raw} != "no" ]; then
  split="${dataset_name}_split"
  q="${dataset_name}_q"
  split_val=${!split}
  q_val=${!q}
  echo "${split}: ${split_val}"
  echo "${q}: ${q_val}"
fi

for source_id in ${source_ids_arr[@]}
do
  hdfs dfs -rm -r -f "${output_path}_${source_id}"

  echo "start to clean cache and sleep 30s"
  ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
  ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
  ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
  ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
  sleep 30

  echo "start to submit spark jobs -- bfs-${dataset_name}_${source_id}"
  if [ ${is_raw} == "no" ]; then
    scp lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent1:/opt/graph_classpath/
    scp lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent2:/opt/graph_classpath/
    scp lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar root@agent3:/opt/graph_classpath/

    spark-submit \
    --class com.bigdata.graph.BFSRunner \
    --master yarn \
    --deploy-mode ${deploy_mode_val} \
    --num-executors ${num_executors_val} \
    --executor-memory ${executor_memory_val} \
    --executor-cores ${executor_cores_val} \
    --driver-memory 200g \
    --conf "spark.driver.maxResultSize=200g" \
    --conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
    --conf "spark.driver.extraJavaOptions=-Xms200g" \
    --conf spark.locality.wait.node=0 \
    --jars "lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
    --driver-class-path "lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
    --conf "spark.executor.extraClassPath=/opt/graph_classpath/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
    ./lib/kal-test_${scala_version_val}-0.1.jar ${dataset_name} ${source_id} ${num_partitions_val} ${is_raw} ${data_path_val} "${output_path}_${source_id}" | tee ./log/log
  else
    spark-submit \
    --class Driver \
    --master yarn \
    --deploy-mode ${deploy_mode_val} \
    --num-executors ${num_executors_val} \
    --executor-memory ${executor_memory_val} \
    --executor-cores ${executor_cores_val} \
    --driver-memory 80g \
    --conf "spark.driver.maxResultSize=80g" \
    --conf "spark.executor.extraJavaOptions=${extra_java_options_val}" \
    --conf "spark.driver.extraJavaOptions=-Xms80g" \
    --conf spark.locality.wait.node=0 \
    --jars "lib/scopt_2.10-3.5.0.jar" \
    --driver-class-path "lib/scopt_2.10-3.5.0.jar" \
    --conf "spark.executor.extraClassPath=scopt_2.10-3.5.0.jar" \
    ./lib/bfs_2.10-0.1.2.jar \
    -g 'EdgeList' \
    -p 'EdgePartition2D' \
    -n ${num_partitions_val} \
    -i ${source_id} \
    -d 10 \
    -l 'yarn' \
    -f "${data_path_val}" \
    -t "${split_val}" \
    -o "${output_path}_${source_id}" \
    -z ${num_partitions_val} \
    -q ${q_val} > bfs_temp_1.log

    cat bfs_temp_1.log | grep "BFS: Time:" > bfs_temp_2.log
    if [ -s bfs_temp_2.log ];then
      echo "start to report result"
    else
      exit 1
    fi

    while read line
    do
      echo $line
      IFS=" "
      line_arr=($line)
      time_temp=${line_arr[${#line_arr[@]}-1]}
      bfs_time=$(echo -e "scale=4;${time_temp}/1000" | bc)
      echo "CostTime(s): ${bfs_time}"
      if [ ! -d "./report" ]; then
        mkdir report
      fi
      currentTime=$(date "+%Y%m%d_%H%M%S")
      echo -e "algorithmName: BFS\ncostTime: $bfs_time\ndatasetName: ${dataset_name}\nisRaw: 'yes'\nsourceID: ${source_id}\ntestcaseType: BFS_opensource_${dataset_name}_${source_id}\n" > ./report/"BFS_${currentTime}.yml"
    done < bfs_temp_2.log
    rm -rf "bfs_temp_1.log"
    rm -rf "bfs_temp_2.log"
  fi
done
