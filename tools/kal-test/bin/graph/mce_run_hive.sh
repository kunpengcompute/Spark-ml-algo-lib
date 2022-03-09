#!/bin/bash
set -e
case "$1" in
-h | --help | ?)
 echo "Usage:<table name> <col1> <col2> <minK> <maxDegree> <partition> <save_mode> <save_arg>"
 exit 0
 ;;
esac

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')
if [ ${cpu_name} == "aarch64" ]
then
  cpu_name="aarch_64"
fi

source conf/graph/graph_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}

table_name=$1
col1=$2
col2=$3
minK=$4
maxDegree=$5
partition=$6
save_mode=$7
save_arg=$8

echo "table_name: $table_name"
echo "col1: $col1"
echo "col2: $col2"
echo "minK: $minK"
echo "maxDegree: $maxDegree"
echo "partition: $partition"
echo "save_mode: $save_mode"
echo "save_arg: $save_arg"

spark-submit \
--class com.bigdata.graph.MaximalCliqueEnumerationHiveRunner \
--master yarn \
--deploy-mode "client" \
--driver-memory "80g" \
--num-executors 59 \
--executor-cores 4 \
--executor-memory "15g" \
--conf spark.locality.wait=10 \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.rdd.compress=true \
--conf spark.shuffle.compress=true \
--conf spark.shuffle.spill.compress=true \
--conf spark.io.compression.codec=lz4 \
--jars "lib/boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
--driver-class-path "lib/kal-test_2.11-0.1.jar:lib/boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
--conf "spark.executor.extraClassPath=boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
./lib/kal-test_2.11-0.1.jar ${table_name} ${col1} ${col2} ${minK} ${maxDegree} ${partition} ${save_mode} ${save_arg}





