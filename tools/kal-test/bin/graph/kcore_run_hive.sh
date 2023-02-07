#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
 echo "Usage:<table name> <col1> <col2> <partition> <save_mode> <save_arg>"
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
kal_version=kalVersion
kal_version_val=${!kal_version}
scala_version=scalaVersion
scala_version_val=${!scala_version}

table_name=$1
col1=$2
col2=$3
partition=$4
save_mode=$5
save_arg=$6

echo "table_name: $table_name"
echo "col1: $col1"
echo "col2: $col2"
echo "partition: $partition"
echo "save_mode: $save_mode"
echo "save_arg: $save_arg"

spark-submit \
--class com.bigdata.graph.KCoreDecompositionHiveRunner \
--deploy-mode "client" \
--driver-memory "16g" \
--num-executors 35 \
--executor-cores 4 \
--executor-memory "25g" \
--conf spark.driver.maxResultSize=200g \
--conf spark.locality.wait.node=0 \
--conf spark.executor.memoryOverhead=10240 \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.rdd.compress=true \
--conf spark.shuffle.compress=true \
--conf spark.shuffle.spill.compress=true \
--conf spark.io.compression.codec=lz4 \
--jars "./lib/kal-test_${scala_version_val}-0.1.jar:lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
--driver-class-path "lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
--conf "spark.executor.extraClassPath=/opt/graph_classpath/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
./lib/kal-test_${scala_version_val}-0.1.jar ${table_name} ${col1} ${col2} ${partition} ${save_mode} ${save_arg}
