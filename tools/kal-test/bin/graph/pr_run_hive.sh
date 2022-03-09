#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
 echo "Usage:<table name> <col1> <col2> <api> <tol> <resetProb> <numIter> <partition> <save_mode> <save_arg>"
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
api=$4
tol=$5
resetProb=$6
numIter=$7
partition=$8
save_mode=$9
save_arg=${10}

echo "table_name: $table_name"
echo "col1: $col1"
echo "col2: $col2"
echo "api: $api"
echo "tol: $tol"
echo "resetProb: $resetProb"
echo "numIter: $numIter"
echo "partition: $partition"
echo "save_mode: $save_mode"
echo "save_arg: $save_arg"

spark-submit \
--class com.bigdata.graph.PageRankHiveRunner \
--master yarn \
--deploy-mode "client" \
--num-executors 36 \
--executor-memory "25g" \
--executor-cores 4 \
--driver-memory 100g \
--conf spark.driver.maxResultSize=200g \
--conf spark.driver.extraJavaOptions="-Xms100G" \
--conf spark.locality.wait.node=0 \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.rdd.compress=true \
--conf spark.shuffle.compress=true \
--conf spark.shuffle.spill.compress=true \
--conf spark.io.compression.codec=lz4 \
--jars "lib/fastutil-8.3.1.jar,lib/boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
--driver-class-path "lib/kal-test_2.11-0.1.jar:lib/boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
--conf "spark.executor.extraClassPath=fastutil-8.3.1.jar:boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
./lib/kal-test_2.11-0.1.jar ${table_name} ${col1} ${col2} ${api} ${tol} ${resetProb} ${numIter} ${partition} ${save_mode} ${save_arg}
