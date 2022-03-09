#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
 echo "Usage:<table name> <col1> <col2> <colWeight> <iterNum> <isDirected> <partition> <save_mode> <save_arg>"
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
colWeight=$4
iterNum=$5
isDirected=$6
partition=$7
save_mode=$8
save_arg=$9

echo "table_name: $table_name"
echo "col1: $col1"
echo "colWeight: $colWeight"
echo "iterNum: $iterNum"
echo "isDirected: $isDirected"
echo "partition: $partition"
echo "save_mode: $save_mode"
echo "save_arg: $save_arg"

spark-submit \
--class com.bigdata.graph.LouvainHiveRunner \
--master yarn \
--deploy-mode "client" \
--num-executors 35 \
--executor-memory "25g"  \
--executor-cores 8 \
--driver-memory "16g" \
--conf spark.scheduler.maxRegisteredResourcesWaitingTime=3600000 \
--conf spark.worker.timeout=3600 \
--conf spark.driver.maxResultSize=200g \
--conf spark.rpc.askTimeout=36000 \
--conf spark.network.timeout=6000s \
--conf spark.broadcast.blockSize=4m \
--conf spark.shuffle.manager=SORT \
--conf spark.shuffle.blockTransferService=nio \
--conf spark.locality.wait.node=0 \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.rdd.compress=true \
--conf spark.shuffle.compress=true \
--conf spark.shuffle.spill.compress=true \
--conf spark.io.compression.codec=lz4 \
--jars "lib/fastutil-8.3.1.jar,lib/boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
--driver-class-path "lib/kal-test_2.11-0.1.jar:lib/boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
--conf "spark.executor.extraClassPath=fastutil-8.3.1.jar:boostkit-graph-kernel-2.11-1.3.0-${spark_version_val}-${cpu_name}.jar" \
./lib/kal-test_2.11-0.1.jar ${table_name} ${col1} ${col2} ${colWeight} ${iterNum} ${isDirected} ${partition} ${save_mode} ${save_arg}
