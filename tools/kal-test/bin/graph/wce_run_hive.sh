#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
 echo "Usage:<table name> <col1> <col2> <maxIter> <maxDegree> <save_mode> <save_arg>"
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
maxIter=$4
maxDegree=$5
save_mode=$6
save_arg=$7

spark-submit \
--class com.bigdata.graph.WCEHiveRunner \
--driver-memory 80g \
--master yarn \
--num-executors 35 \
--executor-cores 8 \
--executor-memory "25g" \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.rdd.compress=true \
--conf spark.shuffle.compress=true \
--conf spark.shuffle.spill.compress=true \
--conf spark.io.compression.codec=lz4 \
--jars "lib/fastutil-8.3.1.jar,lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
--driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
--conf "spark.executor.extraClassPath=/opt/graph_classpath/fastutil-8.3.1.jar:/opt/graph_classpath/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
./lib/kal-test_${scala_version_val}-0.1.jar ${table_name} ${col1} ${col2} ${maxIter} ${maxDegree} ${save_mode} ${save_arg}
