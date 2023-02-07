#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <path0> <path1> <partition num> <split>"
  echo "1st argument: path of dataset"
  echo "2nd argument: path of baseline result"
  echo "3rd argument: path of algorithm result"
  exit 0
  ;;
esac

input=$1
path0=$2
path1=$3

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

source conf/graph/graph_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}
kal_version=kalVersion
kal_version_val=${!kal_version}
scala_version=scalaVersion
scala_version_val=${!scala_version}

spark-submit \
--class com.bigdata.compare.graph.LpaVerify \
--master yarn \
--num-executors 29 \
--executor-memory 35g \
--executor-cores 8 \
--driver-memory 50g \
--jars "lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
./lib/kal-test_${scala_version_val}-0.1.jar ${input} ${path0} ${path1}