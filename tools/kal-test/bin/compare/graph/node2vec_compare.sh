#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <input path> <ground truth path> <input path> <partition num> "
  echo "1st argument: path of algorithm dataset"
  echo "2nd argument: path of ground truth"
  echo "3rd argument: path of algorithm result"
  echo "4th argument: partition num, default value 240"
  exit 0
  ;;
esac
input=$1
ground=$2
output=$3
part=$4

source conf/graph/graph_datasets.properties
scala_version=scalaVersion
scala_version_val=${!scala_version}

spark-submit \
--class com.bigdata.compare.graph.Node2vecVerify \
--master yarn \
--num-executors 3 \
--executor-memory 315g \
--executor-cores 93 \
--driver-memory 300g \
--conf "spark.driver.maxResultSize=300g" \
--conf spark.driver.extraJavaOptions="-Xms300g" \
--jars "lib/smile-core-2.5.3.jar,lib/smile-math-2.5.3.jar" \
./lib/kal-test_${scala_version_val}-0.1.jar ${input} ${ground} ${output} ${part:-240}