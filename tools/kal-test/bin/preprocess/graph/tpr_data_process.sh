#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <inputPath> <outPath>"
  echo "1st argument: inputPath of dataset"
  echo "2nd argument: outPath of dataset"
  exit 0
  ;;
esac

if [ $# -ne 2 ]; then
  echo "please input 2 arguments: <inputPath> <outPath>"
  echo "1st argument: inputPath of dataset"
  echo "2nd argument: outPath of dataset"
  exit 0
fi

inputPath=$1
outPath=$2

source conf/graph/graph_datasets.properties
scala_version=scalaVersion
scala_version_val=${!scala_version}

spark-submit \
--class com.bigdata.preprocess.graph.TrillionPageRankDataProcess \
--master yarn \
--num-executors 39 \
--executor-memory 23g \
--executor-cores 7 \
--driver-memory 80g \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.kryoserializer.buffer=2040m \
./lib/kal-test_${scala_version_val}-0.1.jar ${inputPath} ${outPath} | tee ./log/log