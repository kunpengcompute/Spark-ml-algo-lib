#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <master> <resultPath> <graphType> <datasetPath> <splitGraph> <outputCheckResult> <partition>"
  echo "1st argument: master:local/yarn"
  echo "2nd argument: path of algorithm result"
  echo "3rd argument: name of graphType"
  echo "4th argument: path of dataset"
  echo "5th argument: splitGraph"
  echo "6th argument: path of outputCheckResult"
  echo "7th argument: partition"
  exit 0
  ;;
esac

master=$1
resultPath=$2
graphType=$3
datasetPath=$4
splitGraph=$5
outputCheckResult=$6
partition=$7

source conf/graph/graph_datasets.properties
scala_version=scalaVersion
scala_version_val=${!scala_version}

spark-submit \
--class com.bigdata.compare.graph.SubgraphMatchingVerify \
--master yarn \
--num-executors 29 \
--executor-memory 35g \
--executor-cores 8 \
--driver-memory 50g \
./lib/kal-test_${scala_version_val}-0.1.jar ${master} ${resultPath} ${graphType} ${datasetPath} ${splitGraph} ${outputCheckResult} ${partition}