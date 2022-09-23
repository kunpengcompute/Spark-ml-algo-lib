#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <path0> <path1> <api name> <partition num>"
  echo "1st argument: path of baseline result"
  echo "2nd argument: path of algorithm result"
  echo "3rd argument: name of api: fixMS,fixSS,conSS"
  echo "4th argument: sourceCnt: 1,5,10,50,100, default value 240"
  exit 0
  ;;
esac
path0=$1
path1=$2
api=$3
src=$4

source conf/graph/graph_datasets.properties
scala_version=scalaVersion
scala_version_val=${!scala_version}

spark-submit \
--class com.bigdata.compare.graph.PersonalizedPageRankVerify \
--master yarn \
--num-executors 29 \
--executor-memory 35g \
--executor-cores 8 \
--driver-memory 50g \
./lib/kal-test_${scala_version_val}-0.1.jar ${path0} ${path1} ${api} ${src:-1}