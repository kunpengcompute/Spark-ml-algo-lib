#!/bin/bash
set -e

function usage() {
  echo "Usage: <path0> <path1>"
  echo "1st argument: path of SimRank opt result: eg [hdfs:///tmp/ml/result/SimRank/simrank3w]"
  echo "2nd argument: path of SimRank raw result: eg [hdfs:///tmp/ml/result/SimRank/simrank3w]"
}

case "$1" in
-h | --help | ?)
  usage
  exit 0
  ;;
esac

if [ $# -ne 2 ]; then
  usage
  exit 0
fi

path0=$1
path1=$2

source conf/ml/ml_datasets.properties
scala_version=scalaVersion
scala_version_val=${!scala_version}

spark-submit \
--driver-java-options "-Dlog4j.configuration=file:./log4j.properties" \
--class com.bigdata.compare.ml.SimRankVerify \
--master yarn \
--deploy-mode client \
--driver-cores 36 \
--driver-memory 50g \
--num-executors 71 \
--executor-memory 12g \
--executor-cores 4 \
./lib/kal-test_${scala_version_val}-0.1.jar ${path0} ${path1}
