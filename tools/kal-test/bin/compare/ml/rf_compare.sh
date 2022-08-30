#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <path0> <path1>"
  echo "1st argument: path of RF result: eg [hdfs:///tmp/ml/result/RF/]"
  echo "2nd argument: path of RF result: eg [hdfs:///tmp/ml/result/RF/]"
  exit 0
  ;;
esac

if [ $# -ne 2 ]; then
  echo "Usage: <path0> <path1>"
  echo "1st argument: path of RF result: eg [hdfs:///tmp/ml/result/RF/]"
  echo "2nd argument: path of RF result: eg [hdfs:///tmp/ml/result/RF/]"
  exit 0
fi

path0=$1
path1=$2

source conf/ml/ml_datasets.properties
scala_version=scalaVersion
scala_version_val=${!scala_version}

spark-submit \
--class com.bigdata.compare.ml.EvaluationVerify \
--master yarn \
--num-executors 29 \
--executor-memory 35g \
--executor-cores 8 \
--driver-memory 50g \
./lib/kal-test_${scala_version_val}-0.1.jar ${path0} ${path1}