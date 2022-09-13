#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <algorithm type> <path0> <path1>"
  echo "1st argument: type of algorithm: [classification/regression]"
  echo "2nd argument: path of opt result: eg [hdfs:///tmp/ml/result/RF/classification_epsilon_dataframe_fit1]"
  echo "3rd argument: path of raw result: eg [hdfs:///tmp/ml/result/RF/classification_epsilon_dataframe_fit1_raw]"
  exit 0
  ;;
esac

if [ $# -ne 3 ]; then
  echo "Usage: <algorithm type> <path0> <path1>"
  echo "1st argument: type of algorithm: [classification/regression]"
  echo "2nd argument: path of opt result: eg [hdfs:///tmp/ml/result/RF/classification_epsilon_dataframe_fit1]"
  echo "3rd argument: path of raw result: eg [hdfs:///tmp/ml/result/RF/classification_epsilon_dataframe_fit1_raw]"
  exit 0
fi

algorithm_type=$1
path0=$2
path1=$3

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
./lib/kal-test_${scala_version_val}-0.1.jar ${algorithm_type} ${path0} ${path1}