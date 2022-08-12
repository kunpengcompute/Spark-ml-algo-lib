#!/bin/bash
set -e

function usage() {
  echo "Usage: <path0> <path1>"
  echo "1st argument: path of 1st result: eg [hdfs:///tmp/ml/result/RF/classification_epsilon_dataframe_fit1]"
  echo "2nd argument: path of 2nd result: eg [hdfs:///tmp/ml/result/RF/classification_epsilon_dataframe_fit1_raw]"
  echo "Applicable to algorithm ALS KMeans LinR SVM GBDT.regression RF.regression XGBT.regression"
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
--class com.bigdata.compare.ml.DownEvaluationVerify \
--master yarn \
--num-executors 29 \
--executor-memory 35g \
--executor-cores 8 \
--driver-memory 50g \
./lib/kal-test_${scala_version_val}-0.1.jar ${path0} ${path1}