#!/bin/bash
set -e

function usage() {
  echo "Usage: <path0> <path1>"
  echo "1st argument: path of PrefixSpan result: eg [hdfs:///tmp/ml/result/PrefixSpan/]"
  echo "2nd argument: path of PrefixSpan result: eg [hdfs:///tmp/ml/result/PrefixSpan/]"
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
--class com.bigdata.compare.ml.PrefixSpanVerify \
--master yarn \
--num-executors 29 \
--executor-memory 35g \
--executor-cores 8 \
--driver-memory 50g \
./lib/kal-test_${scala_version_val}-0.1.jar ${path0} ${path1}