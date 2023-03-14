#!/bin/bash
set -e

function usage() {
  echo "Usage: <path0> <path1>"
  echo "1st argument: path of SimRank opt result: eg [hdfs:///tmp/ml/result/TargetEncoder/taobao]"
  echo "2nd argument: path of SimRank raw result: eg [hdfs:///tmp/ml/result/TargetEncoder/taobao_raw]"
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
--class com.bigdata.compare.ml.TEVerify \
--master yarn \
--num-executors 26 \
--executor-memory 40g \
--executor-cores 8 \
--conf spark.driver.memory=128g \
--conf spark.driver.maxResultSize=40g \
--conf spark.network.timeout=60000s \
--conf spark.rpc.askTimeout=60000s \
--conf spark.executor.heartbeatInterval=600s \
--conf spark.eventLog.enabled=false \
./lib/kal-test_${scala_version_val}-0.1.jar ${path0} ${path1}