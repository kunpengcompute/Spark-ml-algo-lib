#!/bin/bash
set -e

function usage() {
  echo "Usage: <path0> <path1>"
  echo "1st argument: path of opt result: eg [hdfs:///tmp/ml/result/IDF/D2g250m]"
  echo "2nd argument: path of raw result: eg [hdfs:///tmp/ml/result/IDF/D2g250m_raw]"
  echo "Applicable to algorithm IDF"
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
--class com.bigdata.compare.ml.IDFVerify \
--master yarn \
--deploy-mode client \
--driver-cores 36 \
--driver-memory 50g \
--num-executors 12 \
--executor-cores 23 \
--executor-memory 79g \
--conf "spark.executor.extraJavaOptions=-Xms79g" \
--conf "spark.driver.maxResultSize=1024g" \
--driver-java-options "-Xms20g" \
./lib/kal-test_${scala_version_val}-0.1.jar ${path0} ${path1}