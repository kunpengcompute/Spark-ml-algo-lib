#!/bin/bash
set -e

function usage() {
  echo "Usage: <algorithm name> <path0> <path1>"
  echo "1st argument: algorithm name: als kmeans linr svm dtr(dt.regression) gbdtr(gbdt.regression) rfr(rf.regression) xgbtr(xgbt.regression)
                                      lda logr svm dtc(dt.classification) gbdtc(gbdt.classification) rfc(rf.classification) xgbtc(xgbt.classification)
                                      cov pca pearson spca spearman lda ps svd"
  echo "2st argument: path of opt result: eg [hdfs:///tmp/ml/result/RF/classification_epsilon_dataframe_fit1]"
  echo "3nd argument: path of raw result: eg [hdfs:///tmp/ml/result/RF/classification_epsilon_dataframe_fit1_raw]"
  echo "Applicable to algorithm ALS KMeans LinR SVM GBDT.regression RF.regression XGBT.regression"
}

case "$1" in
-h | --help | ?)
  usage
  exit 0
  ;;
esac

if [ $# -ne 3 ]; then
  usage
  exit 0
fi

alg=$1
path0=$2
path1=$3

if [ $alg == "als" ] || [ $alg == "kmeans" ]|| [ $alg == "linr" ]|| [ $alg == "svm" ]|| [ $alg == "dtr" ]|| [ $alg == "gbdtr" ]|| [ $alg == "rfr" ]|| [ $alg == "xgbtr" ]; then
  class_name=com.bigdata.compare.ml.DownEvaluationVerify
elif [ $alg == "logr" ] || [ $alg == "svm" ] || [ $alg == "dtc" ] || [ $alg == "gbdtc" ] || [ $alg == "rfc" ] || [ $alg == "xgbtc" ] ; then
  class_name=com.bigdata.compare.ml.UpEvaluationVerify
elif [ $alg == "cov" ] || [ $alg == "pca" ] || [ $alg == "pearson" ] || [ $alg == "spca" ] || [ $alg == "spearman" ]; then
  class_name=com.bigdata.compare.ml.MatrixVerify
elif [ $alg == "lda" ]; then
  class_name=com.bigdata.compare.ml.LDAVerify
elif [ $alg == "ps" ]; then
  class_name=com.bigdata.compare.ml.PrefixSpanVerify
elif [ $alg == "svd" ] ; then
  class_name=com.bigdata.compare.ml.SVDVerify
else
  alg_usage
  exit 0
fi

source conf/ml/ml_datasets.properties
scala_version=scalaVersion
scala_version_val=${!scala_version}

spark-submit \
--class ${class_name} \
--master yarn \
--num-executors 29 \
--executor-memory 35g \
--executor-cores 8 \
--driver-memory 50g \
./lib/kal-test_${scala_version_val}-0.1.jar ${path0} ${path1}