#!/bin/bash
set -e

opt_path=/tmp/graph/result/incpr/no
raw_path=/tmp/graph/result/incpr/yes

case "$1" in
-h | --help | ?)
  echo "Usage: <path0> <path1>"
  echo "1st argument: path of incpr result: default value [${opt_path}]"
  echo "2nd argument: path of tpr result: default value [${raw_path}]"
  exit 0
  ;;
esac

path0=$1
path1=$2

path0=${path0:-${opt_path}}
path1=${path1:-${raw_path}}

source conf/graph/graph_datasets.properties
scala_version=scalaVersion
scala_version_val=${!scala_version}

split="\t"
numPart=273
output_path=${output_path_prefix}/incpr/acc
dataset_name=twitter_2010

for rate in "0.001" "0.01" "0.05"
do
  for batch in "1" "2" "3" "4" "5"
  do
    hdfs dfs -rm -r -f "${output_path}"
    incpr_path=${path0}/${dataset_name}_${rate}_batch_${batch}
    tpr_path=${path1}/${dataset_name}_${rate}_batch_${batch}
    datasetPath=${!dataset_name}_${rate}_batch_${batch}
    echo ">>> start twitter-2010_${rate}_batch_${batch} accuracy evaluation"
    spark-submit \
    --class com.bigdata.compare.graph.IncPageRankVerify \
    --master yarn \
    --name incpr_${rate}_batch_${batch} \
    --num-executors 29 \
    --executor-memory 35g \
    --executor-cores 8 \
    --driver-memory 50g \
    ./lib/kal-test_${scala_version_val}-0.1.jar yarn ${incpr_path} ${tpr_path} ${split} ${numPart} ${output_path} ${datasetPath}
    echo ">>> end twitter-2010_${rate}_batch_${batch} accuracy evaluation"
  done
done