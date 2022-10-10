#!/bin/bash
set -e

function alg_usage() {
  echo "Usage: <input dataset> <output dataset>"
  echo "1st argument: name of input dataset: twitter"
  echo "2nd argument: name of output dataset: twitter_2010"
}

case "$1" in
-h | --help | ?)
  alg_usage
  exit 0
  ;;
esac

if [ $# -gt 2 ]; then
  alg_usage
  exit 0
fi

input=${1:-twitter}
output=${2:-twitter_2010}

source conf/graph/graph_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}
kal_version=kalVersion
kal_version_val=${!kal_version}
scala_version=scalaVersion
scala_version_val=${!scala_version}
cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

inputPath=${!input}
outPath=${!output}
split=","
seed=1
iterNum=100
resetProb=0.15
partition=273

for rate in "0.001" "0.01" "0.05"
do
  echo ">>> start twitter-2010-$rate"
  output_incData=${outPath}_${rate}
  hdfs dfs -rm -r -f ${output_incData}*
  spark-submit \
  --class com.bigdata.preprocess.graph.IncDataGeneratorBatch \
  --master yarn \
  --num-executors 39 \
  --executor-memory 23g \
  --executor-cores 7 \
  --driver-memory 80g \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.kryoserializer.buffer=2040m \
  --jars "lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
  ./lib/kal-test_${scala_version_val}-0.1.jar yarn ${inputPath} ${split} ${output_incData} $rate $partition $seed $iterNum $resetProb 5 | tee ./log/log
  echo ">>> end twitter-2010-$rate"
done