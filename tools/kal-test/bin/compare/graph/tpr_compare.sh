#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <path0> <path1> <split0> <split1>"
  echo "1st argument: path of tpr result: default value [/tmp/graph/result/tpr/twitter_tpr/no]"
  echo "2nd argument: path of tpr result: default value [/tmp/graph/result/tpr/twitter_tpr/yes]"
  echo "3rd argument: split of the result in path0: default value [\t]"
  echo "4th argument: split of the result in path1: default value [\t]"
  exit 0
  ;;
esac

path0=$1
path1=$2
split0=$3
split1=$4

path0=${path0:-/tmp/graph/result/tpr/twitter_tpr/no}
path1=${path1:-/tmp/graph/result/tpr/twitter_tpr/yes}
split0=${split0:-"\t"}
split1=${split1:-"\t"}

source conf/graph/graph_datasets.properties
scala_version=scalaVersion
scala_version_val=${!scala_version}

spark-submit \
--class com.bigdata.compare.graph.TrillionPageRankVerify \
--master yarn \
--num-executors 29 \
--executor-memory 35g \
--executor-cores 8 \
--driver-memory 50g \
./lib/kal-test_${scala_version_val}-0.1.jar ${path0} ${path1} ${split0} ${split1}