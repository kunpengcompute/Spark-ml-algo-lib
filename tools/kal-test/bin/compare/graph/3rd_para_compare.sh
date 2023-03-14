#!/bin/bash
set -e

function alg_usage() {
  echo "Usage: <algorithm name> <path0> <path1>"
  echo "1st argument: algorithm name: betweenness, bfs, closeness, clusteringcoefficient, cc, cd, degree, kcore, mssp, pr, scc, tc, tpr, tr, wce, wpr"
  echo "2nd argument: path of baseline result"
  echo "3rd argument: path of algorithm result"
}

case "$1" in
-h | --help | ?)
  alg_usage
  exit 0
  ;;
esac

if [ $# -ne 3 ];then
  alg_usage
	exit 0
fi

alg=$1
path0=$2
path1=$3

if [ $alg == "betweenness" ] || [ $alg == "closeness" ]; then
  class_name=com.bigdata.compare.graph.BetweennessClosenessVerify
elif [ $alg == "bfs" ]; then
  class_name=com.bigdata.compare.graph.BFSVerify
elif [ $alg == "clusteringcoefficient" ] || [ $alg == "tc" ]; then
  class_name=com.bigdata.compare.graph.ClusteringCoefficientTCVerify
elif [ $alg == "cc" ]; then
  class_name=com.bigdata.compare.graph.CCVerify
elif [ $alg == "degree" ]; then
  class_name=com.bigdata.compare.graph.DegreeVerify
elif [ $alg == "cd" ]; then
  class_name=com.bigdata.compare.graph.CDVerify
elif [ $alg == "kcore" ]; then
  class_name=com.bigdata.compare.graph.KCoreVerify
elif [ $alg == "wce" ]; then
  class_name=com.bigdata.compare.graph.MceWceVerify
elif [ $alg == "mssp" ]; then
  class_name=com.bigdata.compare.graph.MsspVerify
elif [ $alg == "pr" ]; then
  class_name=com.bigdata.compare.graph.PageRankVerify
elif [ $alg == "scc" ]; then
  class_name=com.bigdata.compare.graph.SCCVerify
elif [ $alg == "tpr" ]; then
  class_name=com.bigdata.compare.graph.TrillionPageRankVerify
elif [ $alg == "tr" ]; then
  class_name=com.bigdata.compare.graph.TrustRankVerify
elif [ $alg == "wpr" ]; then
  class_name=com.bigdata.compare.graph.WeightedPageRankVerify
else
  alg_usage
  exit 0
fi

source conf/graph/graph_datasets.properties
scala_version=scalaVersion
scala_version_val=${!scala_version}

spark-submit \
--class ${class_name} \
--master yarn \
--num-executors 29 \
--executor-memory 35g \
--executor-cores 8 \
--driver-memory 50g \
--conf "spark.driver.maxResultSize=100g" \
./lib/kal-test_${scala_version_val}-0.1.jar ${path0} ${path1}