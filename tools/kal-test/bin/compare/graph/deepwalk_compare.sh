#!/bin/bash
set -e


case "$1" in
-h | --help | ?)
  echo "Usage: <dataset name> <isRaw>"
  echo "1st argument: name of dataset: cit_patents_deepwalk"
  echo "2nd argument: optimization algorithm or raw: no/yes"
  exit 0
  ;;
esac

if [ $# -ne 2 ]; then
  echo "please input 2 arguments: <dataset name> <isRaw>"
  echo "1st argument: name of dataset: cit_patents_deepwalk"
  echo "2nd argument: optimization algorithm or raw: no/yes"
  exit 0
fi

source conf/graph/deepwalk/deepwalk_spark.properties

dataset_name=$1
is_raw=$2

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

source conf/graph/graph_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}
kal_version=kalVersion
kal_version_val=${!kal_version}
scala_version=scalaVersion
scala_version_val=${!scala_version}

data_path=${dataset_name}
data_path_val=${!data_path}
echo "${dataset_name} : ${data_path_val}"

negEdge_data_path="${dataset_name}_negEdge"
negEdge_data_path_val=${!negEdge_data_path}
echo "${dataset_name} negEdge: ${negEdge_data_path_val}"

model_conf=${dataset_name}-${cpu_name}
model_Path="/tmp/graph/result/deepawlk/${dataset_name}/${is_raw}"


spark-submit \
--class com.bigdata.compare.graph.DeepWalkVerify \
--master yarn \
--num-executors 29 \
--executor-memory 35g \
--executor-cores 8 \
--driver-memory 300g \
--conf spark.kryoserializer.buffer.max=2047m \
--conf spark.ui.showConsoleProgress=true \
--conf spark.driver.maxResultSize=0 \
--conf spark.driver.extraJavaOptions="-Xms300G -XX:hashCode=0" \
--conf spark.executor.extraJavaOptions="-Xms35G -XX:hashCode=0" \
--conf spark.rpc.askTimeout=1000000s \
--conf spark.network.timeout=1000000s \
--conf spark.executor.heartbeatInterval=100000s \
--conf spark.rpc.message.maxSize=1000 \
--jars "lib/fastutil-8.3.1.jar,lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
--driver-class-path "lib/kal-test_${scala_version_val}-0.1.jar:lib/snakeyaml-1.19.jar:lib/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
--conf "spark.executor.extraClassPath=/opt/graph_classpath/boostkit-graph-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar" \
./lib/kal-test_${scala_version_val}-0.1.jar ${model_conf} ${data_path_val} ${negEdge_data_path_val} ${model_Path} ${is_raw}



