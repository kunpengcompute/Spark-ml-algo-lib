mapPath=./datasets/featureMap_400m.json
dataPath=hdfs:///tmp/ml/dataset/encoder/encoder_400m
num_executors=71
executor_cores=4
executor_memory=12

function createDir() {
    dir=$1
    if [ ! -d $dir ]; then
      mkdir $dir
    fi
}
createDir datasets
hadoop fs -mkdir -p ${dataPath}
hadoop fs -rm -r ${dataPath}

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')

source conf/ml/ml_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}
kal_version=kalVersion
kal_version_val=${!kal_version}
scala_version=scalaVersion
scala_version_val=${!scala_version}

echo "start to clean cache and sleep 3s"
ssh server1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent1 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent2 "echo 3 > /proc/sys/vm/drop_caches"
ssh agent3 "echo 3 > /proc/sys/vm/drop_caches"
sleep 3

# --conf spark.executor.extraJavaOptions="-Xms${executor_memory}g" \

spark-submit \
--class com.bigdata.preprocess.ml.EncoderDataGenRun \
--jars ./lib/boostkit-ml-kernel-${scala_version_val}-${kal_version_val}-${spark_version_val}-${cpu_name}.jar \
--driver-java-options "-Dhdp.version=3.1.0.0-78" \
--master yarn \
--num-executors ${num_executors} \
--executor-cores ${executor_cores} \
--executor-memory ${executor_memory}g \
--conf spark.rdd.compress=false \
--conf spark.eventLog.enabled=true \
--conf spark.driver.maxResultSize=40g \
--conf spark.network.timeout=60s \
--conf "spark.driver.extraJavaOptions=-Xss5g -Dlog4j.configuration=file:./log4j.properties" \
./lib/kal-test_${scala_version_val}-0.1.jar \
--mapPath ${mapPath} \
--dataPath ${dataPath} \
--numSamples 400000000

# --conf spark.executor.memoryOverhead=2048 \
#--driver-java-options "-Xms15g" \
#--conf spark.driver.cores=36 \
#--conf spark.driver.memory=50g \
#--conf spark.sql.cbo.enabled=true \
