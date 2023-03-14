saveDataPath=/tmp/ml/dataset/ALS

source conf/ml/ml_datasets.properties
spark_version=sparkVersion
spark_version_val=${!spark_version}
scala_version=scalaVersion
scala_version_val=${!scala_version}

ALS_Path=${saveDataPath}/${spark_version_val}"/als"
ALSbs_Path=${saveDataPath}/${spark_version_val}"/als_bs"
ALSh_Path=${saveDataPath}/${spark_version_val}"/als_h"

spark-submit \
--class com.bigdata.preprocess.ml.ALSDataGenRun \
--master yarn \
--deploy-mode client \
--driver-cores 36 \
--driver-memory 50g \
--num-executors 12 \
--executor-cores 23 \
--executor-memory 79g \
--conf "spark.executor.extraJavaOptions=-Xms20g -Xss5g" \
./lib/kal-test_${scala_version_val}-0.1.jar \
${ALS_Path} 50000 60000 0.05 true

spark-submit \
--class com.bigdata.preprocess.ml.ALSDataGenRun \
--master yarn \
--deploy-mode client \
--driver-cores 36 \
--driver-memory 50g \
--num-executors 12 \
--executor-cores 23 \
--executor-memory 79g \
--conf "spark.executor.extraJavaOptions=-Xms20g -Xss5g" \
./lib/kal-test_${scala_version_val}-0.1.jar \
${ALSbs_Path} 100000 120000 0.02 true

spark-submit \
--class com.bigdata.preprocess.ml.ALSDataGenRun \
--master yarn \
--deploy-mode client \
--driver-cores 36 \
--driver-memory 50g \
--num-executors 12 \
--executor-cores 23 \
--executor-memory 79g \
--conf "spark.executor.extraJavaOptions=-Xms20g -Xss5g" \
./lib/kal-test_${scala_version_val}-0.1.jar \
${ALSh_Path} 25000 30000 0.05 true