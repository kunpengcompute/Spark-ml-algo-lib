# kal-test


### Description
The Kunpeng algorithm library test tool can be used to test machine learning and graph analysis algorithms.


### Compilation Tutorial

####Prerequisites
1.  The Maven compilation environment has been configured.
2.  The algorithm software package has been obtained.
3.  Go to the Spark-ml-algo-lib/tools/kal-test directory in the compilation environment.
4.  Install the dependencies.<br/>
    mvn install:install-file -DgroupId=org.apache.spark.graphx.lib -DartifactId=boostkit-graph-kernel-client_2.11 -Dversion=2.1.0 -Dfile=lib/boostkit-graph-kernel-client_2.11-1.2.0.jar -Dpackaging=jar**_
5.  Run the compile command:<br/>
    mvn clean install -DskipTests_**
6.  View the kal-test_2.11-0.1.jar file generated in Spark-ml-algo-lib/tools/kal-test/target.

### Deployment and Usage Description

1.  Deploy the kal-test folder in the test environment, for example, in the /home/test/boostkit/ directory. If the directory does not exist, create one.<br/>mkdir -p /home/test/boostkit/_**
2.  Go to the directory.<br/>
    cd /home/test/boostkit/kal-test/_**
3.  Save the obtained boostkit-graph-kernel-scala_version-kal_version-spark_version-aarch64.jar, boostkit-graph-acc_scala_version-kal_version-spark_version.jar, and boostkit-graph-core_scala_version-kal_version-spark_version.jar files to /home/test/boostkit/kal-test/lib.
4.  Go to the /home/test/boostkit/kal-test directory.<br/>
    cd /home/test/boostkit/kal-test_**
5.  Run the following command in the /home/test/boostkit/kal-test/ directory (taking the PageRank algorithm as an example):<br/>bash bin/graph/pr_run.sh uk_2002 run_**
6.  Check the algorithm running status.

### Algorithm and Dataset


| Algorithm | Dataset | Interface |
| :-----| ----: | :----: |
| PageRank | uk_2002 | run |


### References

1. KNN open source: https://github.com/saurfang/spark-knn.git
2. BFS open source: https://github.com/prasad223/GraphxBFS
3. DBSCAN open source: https://github.com/alitouka/spark_dbscan
4. ClusteringCoefficient open source: https://github.com/apache/spark/pull/9150/files
5. Betweenness open source: https://github.com/Sotera/distributed-graph-analytics/tree/master/dga-graphx/src/main/scala/com/soteradefense/dga/graphx/hbse
6. Node2Vec open source: https://github.com/QuanLab/node2vec-spark
7. SubgraphMatching open source: https://datalab.snu.ac.kr/pegasusn/scala-apidoc/#pegasus.spark.subgraph.PSE
8. XGBoost open source: https://github.com/dmlc/xgboost/tree/v1.1.0
