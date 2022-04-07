# kal-test


### 介绍
kal 算法测试工具开发，包含机器学习算法和图算法。


### 编译教程

####前提
1.  已配置Maven编译环境
2.  已获取算法软件包
####编译命令
1.  在编译环境，进入Spark-ml-algo-lib/tools/kal-test目录
2.  执行依赖安装： <br/>&emsp;&emsp;_**mvn install:install-file -DgroupId=org.apache.spark.graphx.lib -DartifactId=boostkit-graph-kernel-client_2.11 -Dversion=2.1.0 -Dfile=lib/boostkit-graph-kernel-client_2.11-1.2.0.jar -Dpackaging=jar**_
3.  执行编译命令：<br/>&emsp;&emsp;**_mvn clean install -DskipTests_**
4.  查看Spark-ml-algo-lib/tools/kal-test/target目录下生成的 kal-test_2.11-0.1.jar文件

### 部署和使用说明

1.  将kal-test文件夹部署至测试环境，如“/home/test/boostkit/”目录下，
    若该目录不存在，先创建目录 <br/>&emsp;&emsp;**_mkdir -p /home/test/boostkit/_**
2.  进入该目录 <br/>&emsp;&emsp;**_cd /home/test/boostkit/kal-test/_**
3.  将获得的 boostkit-graph-kernel-scala_version-kal_version-spark_version-aarch64.jar、
    boostkit-graph-acc_scala_version-kal_version-spark_version.jar
    和boostkit-graph-core_scala_version-kal_version-spark_version.jar放入到“/home/test/boostkit/kal-test/lib”下。
4.  进入“/home/test/boostkit/kal-test”目录中 <br/>&emsp;&emsp;**_cd /home/test/boostkit/kal-test_**
5.  以PageRank算法为例，在/home/test/boostkit/kal-test/目录下执行命令： <br/>&emsp;&emsp;**_bash bin/graph/pr_run.sh uk_2002 run_**
6.  查看算法运行情况

### 算法与数据集


| 算法名 | 数据集 | 接口名 |
| :-----| ----: | :----: |
| PageRank | uk_2002 | run |


### 引用与参考

1. KNN open source: https://github.com/saurfang/spark-knn.git
2. BFS open source: https://github.com/prasad223/GraphxBFS
3. DBSCAN open source: https://github.com/alitouka/spark_dbscan
4. ClusteringCoefficient open source: https://github.com/apache/spark/pull/9150/files
5. Betweenness open source: https://github.com/Sotera/distributed-graph-analytics/tree/master/dga-graphx/src/main/scala/com/soteradefense/dga/graphx/hbse
6. Node2Vec open source: https://github.com/QuanLab/node2vec-spark
7. SubgraphMatching open source: https://datalab.snu.ac.kr/pegasusn/scala-apidoc/#pegasus.spark.subgraph.PSE
8. XGBoost open source: https://github.com/dmlc/xgboost/tree/v1.1.0