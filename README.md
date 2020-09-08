# Spark-ml-algo-lib



Introduction
============

MACHINE LEARNING Algorithm library, running on KUNPENG chipset, is an accelerated library that provides a rich set of higher-level tools for machine learning algorithms. It is based on the original APIs from Apache Spark version 2.3.2. The accelerated library for performance optimization greatly improves the computational performance of big data algorithm scenarios.

The current version provides three common learning algorithms: Support Vector Machine (SVM) Algorithm, Random Forest Classifier (RFC) algorithm, and Gradient Boosting Decision Tree (GBDT) algorithm. 
You can find the latest documentation, including a programming guide, on the project web page. This README file only contains basic setup instructions.





Building And Package
====================

(1) Compile under the "Spark-ml-algo-lib" :

    sbt clean compile
    sbt "project mlcore" package
    sbt "project mlacc" package


(2) get "sophon-ml-core_2.11-1.0.0.jar" under the "Spark-ml-algo-lib/ml-core/target/scala-2.11"

    cd ml-core/target/scala-2.11
 
   get "sophon-ml-core_2.11-1.0.0.jar" under the "Spark-ml-algo-lib/ml-core/target/scala-2.11"

    cd ml-accelerator/target/scala-2.11

 

Contact
=======

Spark is distributed through GitHub. For the latest version, a bug tracker,
and other information, see

  http://spark.apache.org/

or the repository at

  https://github.com/apache/spark/tree/v2.3.2
