# Spark-ml-algo-lib



Introduction
============

The machine learning algorithm library running on Kunpeng processors is an acceleration library that provides a rich set of high-level tools for machine learning algorithms. It is based on the original APIs of Apache [Spark 3.3.1](https://github.com/apache/spark/tree/v3.3.1). The acceleration library for greatly improves the computing power in big data scenarios.

The library provides 4 machine learning algorithms: Density-based spatial clustering of applicaitons with noise (DBSCAN), Support Vector Machines)SVM), decision tree bucket(DTB) and Word2Vec. You can find the latest documentation on the project web page. This README file contains only basic setup instructions.
You can find the latest documentation, including a programming guide, on the project web page. This README file only contains basic setup instructions.





Building And Packageing
====================

(1) Build the project under the "Spark-ml-algo-lib" directory:

    mvn clean package

(2) Obtain "boostkit-ml-core_2.12-3.0.0-spark3.3.1.jar" under the "Spark-ml-algo-lib/ml-core/target" directory.

   Obtain "boostkit-ml-acc_2.12-3.0.0-spark3.3.1.jar" under the "Spark-ml-algo-lib/ml-accelerator/target" directory.


Contribution Guidelines
========

Track the bugs and feature requests via GitHub [issues](https://github.com/kunpengcompute/Spark-ml-algo-lib/issues).

More Information
========

For further assistance, send an email to kunpengcompute@huawei.com.
