# Spark-ml-algo-lib



Introduction
============

MACHINE LEARNING Algorithm library, running on KUNPENG chipset, is an accelerated library that provides a rich set of higher-level tools for machine learning algorithms. It is based on the original APIs from Apache [Spark 2.3.2](https://github.com/apache/spark/tree/v2.3.2) and [breeze 0.13.1](https://github.com/scalanlp/breeze/tree/releases/v0.13.1). The accelerated library for performance optimization greatly improves the computational performance of big data algorithm scenarios.

The current version provides nine common learning algorithms: Support Vector Machine (SVM) Algorithm, Random Forest Classifier (RFC) algorithm, Gradient Boosting Decision Tree (GBDT) algorithm, Decision Tree (DT) algorithm, K-means Clustering algorithm, Linear Regression algorithm, Logistic Regression algorithm, Principal Component Analysis (PCA) algorithm, and Singular Value Decomposition (SVD) algorithm. 
You can find the latest documentation, including a programming guide, on the project web page. This README file only contains basic setup instructions.





Building And Package
====================

(1) Package under the "Spark-ml-algo-lib" :

    mvn clean package


(2) get "sophon-ml-core_2.11-1.1.0.jar" under the "Spark-ml-algo-lib/ml-core/target"

   get "sophon-ml-acc_2.11-1.1.0.jar" under the "Spark-ml-algo-lib/ml-accelerator/target"



Contribution guidelines
========

Please use GitHub [issues](https://github.com/kunpengcompute/Spark-ml-algo-lib/issues) for tracking requests and bugs.

More Information
========

For further assistance, you can send email to kunpengcompute@huawei.com.
