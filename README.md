# Spark-ml-algo-lib



Introduction
============

The machine learning algorithm library running on Kunpeng processors is an acceleration library that provides a rich set of high-level tools for machine learning algorithms. It is based on the original APIs of Apache [Spark 2.4.6](https://github.com/apache/spark/tree/v2.4.6), [breeze 0.13.1](https://github.com/scalanlp/breeze/tree/releases/v0.13.1) and [xgboost 1.1.0](https://github.com/dmlc/xgboost/tree/release_1.0.0). The acceleration library for greatly improves the computing power in big data scenarios.

The library provides 23 machine learning algorithms: support vector machine (SVM), random forest classifier (RFC), gradient boosting decision tree (GBDT), decision tree (DT), K-means clustering, linear regression, logistic regression algorithm, principal component analysis (PCA), principal component analysis for Sparse Matrix(SPCA), singular value decomposition (SVD), latent dirichlet allocation (LDA), prefix-projected pattern prowth (Prefix-Span), alternating least squares (ALS), K-nearest neighbors (KNN), Covariance, Density-based spatial clustering of applicaitons with noise (DBSCAN), Pearson, Spearman, XGboost, Inverse Document Frequency(IDF), SimRank, Decision Tree Bucket(DTB) and Word2Vec. You can find the latest documentation on the project web page. This README file contains only basic setup instructions.
You can find the latest documentation, including a programming guide, on the project web page. This README file only contains basic setup instructions.





Building And Packageing
====================

(1) Build the project under the "Spark-ml-algo-lib" directory:

    mvn clean package

(2) Build XGBoost project under the "Spark-ml-algo-lib/ml-xgboost/jvm-packages" directory:

    mvn clean package

(3) Obtain "boostkit-ml-core_2.11-2.2.0-spark2.4.6.jar" under the "Spark-ml-algo-lib/ml-core/target" directory.

   Obtain "boostkit-ml-acc_2.11-2.2.0-spark2.4.6.jar" under the "Spark-ml-algo-lib/ml-accelerator/target" directory.

   Obtain "boostkit-xgboost4j_2.11-2.2.0.jar" under the "Spark-ml-algo-lib/ml-xgboost/jvm-packages/boostkit-xgboost4j/target" directory.

   Obtain "boostkit-xgboost4j-spark2.4.6_2.11-2.2.0.jar" under the "Spark-ml-algo-lib/ml-xgboost/jvm-packages/boostkit-xgboost4j-spark/target" directory.


Contribution Guidelines
========

Track the bugs and feature requests via GitHub [issues](https://github.com/kunpengcompute/Spark-ml-algo-lib/issues).

More Information
========

For further assistance, send an email to kunpengcompute@huawei.com.
