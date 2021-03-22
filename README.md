# Spark-ml-algo-lib



Introduction
============

The machine learning algorithm library running on Kunpeng processors is an acceleration library that provides a rich set of high-level tools for machine learning algorithms. It is based on the original APIs of Apache [Spark 2.3.2](https://github.com/apache/spark/tree/v2.3.2) and [breeze 0.13.1](https://github.com/scalanlp/breeze/tree/releases/v0.13.1). The acceleration library for greatly improves the computing power in big data scenarios.

The library provides nine machine learning algorithms: support vector machine (SVM), random forest classifier (RFC), gradient boosting decision tree (GBDT), decision tree (DT), K-means clustering, linear regression, logistic regression algorithm, principal component analysis (PCA), singular value decomposition (SVD), latent dirichlet allocation (LDA), prefix-projected pattern prowth (Prefix-Span), alternating least squares (ALS), and K-nearest neighbors (KNN). You can find the latest documentation on the project web page. This README file contains only basic setup instructions.
You can find the latest documentation, including a programming guide, on the project web page. This README file only contains basic setup instructions.





Building And Packageing
====================

(1) Build the project under the "Spark-ml-algo-lib" directory:

    mvn clean package


(2) Obtain "sophon-ml-core_2.11-1.2.0.jar" under the "Spark-ml-algo-lib/ml-core/target" directory.

   Obtain "sophon-ml-acc_2.11-1.2.0.jar" under the "Spark-ml-algo-lib/ml-accelerator/target" directory.



Contribution Guidelines
========

Track the bugs and feature requests via GitHub [issues](https://github.com/kunpengcompute/Spark-ml-algo-lib/issues).

More Information
========

For further assistance, send an email to kunpengcompute@huawei.com.
