#!/bin/bash

#SPCA
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/SPCA/Kemelmacher_fit /tmp/ml/result/SPCA/Kemelmacher_fit_raw
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/SPCA/mesh_deform_fit /tmp/ml/result/SPCA/mesh_deform_fit_raw

#KNN
bash bin/compare/ml/knn_compare.sh glove
bash bin/compare/ml/knn_compare.sh gist
bash bin/compare/ml/knn_compare.sh deep1b

#SVD
bash ./bin/compare/ml/svd_compare.sh /tmp/ml/result/SVD/D10M1K /tmp/ml/result/SVD/D10M1K_raw
bash ./bin/compare/ml/svd_compare.sh /tmp/ml/result/SVD/D1M10K /tmp/ml/result/SVD/D1M10K_raw
bash ./bin/compare/ml/svd_compare.sh /tmp/ml/result/SVD/MESH /tmp/ml/result/SVD/MESH_raw
bash ./bin/compare/ml/svd_compare.sh /tmp/ml/result/SVD/RUCCI /tmp/ml/result/SVD/RUCCI_raw

#SimRank
bash ./bin/compare/ml/simrank_compare.sh /tmp/ml/result/SimRank/simrank3w /tmp/ml/result/SimRank/simrank3w_raw
bash ./bin/compare/ml/simrank_compare.sh /tmp/ml/result/SimRank/simrank5w /tmp/ml/result/SimRank/simrank5w_raw
bash ./bin/compare/ml/simrank_compare.sh /tmp/ml/result/SimRank/simrank7w /tmp/ml/result/SimRank/simrank7w_raw

#XGBT
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/XGBT/classification_higgs_dataframe_fit /tmp/ml/result/XGBT/classification_higgs_dataframe_fit_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/XGBT/classification_mnist8m_dataframe_fit /tmp/ml/result/XGBT/classification_mnist8m_dataframe_fit_raw

bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/XGBT/regression_higgs_dataframe_fit /tmp/ml/result/XGBT/regression_higgs_dataframe_fit_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/XGBT/regression_mnist8m_dataframe_fit /tmp/ml/result/XGBT/regression_mnist8m_dataframe_fit_raw

#Pearson
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/Pearson/dataframe_CP10M1K /tmp/ml/result/Pearson/dataframe_CP10M1K_raw
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/Pearson/dataframe_CP1M10K /tmp/ml/result/Pearson/dataframe_CP1M10K_raw
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/Pearson/dataframe_CP2M5K /tmp/ml/result/Pearson/dataframe_CP2M5K_raw

#PrefixSpan
bash ./bin/compare/ml/ps_compare.sh /tmp/ml/result/PrefixSpan/IBM100M47 /tmp/ml/result/PrefixSpan/IBM100M47_raw
bash ./bin/compare/ml/ps_compare.sh /tmp/ml/result/PrefixSpan/IBM10M47 /tmp/ml/result/PrefixSpan/IBM10M47_raw
bash ./bin/compare/ml/ps_compare.sh /tmp/ml/result/PrefixSpan/kosarak /tmp/ml/result/PrefixSpan/kosarak_raw

#SPCA
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/SPCA/Kemelmacher_fit /tmp/ml/result/SPCA/Kemelmacher_fit_raw
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/SPCA/Kemelmacher_fit1 /tmp/ml/result/SPCA/Kemelmacher_fit1_raw
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/SPCA/Kemelmacher_fit2 /tmp/ml/result/SPCA/Kemelmacher_fit2_raw
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/SPCA/Kemelmacher_fit3 /tmp/ml/result/SPCA/Kemelmacher_fit3_raw
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/SPCA/mesh_deform_fit /tmp/ml/result/SPCA/mesh_deform_fit_raw
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/SPCA/mesh_deform_fit1 /tmp/ml/result/SPCA/mesh_deform_fit1_raw
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/SPCA/mesh_deform_fit2 /tmp/ml/result/SPCA/mesh_deform_fit2_raw
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/SPCA/mesh_deform_fit3 /tmp/ml/result/SPCA/mesh_deform_fit3_raw
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/SPCA/wathen100_fit /tmp/ml/result/SPCA/wathen100_fit_raw
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/SPCA/wathen100_fit1 /tmp/ml/result/SPCA/wathen100_fit1_raw
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/SPCA/wathen100_fit2 /tmp/ml/result/SPCA/wathen100_fit2_raw
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/SPCA/wathen100_fit3 /tmp/ml/result/SPCA/wathen100_fit3_raw

# GBDT
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/GBDT/classification_dataframe_D10M4096libsvm_fit /tmp/ml/result/GBDT/classification_dataframe_D10M4096libsvm_fit_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/GBDT/classification_dataframe_D10M4096libsvm_fit1 /tmp/ml/result/GBDT/classification_dataframe_D10M4096libsvm_fit1_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/GBDT/classification_dataframe_D10M4096libsvm_fit2 /tmp/ml/result/GBDT/classification_dataframe_D10M4096libsvm_fit2_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/GBDT/classification_dataframe_D10M4096libsvm_fit3 /tmp/ml/result/GBDT/classification_dataframe_D10M4096libsvm_fit3_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/GBDT/classification_dataframe_epsilon_fit /tmp/ml/result/GBDT/classification_dataframe_epsilon_fit_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/GBDT/classification_dataframe_epsilon_fit1 /tmp/ml/result/GBDT/classification_dataframe_epsilon_fit1_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/GBDT/classification_dataframe_epsilon_fit2 /tmp/ml/result/GBDT/classification_dataframe_epsilon_fit2_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/GBDT/classification_dataframe_epsilon_fit3 /tmp/ml/result/GBDT/classification_dataframe_epsilon_fit3_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/GBDT/classification_dataframe_rcv_fit /tmp/ml/result/GBDT/classification_dataframe_rcv_fit_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/GBDT/classification_dataframe_rcv_fit1 /tmp/ml/result/GBDT/classification_dataframe_rcv_fit1_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/GBDT/classification_dataframe_rcv_fit2 /tmp/ml/result/GBDT/classification_dataframe_rcv_fit2_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/GBDT/classification_dataframe_rcv_fit3 /tmp/ml/result/GBDT/classification_dataframe_rcv_fit3_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/GBDT/regression_dataframe_D10M4096libsvm_fit  /tmp/ml/result/GBDT/regression_dataframe_D10M4096libsvm_fit_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/GBDT/regression_dataframe_D10M4096libsvm_fit1 /tmp/ml/result/GBDT/regression_dataframe_D10M4096libsvm_fit1_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/GBDT/regression_dataframe_D10M4096libsvm_fit2 /tmp/ml/result/GBDT/regression_dataframe_D10M4096libsvm_fit2_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/GBDT/regression_dataframe_D10M4096libsvm_fit3 /tmp/ml/result/GBDT/regression_dataframe_D10M4096libsvm_fit3_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/GBDT/regression_dataframe_epsilon_fit /tmp/ml/result/GBDT/regression_dataframe_epsilon_fit_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/GBDT/regression_dataframe_epsilon_fit1 /tmp/ml/result/GBDT/regression_dataframe_epsilon_fit1_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/GBDT/regression_dataframe_epsilon_fit2 /tmp/ml/result/GBDT/regression_dataframe_epsilon_fit2_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/GBDT/regression_dataframe_epsilon_fit3 /tmp/ml/result/GBDT/regression_dataframe_epsilon_fit3_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/GBDT/regression_dataframe_rcv_fit /tmp/ml/result/GBDT/regression_dataframe_rcv_fit_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/GBDT/regression_dataframe_rcv_fit1 /tmp/ml/result/GBDT/regression_dataframe_rcv_fit1_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/GBDT/regression_dataframe_rcv_fit2 /tmp/ml/result/GBDT/regression_dataframe_rcv_fit2_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/GBDT/regression_dataframe_rcv_fit3 /tmp/ml/result/GBDT/regression_dataframe_rcv_fit3_raw

# RF
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/RF/classification_epsilon_dataframe_fit /tmp/ml/result/RF/classification_epsilon_dataframe_fit_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/RF/classification_epsilon_dataframe_fit1 /tmp/ml/result/RF/classification_epsilon_dataframe_fit1_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/RF/classification_epsilon_dataframe_fit2 /tmp/ml/result/RF/classification_epsilon_dataframe_fit2_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/RF/classification_epsilon_dataframe_fit3 /tmp/ml/result/RF/classification_epsilon_dataframe_fit3_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/RF/classification_higgs_dataframe_fit /tmp/ml/result/RF/classification_higgs_dataframe_fit_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/RF/classification_higgs_dataframe_fit1 /tmp/ml/result/RF/classification_higgs_dataframe_fit1_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/RF/classification_higgs_dataframe_fit2 /tmp/ml/result/RF/classification_higgs_dataframe_fit2_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/RF/classification_higgs_dataframe_fit3 /tmp/ml/result/RF/classification_higgs_dataframe_fit3_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/RF/classification_mnist8m_dataframe_fit /tmp/ml/result/RF/classification_mnist8m_dataframe_fit_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/RF/classification_mnist8m_dataframe_fit1 /tmp/ml/result/RF/classification_mnist8m_dataframe_fit1_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/RF/classification_mnist8m_dataframe_fit2 /tmp/ml/result/RF/classification_mnist8m_dataframe_fit2_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/RF/classification_mnist8m_dataframe_fit3 /tmp/ml/result/RF/classification_mnist8m_dataframe_fit3_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/RF/classification_rcv_dataframe_fit /tmp/ml/result/RF/classification_rcv_dataframe_fit_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/RF/classification_rcv_dataframe_fit1 /tmp/ml/result/RF/classification_rcv_dataframe_fit1_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/RF/classification_rcv_dataframe_fit2 /tmp/ml/result/RF/classification_rcv_dataframe_fit2_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/RF/classification_rcv_dataframe_fit3 /tmp/ml/result/RF/classification_rcv_dataframe_fit3_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/RF/regression_epsilon_dataframe_fit /tmp/ml/result/RF/regression_epsilon_dataframe_fit_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/RF/regression_epsilon_dataframe_fit1 /tmp/ml/result/RF/regression_epsilon_dataframe_fit1_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/RF/regression_epsilon_dataframe_fit2 /tmp/ml/result/RF/regression_epsilon_dataframe_fit2_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/RF/regression_epsilon_dataframe_fit3 /tmp/ml/result/RF/regression_epsilon_dataframe_fit3_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/RF/regression_higgs_dataframe_fit /tmp/ml/result/RF/regression_higgs_dataframe_fit_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/RF/regression_higgs_dataframe_fit1 /tmp/ml/result/RF/regression_higgs_dataframe_fit1_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/RF/regression_higgs_dataframe_fit2 /tmp/ml/result/RF/regression_higgs_dataframe_fit2_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/RF/regression_higgs_dataframe_fit3 /tmp/ml/result/RF/regression_higgs_dataframe_fit3_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/RF/regression_mnist8m_dataframe_fit /tmp/ml/result/RF/regression_mnist8m_dataframe_fit_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/RF/regression_mnist8m_dataframe_fit1 /tmp/ml/result/RF/regression_mnist8m_dataframe_fit1_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/RF/regression_mnist8m_dataframe_fit2 /tmp/ml/result/RF/regression_mnist8m_dataframe_fit2_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/RF/regression_mnist8m_dataframe_fit3 /tmp/ml/result/RF/regression_mnist8m_dataframe_fit3_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/RF/regression_rcv_dataframe_fit /tmp/ml/result/RF/regression_rcv_dataframe_fit_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/RF/regression_rcv_dataframe_fit1 /tmp/ml/result/RF/regression_rcv_dataframe_fit1_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/RF/regression_rcv_dataframe_fit2 /tmp/ml/result/RF/regression_rcv_dataframe_fit2_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/RF/regression_rcv_dataframe_fit3 /tmp/ml/result/RF/regression_rcv_dataframe_fit3_raw

#ALS
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/ALS/als_dataframe_fit1_aarch64 /tmp/ml/result/ALS/als_dataframe_fit1_aarch64_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/ALS/als_dataframe_fit2_aarch64 /tmp/ml/result/ALS/als_dataframe_fit2_aarch64_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/ALS/als_dataframe_fit3_aarch64 /tmp/ml/result/ALS/als_dataframe_fit3_aarch64_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/ALS/als_dataframe_fit_aarch64 /tmp/ml/result/ALS/als_dataframe_fit_aarch64_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/ALS/alsbs_dataframe_fit1_aarch64 /tmp/ml/result/ALS/alsbs_dataframe_fit1_aarch64_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/ALS/alsbs_dataframe_fit2_aarch64 /tmp/ml/result/ALS/alsbs_dataframe_fit2_aarch64_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/ALS/alsbs_dataframe_fit3_aarch64 /tmp/ml/result/ALS/alsbs_dataframe_fit3_aarch64_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/ALS/alsbs_dataframe_fit_aarch64 /tmp/ml/result/ALS/alsbs_dataframe_fit_aarch64_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/ALS/alsh_dataframe_fit1_aarch64 /tmp/ml/result/ALS/alsh_dataframe_fit1_aarch64_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/ALS/alsh_dataframe_fit2_aarch64 /tmp/ml/result/ALS/alsh_dataframe_fit2_aarch64_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/ALS/alsh_dataframe_fit3_aarch64 /tmp/ml/result/ALS/alsh_dataframe_fit3_aarch64_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/ALS/alsh_dataframe_fit_aarch64 /tmp/ml/result/ALS/alsh_dataframe_fit_aarch64_raw

#DT
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/DT/classification_epsilon_dataframe_fit /tmp/ml/result/DT/classification_epsilon_dataframe_fit_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/DT/classification_epsilon_dataframe_fit1 /tmp/ml/result/DT/classification_epsilon_dataframe_fit1_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/DT/classification_epsilon_dataframe_fit2 /tmp/ml/result/DT/classification_epsilon_dataframe_fit2_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/DT/classification_epsilon_dataframe_fit3 /tmp/ml/result/DT/classification_epsilon_dataframe_fit3_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/DT/classification_higgs_dataframe_fit /tmp/ml/result/DT/classification_higgs_dataframe_fit_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/DT/classification_higgs_dataframe_fit1 /tmp/ml/result/DT/classification_higgs_dataframe_fit1_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/DT/classification_higgs_dataframe_fit2 /tmp/ml/result/DT/classification_higgs_dataframe_fit2_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/DT/classification_higgs_dataframe_fit3 /tmp/ml/result/DT/classification_higgs_dataframe_fit3_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/DT/classification_mnist8m_dataframe_fit /tmp/ml/result/DT/classification_mnist8m_dataframe_fit_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/DT/classification_mnist8m_dataframe_fit1 /tmp/ml/result/DT/classification_mnist8m_dataframe_fit1_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/DT/classification_mnist8m_dataframe_fit2 /tmp/ml/result/DT/classification_mnist8m_dataframe_fit2_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/DT/classification_mnist8m_dataframe_fit3 /tmp/ml/result/DT/classification_mnist8m_dataframe_fit3_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/DT/regression_epsilon_dataframe_fit /tmp/ml/result/DT/regression_epsilon_dataframe_fit_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/DT/regression_epsilon_dataframe_fit1 /tmp/ml/result/DT/regression_epsilon_dataframe_fit1_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/DT/regression_epsilon_dataframe_fit2 /tmp/ml/result/DT/regression_epsilon_dataframe_fit2_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/DT/regression_epsilon_dataframe_fit3 /tmp/ml/result/DT/regression_epsilon_dataframe_fit3_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/DT/regression_higgs_dataframe_fit /tmp/ml/result/DT/regression_higgs_dataframe_fit_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/DT/regression_higgs_dataframe_fit1 /tmp/ml/result/DT/regression_higgs_dataframe_fit1_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/DT/regression_higgs_dataframe_fit2 /tmp/ml/result/DT/regression_higgs_dataframe_fit2_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/DT/regression_higgs_dataframe_fit3 /tmp/ml/result/DT/regression_higgs_dataframe_fit3_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/DT/regression_mnist8m_dataframe_fit /tmp/ml/result/DT/regression_mnist8m_dataframe_fit_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/DT/regression_mnist8m_dataframe_fit1 /tmp/ml/result/DT/regression_mnist8m_dataframe_fit1_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/DT/regression_mnist8m_dataframe_fit2 /tmp/ml/result/DT/regression_mnist8m_dataframe_fit2_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/DT/regression_mnist8m_dataframe_fit3 /tmp/ml/result/DT/regression_mnist8m_dataframe_fit3_raw

#KMeans
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/KMeans/D1200M20_dataframe_fit /tmp/ml/result/KMeans/D1200M20_dataframe_fit_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/KMeans/D1200M20_dataframe_fit1 /tmp/ml/result/KMeans/D1200M20_dataframe_fit1_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/KMeans/D1200M20_dataframe_fit2 /tmp/ml/result/KMeans/D1200M20_dataframe_fit2_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/KMeans/D1200M20_dataframe_fit3 /tmp/ml/result/KMeans/D1200M20_dataframe_fit3_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/KMeans/D200M100_dataframe_fit /tmp/ml/result/KMeans/D200M100_dataframe_fit_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/KMeans/D200M100_dataframe_fit1 /tmp/ml/result/KMeans/D200M100_dataframe_fit1_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/KMeans/D200M100_dataframe_fit2 /tmp/ml/result/KMeans/D200M100_dataframe_fit2_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/KMeans/D200M100_dataframe_fit3 /tmp/ml/result/KMeans/D200M100_dataframe_fit3_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/KMeans/D200M20_dataframe_fit /tmp/ml/result/KMeans/D200M20_dataframe_fit_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/KMeans/D200M20_dataframe_fit1 /tmp/ml/result/KMeans/D200M20_dataframe_fit1_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/KMeans/D200M20_dataframe_fit2 /tmp/ml/result/KMeans/D200M20_dataframe_fit2_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/KMeans/D200M20_dataframe_fit3 /tmp/ml/result/KMeans/D200M20_dataframe_fit3_raw

#LDA
bash ./bin/compare/ml/lda_compare.sh /tmp/ml/result/LDA/D20M200K_dataframe_fit1 /tmp/ml/result/LDA/D20M200K_dataframe_fit1_raw
bash ./bin/compare/ml/lda_compare.sh /tmp/ml/result/LDA/D20M200K_dataframe_fit2 /tmp/ml/result/LDA/D20M200K_dataframe_fit2_raw
bash ./bin/compare/ml/lda_compare.sh /tmp/ml/result/LDA/D20M200K_dataframe_fit3 /tmp/ml/result/LDA/D20M200K_dataframe_fit3_raw
bash ./bin/compare/ml/lda_compare.sh /tmp/ml/result/LDA/D20M200K_dataframe_fit /tmp/ml/result/LDA/D20M200K_dataframe_fit_raw
bash ./bin/compare/ml/lda_compare.sh /tmp/ml/result/LDA/nytimes_dataframe_fit /tmp/ml/result/LDA/nytimes_dataframe_fit_raw
bash ./bin/compare/ml/lda_compare.sh /tmp/ml/result/LDA/nytimes_dataframe_fit1 /tmp/ml/result/LDA/nytimes_dataframe_fit1_raw
bash ./bin/compare/ml/lda_compare.sh /tmp/ml/result/LDA/nytimes_dataframe_fit2 /tmp/ml/result/LDA/nytimes_dataframe_fit2_raw
bash ./bin/compare/ml/lda_compare.sh /tmp/ml/result/LDA/nytimes_dataframe_fit3 /tmp/ml/result/LDA/nytimes_dataframe_fit3_raw
bash ./bin/compare/ml/lda_compare.sh /tmp/ml/result/LDA/pubmed_dataframe_fit /tmp/ml/result/LDA/pubmed_dataframe_fit_raw
bash ./bin/compare/ml/lda_compare.sh /tmp/ml/result/LDA/pubmed_dataframe_fit1 /tmp/ml/result/LDA/pubmed_dataframe_fit1_raw
bash ./bin/compare/ml/lda_compare.sh /tmp/ml/result/LDA/pubmed_dataframe_fit2 /tmp/ml/result/LDA/pubmed_dataframe_fit2_raw
bash ./bin/compare/ml/lda_compare.sh /tmp/ml/result/LDA/pubmed_dataframe_fit3 /tmp/ml/result/LDA/pubmed_dataframe_fit3_raw

#LinR
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/LinR/Twitter_aarch64 /tmp/ml/result/LinR/Twitter_aarch64_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/LinR/mnist8m_aarch64 /tmp/ml/result/LinR/mnist8m_aarch64_raw
bash ./bin/compare/ml/downevaluation_compare.sh /tmp/ml/result/LinR/rcv_aarch64 /tmp/ml/result/LinR/rcv_aarch64_raw

#LogR
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/LogR/Twitter /tmp/ml/result/LogR/Twitter_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/LogR/mnist8m /tmp/ml/result/LogR/mnist8m_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/LogR/rcv /tmp/ml/result/LogR/rcv_raw

#SVM
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/SVM/D10M4096_fit /tmp/ml/result/SVM/D10M4096_fit_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/SVM/D10M4096_fit1 /tmp/ml/result/SVM/D10M4096_fit1_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/SVM/D10M4096_fit2 /tmp/ml/result/SVM/D10M4096_fit2_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/SVM/D10M4096_fit3 /tmp/ml/result/SVM/D10M4096_fit3_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/SVM/D40M1K_fit /tmp/ml/result/SVM/D40M1K_fit_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/SVM/D40M1K_fit1 /tmp/ml/result/SVM/D40M1K_fit1_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/SVM/D40M1K_fit2 /tmp/ml/result/SVM/D40M1K_fit2_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/SVM/D40M1K_fit3 /tmp/ml/result/SVM/D40M1K_fit3_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/SVM/D280M118_fit /tmp/ml/result/SVM/D280M118_fit_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/SVM/D280M118_fit1 /tmp/ml/result/SVM/D280M118_fit1_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/SVM/D280M118_fit2 /tmp/ml/result/SVM/D280M118_fit2_raw
bash ./bin/compare/ml/upevaluation_compare.sh /tmp/ml/result/SVM/D280M118_fit3 /tmp/ml/result/SVM/D280M118_fit3_raw

#Cov
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/Cov/CP10M1K /tmp/ml/result/Cov/CP10M1K_raw
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/Cov/CP1M10K /tmp/ml/result/Cov/CP1M10K_raw
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/Cov/CP2M5K /tmp/ml/result/Cov/CP2M5K_raw

#PCA
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/PCA/dataframe_D10M1K_fit /tmp/ml/result/PCA/dataframe_D10M1K_fit_raw
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/PCA/dataframe_D10M1K_fit1 /tmp/ml/result/PCA/dataframe_D10M1K_fit1_raw
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/PCA/dataframe_D10M1K_fit2 /tmp/ml/result/PCA/dataframe_D10M1K_fit2_raw
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/PCA/dataframe_D10M1K_fit3 /tmp/ml/result/PCA/dataframe_D10M1K_fit3_raw
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/PCA/dataframe_MESH_fit /tmp/ml/result/PCA/dataframe_MESH_fit_raw
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/PCA/dataframe_MESH_fit1 /tmp/ml/result/PCA/dataframe_MESH_fit1_raw
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/PCA/dataframe_MESH_fit2 /tmp/ml/result/PCA/dataframe_MESH_fit2_raw
bash ./bin/compare/ml/matrix_compare.sh /tmp/ml/result/PCA/dataframe_MESH_fit3 /tmp/ml/result/PCA/dataframe_MESH_fit3_raw