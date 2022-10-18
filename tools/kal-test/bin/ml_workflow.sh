#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <isRaw>"
  echo "1st argument: optimization algorithm or raw: no/yes"
  echo "2nd argument: Whether to Compare Results: no/yes"
  exit 0
  ;;
esac

if [ $# -ne 2 ]; then
  echo "please input 1 arguments: <isRaw>"
  echo "1st argument: optimization algorithm or raw: no/yes"
  echo "2nd argument: Whether to Compare Results: no/yes"
  exit 0
fi

is_raw=$1
if_check=$2
type=arm
if [ $is_raw == "yes" ]; then
  type=raw
fi

function createDir() {
    dir=$1
    if [ ! -d $dir ]; then
      mkdir $dir
    fi
}
createDir logs
createDir log

ml_classpath=/opt/ml_classpath/
function ssh_mkdir() {
    server=$1
    dir=$2
    ssh $server "mkdir -p $dir"
}
ssh_mkdir agent1 $ml_classpath
ssh_mkdir agent2 $ml_classpath
ssh_mkdir agent3 $ml_classpath

# ALS
bash bin/ml/als_run.sh dataframe als fit ${is_raw} ${if_check} 2>&1 | tee -a logs/als_als_fit_${type}.log
bash bin/ml/als_run.sh dataframe als fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/als_als_fit1_${type}.log
bash bin/ml/als_run.sh dataframe als fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/als_als_fit2_${type}.log
bash bin/ml/als_run.sh dataframe als fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/als_als_fit3_${type}.log

bash bin/ml/als_run.sh dataframe alsbs fit ${is_raw} ${if_check} 2>&1 | tee -a logs/als_alsbs_fit_${type}.log
bash bin/ml/als_run.sh dataframe alsbs fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/als_alsbs_fit1_${type}.log
bash bin/ml/als_run.sh dataframe alsbs fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/als_alsbs_fit2_${type}.log
bash bin/ml/als_run.sh dataframe alsbs fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/als_alsbs_fit3_${type}.log

bash bin/ml/als_run.sh dataframe alsh fit ${is_raw} ${if_check} 2>&1 | tee -a logs/als_alsh_fit_${type}.log
bash bin/ml/als_run.sh dataframe alsh fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/als_alsh_fit1_${type}.log
bash bin/ml/als_run.sh dataframe alsh fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/als_alsh_fit2_${type}.log
bash bin/ml/als_run.sh dataframe alsh fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/als_alsh_fit3_${type}.log

# Cov
bash bin/ml/cov_run.sh CP10M1K ${is_raw} ${if_check} 2>&1 | tee -a logs/cov_CP10M1K_${type}.log
bash bin/ml/cov_run.sh CP2M5K ${is_raw} ${if_check} 2>&1 | tee -a logs/cov_CP2M5K_${type}.log
bash bin/ml/cov_run.sh CP1M10K ${is_raw} ${if_check} 2>&1 | tee -a logs/cov_CP1M10K_${type}.log

#DBSCAN
bash bin/ml/dbscan_run.sh bremenSmall ${is_raw} 2>&1 | tee -a logs/dbscan_bremenSmall_${type}.log
bash bin/ml/dbscan_run.sh farm ${is_raw} 2>&1 | tee -a logs/dbscan_farm_${type}.log
bash bin/ml/dbscan_run.sh house ${is_raw} 2>&1 | tee -a logs/dbscan_house_${type}.log

# dtb
bash bin/ml/dtb_run.sh higgs fit  verify ${is_raw} 2>&1 | tee -a logs/dtb_higgs_fit_${type}.log
bash bin/ml/dtb_run.sh higgs fit1 verify ${is_raw} 2>&1 | tee -a logs/dtb_higgs_fi1_${type}.log
bash bin/ml/dtb_run.sh higgs fit2 verify ${is_raw} 2>&1 | tee -a logs/dtb_higgs_fit2_${type}.log
bash bin/ml/dtb_run.sh higgs fit3 verify ${is_raw} 2>&1 | tee -a logs/dtb_higgs_fit3_${type}.log

bash bin/ml/dtb_run.sh mnist8m fit  verify ${is_raw} 2>&1 | tee -a logs/dtb_mnist8m_fit_${type}.log
bash bin/ml/dtb_run.sh mnist8m fit1 verify ${is_raw} 2>&1 | tee -a logs/dtb_mnist8m_fi1_${type}.log
bash bin/ml/dtb_run.sh mnist8m fit2 verify ${is_raw} 2>&1 | tee -a logs/dtb_mnist8m_fit2_${type}.log
bash bin/ml/dtb_run.sh mnist8m fit3 verify ${is_raw} 2>&1 | tee -a logs/dtb_mnist8m_fit3_${type}.log

# DT
bash bin/ml/dt_run.sh classification dataframe epsilon fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/dtc_epsilon_fit_${type}.log
bash bin/ml/dt_run.sh classification dataframe epsilon fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/dtc_epsilon_fit1_${type}.log
bash bin/ml/dt_run.sh classification dataframe epsilon fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/dtc_epsilon_fit2_${type}.log
bash bin/ml/dt_run.sh classification dataframe epsilon fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/dtc_epsilon_fit3_${type}.log

bash bin/ml/dt_run.sh classification dataframe higgs fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/dtc_higgs_fit_${type}.log
bash bin/ml/dt_run.sh classification dataframe higgs fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/dtc_higgs_fit1_${type}.log
bash bin/ml/dt_run.sh classification dataframe higgs fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/dtc_higgs_fit2_${type}.log
bash bin/ml/dt_run.sh classification dataframe higgs fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/dtc_higgs_fit3_${type}.log

bash bin/ml/dt_run.sh classification dataframe mnist8m fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/dtc_mnist8m_fit_${type}.log
bash bin/ml/dt_run.sh classification dataframe mnist8m fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/dtc_mnist8m_fit1_${type}.log
bash bin/ml/dt_run.sh classification dataframe mnist8m fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/dtc_mnist8m_fit2_${type}.log
bash bin/ml/dt_run.sh classification dataframe mnist8m fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/dtc_mnist8m_fit3_${type}.log

bash bin/ml/dt_run.sh regression dataframe epsilon fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/dtr_epsilon_fit_${type}.log
bash bin/ml/dt_run.sh regression dataframe epsilon fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/dtr_epsilon_fit1_${type}.log
bash bin/ml/dt_run.sh regression dataframe epsilon fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/dtr_epsilon_fit2_${type}.log
bash bin/ml/dt_run.sh regression dataframe epsilon fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/dtr_epsilon_fit3_${type}.log

bash bin/ml/dt_run.sh regression dataframe higgs fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/dtr_higgs_fit_${type}.log
bash bin/ml/dt_run.sh regression dataframe higgs fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/dtr_higgs_fit1_${type}.log
bash bin/ml/dt_run.sh regression dataframe higgs fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/dtr_higgs_fit2_${type}.log
bash bin/ml/dt_run.sh regression dataframe higgs fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/dtr_higgs_fit3_${type}.log

bash bin/ml/dt_run.sh regression dataframe mnist8m fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/dtr_mnist8m_fit_${type}.log
bash bin/ml/dt_run.sh regression dataframe mnist8m fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/dtr_mnist8m_fit1_${type}.log
bash bin/ml/dt_run.sh regression dataframe mnist8m fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/dtr_mnist8m_fit2_${type}.log
bash bin/ml/dt_run.sh regression dataframe mnist8m fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/dtr_mnist8m_fit3_${type}.log

# gbdt
bash bin/ml/gbdt_run.sh classification dataframe epsilon fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtc_epsilon_fit_${type}.log
bash bin/ml/gbdt_run.sh classification dataframe epsilon fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtc_epsilon_fit1_${type}.log
bash bin/ml/gbdt_run.sh classification dataframe epsilon fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtc_epsilon_fit2_${type}.log
bash bin/ml/gbdt_run.sh classification dataframe epsilon fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtc_epsilon_fit3_${type}.log

bash bin/ml/gbdt_run.sh classification dataframe rcv fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtc_rcv_fit_${type}.log
bash bin/ml/gbdt_run.sh classification dataframe rcv fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtc_rcv_fit1_${type}.log
bash bin/ml/gbdt_run.sh classification dataframe rcv fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtc_rcv_fit2_${type}.log
bash bin/ml/gbdt_run.sh classification dataframe rcv fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtc_rcv_fit3_${type}.log

bash bin/ml/gbdt_run.sh classification dataframe D10M4096libsvm fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtc_D10M4096libsvm_fit_${type}.log
bash bin/ml/gbdt_run.sh classification dataframe D10M4096libsvm fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtc_D10M4096libsvm_fit1_${type}.log
bash bin/ml/gbdt_run.sh classification dataframe D10M4096libsvm fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtc_D10M4096libsvm_fit2_${type}.log
bash bin/ml/gbdt_run.sh classification dataframe D10M4096libsvm fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtc_D10M4096libsvm_fit3_${type}.log

bash bin/ml/gbdt_run.sh regression dataframe epsilon fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtr_epsilon_fit_${type}.log
bash bin/ml/gbdt_run.sh regression dataframe epsilon fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtr_epsilon_fit1_${type}.log
bash bin/ml/gbdt_run.sh regression dataframe epsilon fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtr_epsilon_fit2_${type}.log
bash bin/ml/gbdt_run.sh regression dataframe epsilon fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtr_epsilon_fit3_${type}.log

bash bin/ml/gbdt_run.sh regression dataframe rcv fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtr_rcv_fit_${type}.log
bash bin/ml/gbdt_run.sh regression dataframe rcv fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtr_rcv_fit1_${type}.log
bash bin/ml/gbdt_run.sh regression dataframe rcv fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtr_rcv_fit2_${type}.log
bash bin/ml/gbdt_run.sh regression dataframe rcv fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtr_rcv_fit3_${type}.log

bash bin/ml/gbdt_run.sh regression dataframe D10M4096libsvm fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtr_D10M4096libsvm_fit_${type}.log
bash bin/ml/gbdt_run.sh regression dataframe D10M4096libsvm fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtr_D10M4096libsvm_fit1_${type}.log
bash bin/ml/gbdt_run.sh regression dataframe D10M4096libsvm fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtr_D10M4096libsvm_fit2_${type}.log
bash bin/ml/gbdt_run.sh regression dataframe D10M4096libsvm fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtr_D10M4096libsvm_fit3_${type}.log

# IDF
bash bin/ml/idf_run.sh D10m200m ${is_raw} ${if_check} 2>&1 | tee -a logs/idf_D10m200m_${type}.log
bash bin/ml/idf_run.sh D2g250m ${is_raw} ${if_check} 2>&1 | tee -a logs/idf_D2g250m_${type}.log

# KMEANS
bash bin/ml/kmeans_run.sh dataframe D1200M20 fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/kmeans_D1200M20_fit_${type}.log
bash bin/ml/kmeans_run.sh dataframe D1200M20 fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/kmeans_D1200M20_fit1_${type}.log
bash bin/ml/kmeans_run.sh dataframe D1200M20 fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/kmeans_D1200M20_fit2_${type}.log
bash bin/ml/kmeans_run.sh dataframe D1200M20 fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/kmeans_D1200M20_fit3_${type}.log

bash bin/ml/kmeans_run.sh dataframe D200M20 fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/kmeans_D200M20_fit_${type}.log
bash bin/ml/kmeans_run.sh dataframe D200M20 fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/kmeans_D200M20_fit1_${type}.log
bash bin/ml/kmeans_run.sh dataframe D200M20 fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/kmeans_D200M20_fit2_${type}.log
bash bin/ml/kmeans_run.sh dataframe D200M20 fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/kmeans_D200M20_fit3_${type}.log

bash bin/ml/kmeans_run.sh dataframe D200M100 fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/kmeans_D200M100_fit_${type}.log
bash bin/ml/kmeans_run.sh dataframe D200M100 fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/kmeans_D200M100_fit1_${type}.log
bash bin/ml/kmeans_run.sh dataframe D200M100 fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/kmeans_D200M100_fit2_${type}.log
bash bin/ml/kmeans_run.sh dataframe D200M100 fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/kmeans_D200M100_fit3_${type}.log

#knn
bash bin/ml/knn_run.sh glove ${is_raw} 2>&1 | tee -a logs/knn_glove_${type}.log
bash bin/ml/knn_run.sh gist ${is_raw} 2>&1 | tee -a logs/knn_gist_${type}.log
bash bin/ml/knn_run.sh deep1b ${is_raw} 2>&1 | tee -a logs/knn_deep1b_${type}.log

#lda
bash bin/ml/lda_run.sh dataframe nytimes fit ${is_raw} ${if_check} 2>&1 | tee -a logs/lda_nytimes_fit_${type}.log
bash bin/ml/lda_run.sh dataframe nytimes fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/lda_nytimes_fit1_${type}.log
bash bin/ml/lda_run.sh dataframe nytimes fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/lda_nytimes_fit2_${type}.log
bash bin/ml/lda_run.sh dataframe nytimes fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/lda_nytimes_fit3_${type}.log

bash bin/ml/lda_run.sh dataframe pubmed fit ${is_raw} ${if_check} 2>&1 | tee -a logs/lda_pubmed_fit_${type}.log
bash bin/ml/lda_run.sh dataframe pubmed fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/lda_pubmed_fit1_${type}.log
bash bin/ml/lda_run.sh dataframe pubmed fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/lda_pubmed_fit2_${type}.log
bash bin/ml/lda_run.sh dataframe pubmed fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/lda_pubmed_fit3_${type}.log

bash bin/ml/lda_run.sh dataframe D20M200K fit ${is_raw} ${if_check} 2>&1 | tee -a logs/lda_D20M200K_fit_${type}.log
bash bin/ml/lda_run.sh dataframe D20M200K fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/lda_D20M200K_fit1_${type}.log
bash bin/ml/lda_run.sh dataframe D20M200K fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/lda_D20M200K_fit2_${type}.log
bash bin/ml/lda_run.sh dataframe D20M200K fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/lda_D20M200K_fit3_${type}.log

#linR
bash bin/ml/linR_run.sh mnist8m fit ${is_raw} ${if_check} 2>&1 | tee -a logs/linR_mnist8m_fit_${type}.log
bash bin/ml/linR_run.sh mnist8m fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/linR_mnist8m_fit1_${type}.log
bash bin/ml/linR_run.sh mnist8m fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/linR_mnist8m_fit2_${type}.log
bash bin/ml/linR_run.sh mnist8m fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/linR_mnist8m_fit3_${type}.log

bash bin/ml/linR_run.sh Twitter fit ${is_raw} ${if_check} 2>&1 | tee -a logs/linR_Twitter_fit_${type}.log
bash bin/ml/linR_run.sh Twitter fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/linR_Twitter_fit1_${type}.log
bash bin/ml/linR_run.sh Twitter fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/linR_Twitter_fit2_${type}.log
bash bin/ml/linR_run.sh Twitter fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/linR_Twitter_fit3_${type}.log

bash bin/ml/linR_run.sh rcv fit ${is_raw} ${if_check} 2>&1 | tee -a logs/linR_rcv_fit_${type}.log
bash bin/ml/linR_run.sh rcv fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/linR_rcv_fit1_${type}.log
bash bin/ml/linR_run.sh rcv fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/linR_rcv_fit2_${type}.log
bash bin/ml/linR_run.sh rcv fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/linR_rcv_fit3_${type}.log


#logR
bash bin/ml/logR_run.sh mnist8m fit ${is_raw} ${if_check} 2>&1 | tee -a logs/logR_mnist8m_fit_${type}.log
bash bin/ml/logR_run.sh mnist8m fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/logR_mnist8m_fit1_${type}.log
bash bin/ml/logR_run.sh mnist8m fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/logR_mnist8m_fit2_${type}.log
bash bin/ml/logR_run.sh mnist8m fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/logR_mnist8m_fit3_${type}.log

bash bin/ml/logR_run.sh Twitter fit ${is_raw} ${if_check} 2>&1 | tee -a logs/logR_Twitter_fit_${type}.log
bash bin/ml/logR_run.sh Twitter fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/logR_Twitter_fit1_${type}.log
bash bin/ml/logR_run.sh Twitter fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/logR_Twitter_fit2_${type}.log
bash bin/ml/logR_run.sh Twitter fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/logR_Twitter_fit3_${type}.log

bash bin/ml/logR_run.sh rcv fit ${is_raw} ${if_check} 2>&1 | tee -a logs/logR_rcv_fit_${type}.log
bash bin/ml/logR_run.sh rcv fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/logR_rcv_fit1_${type}.log
bash bin/ml/logR_run.sh rcv fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/logR_rcv_fit2_${type}.log
bash bin/ml/logR_run.sh rcv fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/logR_rcv_fit3_${type}.log

#pca
bash bin/ml/pca_run.sh dataframe D10M1K fit ${is_raw} ${if_check} 2>&1 | tee -a logs/pca_D10M1K_fit_${type}.log
bash bin/ml/pca_run.sh dataframe D10M1K fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/pca_D10M1K_fit1_${type}.log
bash bin/ml/pca_run.sh dataframe D10M1K fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/pca_D10M1K_fit2_${type}.log
bash bin/ml/pca_run.sh dataframe D10M1K fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/pca_D10M1K_fit3_${type}.log

bash bin/ml/pca_run.sh dataframe D1M10K fit ${is_raw} ${if_check} 2>&1 | tee -a logs/pca_D1M10K_fit_${type}.log
bash bin/ml/pca_run.sh dataframe D1M10K fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/pca_D1M10K_fit1_${type}.log
bash bin/ml/pca_run.sh dataframe D1M10K fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/pca_D1M10K_fit2_${type}.log
bash bin/ml/pca_run.sh dataframe D1M10K fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/pca_D1M10K_fit3_${type}.log

bash bin/ml/pca_run.sh dataframe MESH fit ${is_raw} ${if_check} 2>&1 | tee -a logs/pca_MESH_fit_${type}.log
bash bin/ml/pca_run.sh dataframe MESH fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/pca_MESH_fit1_${type}.log
bash bin/ml/pca_run.sh dataframe MESH fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/pca_MESH_fit2_${type}.log
bash bin/ml/pca_run.sh dataframe MESH fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/pca_MESH_fit3_${type}.log

#pearson
bash bin/ml/pearson_run.sh dataframe CP10M1K ${is_raw} ${if_check} 2>&1 | tee -a logs/pearson_CP10M1K_${type}.log
bash bin/ml/pearson_run.sh dataframe CP2M5K ${is_raw} ${if_check} 2>&1 | tee -a logs/pearson_CP2M5K_${type}.log
bash bin/ml/pearson_run.sh dataframe CP1M10K ${is_raw} ${if_check} 2>&1 | tee -a logs/pearson_CP1M10K_${type}.log

#ps
bash bin/ml/ps_run.sh kosarak ${is_raw} ${if_check} 2>&1 | tee -a logs/ps_kosarak_${type}.log
bash bin/ml/ps_run.sh IBM10M47 ${is_raw} ${if_check} 2>&1 | tee -a logs/ps_IBM10M47_${type}.log
bash bin/ml/ps_run.sh IBM100M47 ${is_raw} ${if_check} 2>&1 | tee -a logs/ps_IBM100M47_${type}.log

# RF
bash bin/ml/rf_run.sh classification dataframe epsilon fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_epsilon_fit_${type}.log
bash bin/ml/rf_run.sh classification dataframe epsilon fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_epsilon_fit1_${type}.log
bash bin/ml/rf_run.sh classification dataframe epsilon fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_epsilon_fit2_${type}.log
bash bin/ml/rf_run.sh classification dataframe epsilon fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_epsilon_fit3_${type}.log

bash bin/ml/rf_run.sh classification dataframe higgs fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_higgs_fit_${type}.log
bash bin/ml/rf_run.sh classification dataframe higgs fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_higgs_fit1_${type}.log
bash bin/ml/rf_run.sh classification dataframe higgs fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_higgs_fit2_${type}.log
bash bin/ml/rf_run.sh classification dataframe higgs fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_higgs_fit3_${type}.log

bash bin/ml/rf_run.sh classification dataframe mnist8m fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_mnist8m_fit_${type}.log
bash bin/ml/rf_run.sh classification dataframe mnist8m fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_mnist8m_fit1_${type}.log
bash bin/ml/rf_run.sh classification dataframe mnist8m fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_mnist8m_fit2_${type}.log
bash bin/ml/rf_run.sh classification dataframe mnist8m fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_mnist8m_fit3_${type}.log

bash bin/ml/rf_run.sh classification dataframe rcv fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_rcv_fit_${type}.log
bash bin/ml/rf_run.sh classification dataframe rcv fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_rcv_fit1_${type}.log
bash bin/ml/rf_run.sh classification dataframe rcv fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_rcv_fit2_${type}.log
bash bin/ml/rf_run.sh classification dataframe rcv fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_rcv_fit3_${type}.log

bash bin/ml/rf_run.sh regression dataframe epsilon fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_epsilon_fit_${type}.log
bash bin/ml/rf_run.sh regression dataframe epsilon fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_epsilon_fit1_${type}.log
bash bin/ml/rf_run.sh regression dataframe epsilon fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_epsilon_fit2_${type}.log
bash bin/ml/rf_run.sh regression dataframe epsilon fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_epsilon_fit3_${type}.log

bash bin/ml/rf_run.sh regression dataframe higgs fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_higgs_fit_${type}.log
bash bin/ml/rf_run.sh regression dataframe higgs fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_higgs_fit1_${type}.log
bash bin/ml/rf_run.sh regression dataframe higgs fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_higgs_fit2_${type}.log
bash bin/ml/rf_run.sh regression dataframe higgs fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_higgs_fit3_${type}.log

bash bin/ml/rf_run.sh regression dataframe mnist8m fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_mnist8m_fit_${type}.log
bash bin/ml/rf_run.sh regression dataframe mnist8m fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_mnist8m_fit1_${type}.log
bash bin/ml/rf_run.sh regression dataframe mnist8m fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_mnist8m_fit2_${type}.log
bash bin/ml/rf_run.sh regression dataframe mnist8m fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_mnist8m_fit3_${type}.log

bash bin/ml/rf_run.sh regression dataframe rcv fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_rcv_fit_${type}.log
bash bin/ml/rf_run.sh regression dataframe rcv fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_rcv_fit1_${type}.log
bash bin/ml/rf_run.sh regression dataframe rcv fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_rcv_fit2_${type}.log
bash bin/ml/rf_run.sh regression dataframe rcv fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_rcv_fit3_${type}.log

#simrank
bash bin/ml/simrank_run.sh simrank3w ${is_raw} ${if_check} 2>&1 | tee -a logs/simrank_simrank3w_${type}.log
bash bin/ml/simrank_run.sh simrank5w ${is_raw} ${if_check} 2>&1 | tee -a logs/simrank_simrank5w_${type}.log
bash bin/ml/simrank_run.sh simrank7w ${is_raw} ${if_check} 2>&1 | tee -a logs/simrank_simrank7w_${type}.log

#spca
bash bin/ml/spca_run.sh Kemelmacher fit ${is_raw} ${if_check} 2>&1 | tee -a logs/spca_Kemelmacher_fit_${type}.log
bash bin/ml/spca_run.sh Kemelmacher fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/spca_Kemelmacher_fit1_${type}.log
bash bin/ml/spca_run.sh Kemelmacher fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/spca_Kemelmacher_fit2_${type}.log
bash bin/ml/spca_run.sh Kemelmacher fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/spca_Kemelmacher_fit3_${type}.log

bash bin/ml/spca_run.sh mesh_deform fit ${is_raw} ${if_check} 2>&1 | tee -a logs/spca_mesh_deform_fit_${type}.log
bash bin/ml/spca_run.sh mesh_deform fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/spca_mesh_deform_fit1_${type}.log
bash bin/ml/spca_run.sh mesh_deform fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/spca_mesh_deform_fit2_${type}.log
bash bin/ml/spca_run.sh mesh_deform fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/spca_mesh_deform_fit3_${type}.log

bash bin/ml/spca_run.sh wathen100 fit ${is_raw} ${if_check} 2>&1 | tee -a logs/spca_wathen100_fit_${type}.log
bash bin/ml/spca_run.sh wathen100 fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/spca_wathen100_fit1_${type}.log
bash bin/ml/spca_run.sh wathen100 fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/spca_wathen100_fit2_${type}.log
bash bin/ml/spca_run.sh wathen100 fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/spca_wathen100_fit3_${type}.log

bash bin/ml/spca_run.sh MOLIERE fit ${is_raw} ${if_check} 2>&1 | tee -a logs/spca_MOLIERE_fit_${type}.log
bash bin/ml/spca_run.sh MOLIERE fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/spca_MOLIERE_fit1_${type}.log
bash bin/ml/spca_run.sh MOLIERE fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/spca_MOLIERE_fit2_${type}.log
bash bin/ml/spca_run.sh MOLIERE fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/spca_MOLIERE_fit3_${type}.log

#spearman
bash bin/ml/spearman_run.sh dataframe CP10M1K ${is_raw} ${if_check} 2>&1 | tee -a logs/spearman_CP10M1K_${type}.log
bash bin/ml/spearman_run.sh dataframe CP2M5K ${is_raw} ${if_check} 2>&1 | tee -a logs/spearman_CP2M5K_${type}.log
bash bin/ml/spearman_run.sh dataframe CP1M10K ${is_raw} ${if_check} 2>&1 | tee -a logs/spearman_CP1M10K_${type}.log

#svd
bash bin/ml/svd_run.sh D10M1K ${is_raw} ${if_check} 2>&1 | tee -a logs/svd_D10M1K_${type}.log
bash bin/ml/svd_run.sh D1M10K ${is_raw} ${if_check} 2>&1 | tee -a logs/svd_D1M10K_${type}.log
bash bin/ml/svd_run.sh MESH ${is_raw} ${if_check} 2>&1 | tee -a logs/svd_MESH_${type}.log
bash bin/ml/svd_run.sh RUCCI ${is_raw} ${if_check} 2>&1 | tee -a logs/svd_RUCCI_${type}.log


#svm
bash bin/ml/svm_run.sh D10M4096 fit ${is_raw} ${if_check} 2>&1 | tee -a logs/svm_10M4096_fit_${type}.log
bash bin/ml/svm_run.sh D10M4096 fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/svm_10M4096_fit1_${type}.log
bash bin/ml/svm_run.sh D10M4096 fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/svm_10M4096_fit2_${type}.log
bash bin/ml/svm_run.sh D10M4096 fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/svm_10M4096_fit3_${type}.log

bash bin/ml/svm_run.sh D280M118 fit ${is_raw} ${if_check} 2>&1 | tee -a logs/svm_D280M118_fit_${type}.log
bash bin/ml/svm_run.sh D280M118 fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/svm_D280M118_fit1_${type}.log
bash bin/ml/svm_run.sh D280M118 fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/svm_D280M118_fit2_${type}.log
bash bin/ml/svm_run.sh D280M118 fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/svm_D280M118_fit3_${type}.log

bash bin/ml/svm_run.sh D40M1K fit ${is_raw} ${if_check} 2>&1 | tee -a logs/svm_D40M1K_fit_${type}.log
bash bin/ml/svm_run.sh D40M1K fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/svm_D40M1K_fit1_${type}.log
bash bin/ml/svm_run.sh D40M1K fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/svm_D40M1K_fit2_${type}.log
bash bin/ml/svm_run.sh D40M1K fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/svm_D40M1K_fit3_${type}.log

# word2vec
bash bin/ml/word2vec_run.sh cate fit  ${is_raw} 2>&1 | tee -a logs/word2vec_cate_fit_${type}.log
bash bin/ml/word2vec_run.sh cate fit1 ${is_raw} 2>&1 | tee -a logs/word2vec_cate_fit1_${type}.log
bash bin/ml/word2vec_run.sh cate fit2 ${is_raw} 2>&1 | tee -a logs/word2vec_cate_fit2_${type}.log
bash bin/ml/word2vec_run.sh cate fit3 ${is_raw} 2>&1 | tee -a logs/word2vec_cate_fit3_${type}.log

bash bin/ml/word2vec_run.sh item fit  ${is_raw} 2>&1 | tee -a logs/word2vec_item_fit_${type}.log
bash bin/ml/word2vec_run.sh item fit1 ${is_raw} 2>&1 | tee -a logs/word2vec_item_fit1_${type}.log
bash bin/ml/word2vec_run.sh item fit2 ${is_raw} 2>&1 | tee -a logs/word2vec_item_fit2_${type}.log
bash bin/ml/word2vec_run.sh item fit3 ${is_raw} 2>&1 | tee -a logs/word2vec_item_fit3_${type}.log

bash bin/ml/word2vec_run.sh node fit  ${is_raw} 2>&1 | tee -a logs/word2vec_node_fit_${type}.log
bash bin/ml/word2vec_run.sh node fit1 ${is_raw} 2>&1 | tee -a logs/word2vec_node_fit1_${type}.log
bash bin/ml/word2vec_run.sh node fit2 ${is_raw} 2>&1 | tee -a logs/word2vec_node_fit2_${type}.log
bash bin/ml/word2vec_run.sh node fit3 ${is_raw} 2>&1 | tee -a logs/word2vec_node_fit3_${type}.log

bash bin/ml/word2vec_run.sh taobao fit  ${is_raw} 2>&1 | tee -a logs/word2vec_taobao_fit_${type}.log
bash bin/ml/word2vec_run.sh taobao fit1 ${is_raw} 2>&1 | tee -a logs/word2vec_taobao_fit1_${type}.log
bash bin/ml/word2vec_run.sh taobao fit2 ${is_raw} 2>&1 | tee -a logs/word2vec_taobao_fit2_${type}.log
bash bin/ml/word2vec_run.sh taobao fit3 ${is_raw} 2>&1 | tee -a logs/word2vec_taobao_fit3_${type}.log

#xgbt
bash bin/ml/xgbt_run.sh higgs classification ${is_raw} ${if_check} 2>&1 | tee -a logs/xgbtc_higgs_${type}.log
bash bin/ml/xgbt_run.sh higgs regression ${is_raw} ${if_check} 2>&1 | tee -a logs/xgbtr_higgs_${type}.log

bash bin/ml/xgbt_run.sh mnist8m classification ${is_raw} ${if_check} 2>&1 | tee -a logs/xgbtc_mnist8m_${type}.log
bash bin/ml/xgbt_run.sh mnist8m regression ${is_raw} ${if_check} 2>&1 | tee -a logs/xgbtr_mnist8m_${type}.log