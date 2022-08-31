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

# gbdt
# gbdt
./bin/ml/gbdt_run.sh classification dataframe epsilon fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtc_epsilon_fit_${type}.log
./bin/ml/gbdt_run.sh classification dataframe epsilon fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtc_epsilon_fit1_${type}.log
./bin/ml/gbdt_run.sh classification dataframe epsilon fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtc_epsilon_fit2_${type}.log
./bin/ml/gbdt_run.sh classification dataframe epsilon fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtc_epsilon_fit3_${type}.log

./bin/ml/gbdt_run.sh classification dataframe rcv fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtc_rcv_fit_${type}.log
./bin/ml/gbdt_run.sh classification dataframe rcv fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtc_rcv_fit1_${type}.log
./bin/ml/gbdt_run.sh classification dataframe rcv fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtc_rcv_fit2_${type}.log
./bin/ml/gbdt_run.sh classification dataframe rcv fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtc_rcv_fit3_${type}.log

./bin/ml/gbdt_run.sh classification dataframe D10M4096libsvm fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtc_D10M4096libsvm_fit_${type}.log
./bin/ml/gbdt_run.sh classification dataframe D10M4096libsvm fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtc_D10M4096libsvm_fit1_${type}.log
./bin/ml/gbdt_run.sh classification dataframe D10M4096libsvm fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtc_D10M4096libsvm_fit2_${type}.log
./bin/ml/gbdt_run.sh classification dataframe D10M4096libsvm fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtc_D10M4096libsvm_fit3_${type}.log

./bin/ml/gbdt_run.sh regression dataframe epsilon fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtr_epsilon_fit_${type}.log
./bin/ml/gbdt_run.sh regression dataframe epsilon fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtr_epsilon_fit1_${type}.log
./bin/ml/gbdt_run.sh regression dataframe epsilon fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtr_epsilon_fit2_${type}.log
./bin/ml/gbdt_run.sh regression dataframe epsilon fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtr_epsilon_fit3_${type}.log

./bin/ml/gbdt_run.sh regression dataframe rcv fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtr_rcv_fit_${type}.log
./bin/ml/gbdt_run.sh regression dataframe rcv fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtr_rcv_fit1_${type}.log
./bin/ml/gbdt_run.sh regression dataframe rcv fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtr_rcv_fit2_${type}.log
./bin/ml/gbdt_run.sh regression dataframe rcv fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtr_rcv_fit3_${type}.log

./bin/ml/gbdt_run.sh regression dataframe D10M4096libsvm fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtr_D10M4096libsvm_fit_${type}.log
./bin/ml/gbdt_run.sh regression dataframe D10M4096libsvm fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtr_D10M4096libsvm_fit1_${type}.log
./bin/ml/gbdt_run.sh regression dataframe D10M4096libsvm fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtr_D10M4096libsvm_fit2_${type}.log
./bin/ml/gbdt_run.sh regression dataframe D10M4096libsvm fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/gbdtr_D10M4096libsvm_fit3_${type}.log

# RF
./bin/ml/rf_run.sh classification dataframe epsilon fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_epsilon_fit_${type}.log
./bin/ml/rf_run.sh classification dataframe epsilon fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_epsilon_fit1_${type}.log
./bin/ml/rf_run.sh classification dataframe epsilon fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_epsilon_fit2_${type}.log
./bin/ml/rf_run.sh classification dataframe epsilon fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_epsilon_fit3_${type}.log

./bin/ml/rf_run.sh classification dataframe higgs fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_higgs_fit_${type}.log
./bin/ml/rf_run.sh classification dataframe higgs fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_higgs_fit1_${type}.log
./bin/ml/rf_run.sh classification dataframe higgs fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_higgs_fit2_${type}.log
./bin/ml/rf_run.sh classification dataframe higgs fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_higgs_fit3_${type}.log

./bin/ml/rf_run.sh classification dataframe mnist8m fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_mnist8m_fit_${type}.log
./bin/ml/rf_run.sh classification dataframe mnist8m fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_mnist8m_fit1_${type}.log
./bin/ml/rf_run.sh classification dataframe mnist8m fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_mnist8m_fit2_${type}.log
./bin/ml/rf_run.sh classification dataframe mnist8m fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_mnist8m_fit3_${type}.log

./bin/ml/rf_run.sh classification dataframe rcv fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_rcv_fit_${type}.log
./bin/ml/rf_run.sh classification dataframe rcv fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_rcv_fit1_${type}.log
./bin/ml/rf_run.sh classification dataframe rcv fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_rcv_fit2_${type}.log
./bin/ml/rf_run.sh classification dataframe rcv fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfc_rcv_fit3_${type}.log

./bin/ml/rf_run.sh regression dataframe epsilon fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_epsilon_fit_${type}.log
./bin/ml/rf_run.sh regression dataframe epsilon fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_epsilon_fit1_${type}.log
./bin/ml/rf_run.sh regression dataframe epsilon fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_epsilon_fit2_${type}.log
./bin/ml/rf_run.sh regression dataframe epsilon fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_epsilon_fit3_${type}.log

./bin/ml/rf_run.sh regression dataframe higgs fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_higgs_fit_${type}.log
./bin/ml/rf_run.sh regression dataframe higgs fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_higgs_fit1_${type}.log
./bin/ml/rf_run.sh regression dataframe higgs fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_higgs_fit2_${type}.log
./bin/ml/rf_run.sh regression dataframe higgs fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_higgs_fit3_${type}.log

./bin/ml/rf_run.sh regression dataframe mnist8m fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_mnist8m_fit_${type}.log
./bin/ml/rf_run.sh regression dataframe mnist8m fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_mnist8m_fit1_${type}.log
./bin/ml/rf_run.sh regression dataframe mnist8m fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_mnist8m_fit2_${type}.log
./bin/ml/rf_run.sh regression dataframe mnist8m fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_mnist8m_fit3_${type}.log

./bin/ml/rf_run.sh regression dataframe rcv fit  ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_rcv_fit_${type}.log
./bin/ml/rf_run.sh regression dataframe rcv fit1 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_rcv_fit1_${type}.log
./bin/ml/rf_run.sh regression dataframe rcv fit2 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_rcv_fit2_${type}.log
./bin/ml/rf_run.sh regression dataframe rcv fit3 ${is_raw} ${if_check} 2>&1 | tee -a logs/rfr_rcv_fit3_${type}.log

# dt
./bin/ml/dt_run.sh classification dataframe epsilon fit  ${is_raw} 2>&1 | tee -a logs/dtc_epsilon_fit_${type}.log
./bin/ml/dt_run.sh classification dataframe epsilon fit1 ${is_raw} 2>&1 | tee -a logs/dtc_epsilon_fit1_${type}.log
./bin/ml/dt_run.sh classification dataframe epsilon fit2 ${is_raw} 2>&1 | tee -a logs/dtc_epsilon_fit2_${type}.log
./bin/ml/dt_run.sh classification dataframe epsilon fit3 ${is_raw} 2>&1 | tee -a logs/dtc_epsilon_fit3_${type}.log

./bin/ml/dt_run.sh classification dataframe higgs fit  ${is_raw} 2>&1 | tee -a logs/dtc_higgs_fit_${type}.log
./bin/ml/dt_run.sh classification dataframe higgs fit1 ${is_raw} 2>&1 | tee -a logs/dtc_higgs_fit1_${type}.log
./bin/ml/dt_run.sh classification dataframe higgs fit2 ${is_raw} 2>&1 | tee -a logs/dtc_higgs_fit2_${type}.log
./bin/ml/dt_run.sh classification dataframe higgs fit3 ${is_raw} 2>&1 | tee -a logs/dtc_higgs_fit3_${type}.log

./bin/ml/dt_run.sh classification dataframe mnist8m fit  ${is_raw} 2>&1 | tee -a logs/dtc_mnist8m_fit_${type}.log
./bin/ml/dt_run.sh classification dataframe mnist8m fit1 ${is_raw} 2>&1 | tee -a logs/dtc_mnist8m_fit1_${type}.log
./bin/ml/dt_run.sh classification dataframe mnist8m fit2 ${is_raw} 2>&1 | tee -a logs/dtc_mnist8m_fit2_${type}.log
./bin/ml/dt_run.sh classification dataframe mnist8m fit3 ${is_raw} 2>&1 | tee -a logs/dtc_mnist8m_fit3_${type}.log

./bin/ml/dt_run.sh regression dataframe epsilon fit  ${is_raw} 2>&1 | tee -a logs/dtr_epsilon_fit_${type}.log
./bin/ml/dt_run.sh regression dataframe epsilon fit1 ${is_raw} 2>&1 | tee -a logs/dtr_epsilon_fit1_${type}.log
./bin/ml/dt_run.sh regression dataframe epsilon fit2 ${is_raw} 2>&1 | tee -a logs/dtr_epsilon_fit2_${type}.log
./bin/ml/dt_run.sh regression dataframe epsilon fit3 ${is_raw} 2>&1 | tee -a logs/dtr_epsilon_fit3_${type}.log

./bin/ml/dt_run.sh regression dataframe higgs fit  ${is_raw} 2>&1 | tee -a logs/dtr_higgs_fit_${type}.log
./bin/ml/dt_run.sh regression dataframe higgs fit1 ${is_raw} 2>&1 | tee -a logs/dtr_higgs_fit1_${type}.log
./bin/ml/dt_run.sh regression dataframe higgs fit2 ${is_raw} 2>&1 | tee -a logs/dtr_higgs_fit2_${type}.log
./bin/ml/dt_run.sh regression dataframe higgs fit3 ${is_raw} 2>&1 | tee -a logs/dtr_higgs_fit3_${type}.log

./bin/ml/dt_run.sh regression dataframe mnist8m fit  ${is_raw} 2>&1 | tee -a logs/dtr_mnist8m_fit_${type}.log
./bin/ml/dt_run.sh regression dataframe mnist8m fit1 ${is_raw} 2>&1 | tee -a logs/dtr_mnist8m_fit1_${type}.log
./bin/ml/dt_run.sh regression dataframe mnist8m fit2 ${is_raw} 2>&1 | tee -a logs/dtr_mnist8m_fit2_${type}.log
./bin/ml/dt_run.sh regression dataframe mnist8m fit3 ${is_raw} 2>&1 | tee -a logs/dtr_mnist8m_fit3_${type}.log

# dtb
./bin/ml/dtb_run.sh higgs fit  verify ${is_raw} 2>&1 | tee -a logs/dtb_higgs_fit_${type}.log
./bin/ml/dtb_run.sh higgs fit1 verify ${is_raw} 2>&1 | tee -a logs/dtb_higgs_fi1_${type}.log
./bin/ml/dtb_run.sh higgs fit2 verify ${is_raw} 2>&1 | tee -a logs/dtb_higgs_fit2_${type}.log
./bin/ml/dtb_run.sh higgs fit3 verify ${is_raw} 2>&1 | tee -a logs/dtb_higgs_fit3_${type}.log

./bin/ml/dtb_run.sh mnist8m fit  verify ${is_raw} 2>&1 | tee -a logs/dtb_mnist8m_fit_${type}.log
./bin/ml/dtb_run.sh mnist8m fit1 verify ${is_raw} 2>&1 | tee -a logs/dtb_mnist8m_fi1_${type}.log
./bin/ml/dtb_run.sh mnist8m fit2 verify ${is_raw} 2>&1 | tee -a logs/dtb_mnist8m_fit2_${type}.log
./bin/ml/dtb_run.sh mnist8m fit3 verify ${is_raw} 2>&1 | tee -a logs/dtb_mnist8m_fit3_${type}.log

# word2vec
./bin/ml/word2vec_run.sh cate fit  ${is_raw} 2>&1 | tee -a logs/word2vec_cate_fit_${type}.log
./bin/ml/word2vec_run.sh cate fit1 ${is_raw} 2>&1 | tee -a logs/word2vec_cate_fit1_${type}.log
./bin/ml/word2vec_run.sh cate fit2 ${is_raw} 2>&1 | tee -a logs/word2vec_cate_fit2_${type}.log
./bin/ml/word2vec_run.sh cate fit3 ${is_raw} 2>&1 | tee -a logs/word2vec_cate_fit3_${type}.log

./bin/ml/word2vec_run.sh item fit  ${is_raw} 2>&1 | tee -a logs/word2vec_item_fit_${type}.log
./bin/ml/word2vec_run.sh item fit1 ${is_raw} 2>&1 | tee -a logs/word2vec_item_fit1_${type}.log
./bin/ml/word2vec_run.sh item fit2 ${is_raw} 2>&1 | tee -a logs/word2vec_item_fit2_${type}.log
./bin/ml/word2vec_run.sh item fit3 ${is_raw} 2>&1 | tee -a logs/word2vec_item_fit3_${type}.log

./bin/ml/word2vec_run.sh node fit  ${is_raw} 2>&1 | tee -a logs/word2vec_node_fit_${type}.log
./bin/ml/word2vec_run.sh node fit1 ${is_raw} 2>&1 | tee -a logs/word2vec_node_fit1_${type}.log
./bin/ml/word2vec_run.sh node fit2 ${is_raw} 2>&1 | tee -a logs/word2vec_node_fit2_${type}.log
./bin/ml/word2vec_run.sh node fit3 ${is_raw} 2>&1 | tee -a logs/word2vec_node_fit3_${type}.log

./bin/ml/word2vec_run.sh taobao fit  ${is_raw} 2>&1 | tee -a logs/word2vec_taobao_fit_${type}.log
./bin/ml/word2vec_run.sh taobao fit1 ${is_raw} 2>&1 | tee -a logs/word2vec_taobao_fit1_${type}.log
./bin/ml/word2vec_run.sh taobao fit2 ${is_raw} 2>&1 | tee -a logs/word2vec_taobao_fit2_${type}.log
./bin/ml/word2vec_run.sh taobao fit3 ${is_raw} 2>&1 | tee -a logs/word2vec_taobao_fit3_${type}.log
