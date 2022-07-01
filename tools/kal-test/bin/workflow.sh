#!/bin/bash

# dt
./bin/ml/dt_run.sh classification dataframe epsilon fit  no 2>&1 | tee -a logs/dt_epsilon_fit.log
./bin/ml/dt_run.sh classification dataframe epsilon fit1 no 2>&1 | tee -a logs/dt_epsilon_fit1.log
./bin/ml/dt_run.sh classification dataframe epsilon fit2 no 2>&1 | tee -a logs/dt_epsilon_fit2.log
./bin/ml/dt_run.sh classification dataframe epsilon fit3 no 2>&1 | tee -a logs/dt_epsilon_fit3.log

./bin/ml/dt_run.sh classification dataframe higgs fit  no 2>&1 | tee -a logs/dt_higgs_fit.log
./bin/ml/dt_run.sh classification dataframe higgs fit1 no 2>&1 | tee -a logs/dt_higgs_fit1.log
./bin/ml/dt_run.sh classification dataframe higgs fit2 no 2>&1 | tee -a logs/dt_higgs_fit2.log
./bin/ml/dt_run.sh classification dataframe higgs fit3 no 2>&1 | tee -a logs/dt_higgs_fit3.log

./bin/ml/dt_run.sh classification dataframe mnist8m fit  no 2>&1 | tee -a logs/dt_mnist8m_fit.log
./bin/ml/dt_run.sh classification dataframe mnist8m fit1 no 2>&1 | tee -a logs/dt_mnist8m_fit1.log
./bin/ml/dt_run.sh classification dataframe mnist8m fit2 no 2>&1 | tee -a logs/dt_mnist8m_fit2.log
./bin/ml/dt_run.sh classification dataframe mnist8m fit3 no 2>&1 | tee -a logs/dt_mnist8m_fit3.log

./bin/ml/dt_run.sh regression dataframe epsilon fit  no 2>&1 | tee -a logs/dt_epsilon_fit.log
./bin/ml/dt_run.sh regression dataframe epsilon fit1 no 2>&1 | tee -a logs/dt_epsilon_fit1.log
./bin/ml/dt_run.sh regression dataframe epsilon fit2 no 2>&1 | tee -a logs/dt_epsilon_fit2.log
./bin/ml/dt_run.sh regression dataframe epsilon fit3 no 2>&1 | tee -a logs/dt_epsilon_fit3.log

./bin/ml/dt_run.sh regression dataframe higgs fit  no 2>&1 | tee -a logs/dt_higgs_fit.log
./bin/ml/dt_run.sh regression dataframe higgs fit1 no 2>&1 | tee -a logs/dt_higgs_fit1.log
./bin/ml/dt_run.sh regression dataframe higgs fit2 no 2>&1 | tee -a logs/dt_higgs_fit2.log
./bin/ml/dt_run.sh regression dataframe higgs fit3 no 2>&1 | tee -a logs/dt_higgs_fit3.log

./bin/ml/dt_run.sh regression dataframe mnist8m fit  no 2>&1 | tee -a logs/dt_mnist8m_fit.log
./bin/ml/dt_run.sh regression dataframe mnist8m fit1 no 2>&1 | tee -a logs/dt_mnist8m_fit1.log
./bin/ml/dt_run.sh regression dataframe mnist8m fit2 no 2>&1 | tee -a logs/dt_mnist8m_fit2.log
./bin/ml/dt_run.sh regression dataframe mnist8m fit3 no 2>&1 | tee -a logs/dt_mnist8m_fit3.log

# dtb
./bin/ml/dtb_run.sh higgs fit  verify no 2>&1 | tee -a logs/dtb_higgs_fit.log
./bin/ml/dtb_run.sh higgs fit1 verify no 2>&1 | tee -a logs/dtb_higgs_fi1.log
./bin/ml/dtb_run.sh higgs fit2 verify no 2>&1 | tee -a logs/dtb_higgs_fit2.log
./bin/ml/dtb_run.sh higgs fit3 verify no 2>&1 | tee -a logs/dtb_higgs_fit3.log

./bin/ml/dtb_run.sh mnist8m fit  verify no 2>&1 | tee -a logs/dtb_mnist8m_fit.log
./bin/ml/dtb_run.sh mnist8m fit1 verify no 2>&1 | tee -a logs/dtb_mnist8m_fi1.log
./bin/ml/dtb_run.sh mnist8m fit2 verify no 2>&1 | tee -a logs/dtb_mnist8m_fit2.log
./bin/ml/dtb_run.sh mnist8m fit3 verify no 2>&1 | tee -a logs/dtb_mnist8m_fit3.log

# word2vec
./bin/ml/word2vec_run.sh cate fit  no 2>&1 | tee -a logs/word2vec_cate_fit.log
./bin/ml/word2vec_run.sh cate fit1 no 2>&1 | tee -a logs/word2vec_cate_fit1.log
./bin/ml/word2vec_run.sh cate fit2 no 2>&1 | tee -a logs/word2vec_cate_fit2.log
./bin/ml/word2vec_run.sh cate fit3 no 2>&1 | tee -a logs/word2vec_cate_fit3.log

./bin/ml/word2vec_run.sh item fit  no 2>&1 | tee -a logs/word2vec_item_fit.log
./bin/ml/word2vec_run.sh item fit1 no 2>&1 | tee -a logs/word2vec_item_fit1.log
./bin/ml/word2vec_run.sh item fit2 no 2>&1 | tee -a logs/word2vec_item_fit2.log
./bin/ml/word2vec_run.sh item fit3 no 2>&1 | tee -a logs/word2vec_item_fit3.log

./bin/ml/word2vec_run.sh node fit  no 2>&1 | tee -a logs/word2vec_node_fit.log
./bin/ml/word2vec_run.sh node fit1 no 2>&1 | tee -a logs/word2vec_node_fit1.log
./bin/ml/word2vec_run.sh node fit2 no 2>&1 | tee -a logs/word2vec_node_fit2.log
./bin/ml/word2vec_run.sh node fit3 no 2>&1 | tee -a logs/word2vec_node_fit3.log

./bin/ml/word2vec_run.sh taobao fit  no 2>&1 | tee -a logs/word2vec_taobao_fit.log
./bin/ml/word2vec_run.sh taobao fit1 no 2>&1 | tee -a logs/word2vec_taobao_fit1.log
./bin/ml/word2vec_run.sh taobao fit2 no 2>&1 | tee -a logs/word2vec_taobao_fit2.log
./bin/ml/word2vec_run.sh taobao fit3 no 2>&1 | tee -a logs/word2vec_taobao_fit3.log

# gbdt
./bin/ml/gbdt_run.sh classification dataframe epsilon fit
./bin/ml/gbdt_run.sh classification dataframe epsilon fit1
./bin/ml/gbdt_run.sh classification dataframe epsilon fit2
./bin/ml/gbdt_run.sh classification dataframe epsilon fit3

./bin/ml/gbdt_run.sh regression dataframe epsilon fit
./bin/ml/gbdt_run.sh regression dataframe epsilon fit1
./bin/ml/gbdt_run.sh regression dataframe epsilon fit2
./bin/ml/gbdt_run.sh regression dataframe epsilon fit3

./bin/ml/gbdt_run.sh classification rdd epsilon rdd
./bin/ml/gbdt_run.sh classification rdd epsilon javardd
./bin/ml/gbdt_run.sh regression rdd epsilon rdd
./bin/ml/gbdt_run.sh regression rdd epsilon javardd