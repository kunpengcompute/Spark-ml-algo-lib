#!/bin/bash

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