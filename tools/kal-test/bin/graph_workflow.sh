#!/bin/bash
set -e

function alg_usage() {
  echo "Usage:  <is raw> <is check>"
  echo "1st argument: optimization algorithm or raw: no/yes"
  echo "2nd argument: verify result: no/yes"
}

case "$1" in
-h | --help | ?)
  alg_usage
  exit 0
  ;;
esac

if [ $# -ne 2 ]; then
  alg_usage
  exit 0
fi

is_raw=$1
is_check=$2

type=opt
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
createDir report

graph_classpath=/opt/graph_classpath/
function ssh_mkdir() {
    server=$1
    dir=$2
    ssh $server "mkdir -p $dir"
}
ssh_mkdir agent1 $graph_classpath
ssh_mkdir agent2 $graph_classpath
ssh_mkdir agent3 $graph_classpath

# betweenness
bash bin/graph/betweenness_run.sh cit_patents ${is_raw} ${is_check} 2>&1 | tee -a logs/betweenness_cit_patents_${type}.log
bash bin/graph/betweenness_run.sh enwiki_2018 ${is_raw} ${is_check} 2>&1 | tee -a logs/betweenness_enwiki_2018_${type}.log
bash bin/graph/betweenness_run.sh uk_2002 ${is_raw} ${is_check} 2>&1 | tee -a logs/betweenness_uk_2002_${type}.log

# bfs
bash bin/graph/bfs_run.sh cit_patents ${is_raw} 2>&1 | tee -a logs/bfs_cit_patents_${type}.log
bash bin/graph/bfs_run.sh enwiki_2018 ${is_raw} 2>&1 | tee -a logs/bfs_enwiki_2018_${type}.log
bash bin/graph/bfs_run.sh arabic_2005 ${is_raw} 2>&1 | tee -a logs/bfs_arabic_2005_${type}.log
bash bin/graph/bfs_run.sh graph500_22 ${is_raw} 2>&1 | tee -a logs/bfs_graph500_22_${type}.log
bash bin/graph/bfs_run.sh graph500_23 ${is_raw} 2>&1 | tee -a logs/bfs_graph500_23_${type}.log
bash bin/graph/bfs_run.sh graph500_25 ${is_raw} 2>&1 | tee -a logs/bfs_graph500_25_${type}.log

# cc
bash bin/graph/cc_run.sh graph500_25 ${is_raw} 2>&1 | tee -a logs/cc_graph500_25_${type}.log
bash bin/graph/cc_run.sh graph500_26 ${is_raw} 2>&1 | tee -a logs/cc_graph500_26_${type}.log
bash bin/graph/cc_run.sh liveJournal ${is_raw} 2>&1 | tee -a logs/cc_liveJournal_${type}.log

# cd
bash bin/graph/cd_run.sh simulate1 2>&1 | tee -a logs/cd_simulate1.log
bash bin/graph/cd_run.sh simulate2 2>&1 | tee -a logs/cd_simulate2.log
bash bin/graph/cd_run.sh usaRoad 2>&1 | tee -a logs/cd_usaRoad.log

# closeness
bash bin/graph/closeness_run.sh cit_patents weighted ${is_check} 2>&1 | tee -a logs/closeness_cit_patents_weighted_${type}.log
bash bin/graph/closeness_run.sh uk_2002 weighted ${is_check} 2>&1 | tee -a logs/closeness_uk_2002_weighted_${type}.log

bash bin/graph/closeness_run.sh cit_patents unweighted ${is_check} 2>&1 | tee -a logs/closeness_cit_patents_unweighted_${type}.log
bash bin/graph/closeness_run.sh uk_2002 unweighted ${is_check} 2>&1 | tee -a logs/closeness_uk_2002_unweighted_${type}.log

# clusteringcoefficient
bash bin/graph/clusteringcoefficient_run.sh cit_patents lcc weighted no 2>&1 | tee -a logs/clusteringcoefficient_cit_patents_lcc_weighted_${type}.log
bash bin/graph/clusteringcoefficient_run.sh uk_2002 lcc weighted no 2>&1 | tee -a logs/clusteringcoefficient_uk_2002_lcc_weighted_${type}.log
bash bin/graph/clusteringcoefficient_run.sh arabic_2005 lcc weighted no 2>&1 | tee -a logs/clusteringcoefficient_arabic_2005_lcc_weighted_${type}.log
bash bin/graph/clusteringcoefficient_run.sh cit_patents lcc unweighted no 2>&1 | tee -a logs/clusteringcoefficient_cit_patents_lcc_unweighted_${type}.log
bash bin/graph/clusteringcoefficient_run.sh uk_2002 lcc unweighted no 2>&1 | tee -a logs/clusteringcoefficient_uk_2002_lcc_unweighted_${type}.log
bash bin/graph/clusteringcoefficient_run.sh arabic_2005 lcc unweighted no 2>&1 | tee -a logs/clusteringcoefficient_arabic_2005_lcc_unweighted_${type}.log

bash bin/graph/clusteringcoefficient_run.sh graph500_22 lcc weighted no 2>&1 | tee -a logs/clusteringcoefficient_graph500_22_lcc_weighted_${type}.log
bash bin/graph/clusteringcoefficient_run.sh graph500_23 lcc weighted no 2>&1 | tee -a logs/clusteringcoefficient_graph500_23_lcc_weighted_${type}.log
bash bin/graph/clusteringcoefficient_run.sh graph500_24 lcc weighted no 2>&1 | tee -a logs/clusteringcoefficient_graph500_24_lcc_weighted_${type}.log
bash bin/graph/clusteringcoefficient_run.sh graph500_25 lcc weighted no 2>&1 | tee -a logs/clusteringcoefficient_graph500_25_lcc_weighted_${type}.log
bash bin/graph/clusteringcoefficient_run.sh graph500_22 lcc unweighted ${is_raw} 2>&1 | tee -a logs/clusteringcoefficient_graph500_22_lcc_unweighted_${type}.log
bash bin/graph/clusteringcoefficient_run.sh graph500_23 lcc unweighted ${is_raw} 2>&1 | tee -a logs/clusteringcoefficient_graph500_23_lcc_unweighted_${type}.log
bash bin/graph/clusteringcoefficient_run.sh graph500_24 lcc unweighted no 2>&1 | tee -a logs/clusteringcoefficient_graph500_24_lcc_unweighted_${type}.log
bash bin/graph/clusteringcoefficient_run.sh graph500_25 lcc unweighted no 2>&1 | tee -a logs/clusteringcoefficient_graph500_25_lcc_unweighted_${type}.log

bash bin/graph/clusteringcoefficient_run.sh graph500_22 avgcc unweighted ${is_raw} 2>&1 | tee -a logs/clusteringcoefficient_graph500_22_avgcc_unweighted_${type}.log
bash bin/graph/clusteringcoefficient_run.sh graph500_23 avgcc unweighted ${is_raw} 2>&1 | tee -a logs/clusteringcoefficient_graph500_23_avgcc_unweighted_${type}.log
bash bin/graph/clusteringcoefficient_run.sh graph500_24 avgcc unweighted no 2>&1 | tee -a logs/clusteringcoefficient_graph500_24_avgcc_unweighted_${type}.log
bash bin/graph/clusteringcoefficient_run.sh graph500_25 avgcc unweighted no 2>&1 | tee -a logs/clusteringcoefficient_graph500_25_avgcc_unweighted_${type}.log

bash bin/graph/clusteringcoefficient_run.sh graph500_22 globalcc unweighted ${is_raw} 2>&1 | tee -a logs/clusteringcoefficient_graph500_22_globalcc_unweighted_${type}.log
bash bin/graph/clusteringcoefficient_run.sh graph500_23 globalcc unweighted ${is_raw} 2>&1 | tee -a logs/clusteringcoefficient_graph500_23_globalcc_unweighted_${type}.log
bash bin/graph/clusteringcoefficient_run.sh graph500_24 globalcc unweighted no 2>&1 | tee -a logs/clusteringcoefficient_graph500_24_globalcc_unweighted_${type}.log
bash bin/graph/clusteringcoefficient_run.sh graph500_25 globalcc unweighted no 2>&1 | tee -a logs/clusteringcoefficient_graph500_25_globalcc_unweighted_${type}.log

# degree
bash bin/graph/degree_run.sh mycielskian20 degrees ${is_raw} 2>&1 | tee -a logs/degree_mycielskian20_degrees_${type}.log
bash bin/graph/degree_run.sh gap_kron degrees ${is_raw} 2>&1 | tee -a logs/degree_gap_kron_degrees_${type}.log
bash bin/graph/degree_run.sh com_friendster degrees ${is_raw} 2>&1 | tee -a logs/degree_com_friendster_degrees_${type}.log

bash bin/graph/degree_run.sh it_2004 inDegrees ${is_raw} 2>&1 | tee -a logs/degree_it_2004_inDegrees_${type}.log
bash bin/graph/degree_run.sh twitter7 inDegrees ${is_raw} 2>&1 | tee -a logs/degree_twitter7_inDegrees_${type}.log
bash bin/graph/degree_run.sh uk_2007_05 inDegrees ${is_raw} 2>&1 | tee -a logs/degree_uk_2007_05_inDegrees_${type}.log

bash bin/graph/degree_run.sh it_2004 outDegrees ${is_raw} 2>&1 | tee -a logs/degree_it_2004_outDegrees_${type}.log
bash bin/graph/degree_run.sh twitter7 outDegrees ${is_raw} 2>&1 | tee -a logs/degree_twitter7_outDegrees_${type}.log
bash bin/graph/degree_run.sh uk_2007_05 outDegrees ${is_raw} 2>&1 | tee -a logs/degree_uk_2007_05_outDegrees_${type}.log

# incpr
bash bin/graph/incpr_run.sh twitter_2010 0.001 1 ${is_raw} 2>&1 | tee -a logs/incpr_twitter_2010_0.001_1_${type}.log
bash bin/graph/incpr_run.sh twitter_2010 0.001 2 ${is_raw} 2>&1 | tee -a logs/incpr_twitter_2010_0.001_2_${type}.log
bash bin/graph/incpr_run.sh twitter_2010 0.001 3 ${is_raw} 2>&1 | tee -a logs/incpr_twitter_2010_0.001_3_${type}.log
bash bin/graph/incpr_run.sh twitter_2010 0.001 4 ${is_raw} 2>&1 | tee -a logs/incpr_twitter_2010_0.001_4_${type}.log
bash bin/graph/incpr_run.sh twitter_2010 0.001 5 ${is_raw} 2>&1 | tee -a logs/incpr_twitter_2010_0.001_5_${type}.log

bash bin/graph/incpr_run.sh twitter_2010 0.01 1 ${is_raw} 2>&1 | tee -a logs/incpr_twitter_2010_0.01_1_${type}.log
bash bin/graph/incpr_run.sh twitter_2010 0.01 2 ${is_raw} 2>&1 | tee -a logs/incpr_twitter_2010_0.01_2_${type}.log
bash bin/graph/incpr_run.sh twitter_2010 0.01 3 ${is_raw} 2>&1 | tee -a logs/incpr_twitter_2010_0.01_3_${type}.log
bash bin/graph/incpr_run.sh twitter_2010 0.01 4 ${is_raw} 2>&1 | tee -a logs/incpr_twitter_2010_0.01_4_${type}.log
bash bin/graph/incpr_run.sh twitter_2010 0.01 5 ${is_raw} 2>&1 | tee -a logs/incpr_twitter_2010_0.01_5_${type}.log

bash bin/graph/incpr_run.sh twitter_2010 0.05 1 ${is_raw} 2>&1 | tee -a logs/incpr_twitter_2010_0.05_1_${type}.log
bash bin/graph/incpr_run.sh twitter_2010 0.05 2 ${is_raw} 2>&1 | tee -a logs/incpr_twitter_2010_0.05_2_${type}.log
bash bin/graph/incpr_run.sh twitter_2010 0.05 3 ${is_raw} 2>&1 | tee -a logs/incpr_twitter_2010_0.05_3_${type}.log
bash bin/graph/incpr_run.sh twitter_2010 0.05 4 ${is_raw} 2>&1 | tee -a logs/incpr_twitter_2010_0.05_4_${type}.log
bash bin/graph/incpr_run.sh twitter_2010 0.05 5 ${is_raw} 2>&1 | tee -a logs/incpr_twitter_2010_0.05_5_${type}.log

# kcore
bash bin/graph/kcore_run.sh graph500_22 ${is_raw} 2>&1 | tee -a logs/kcore_graph500_22_${type}.log
bash bin/graph/kcore_run.sh graph500_23 ${is_raw} 2>&1 | tee -a logs/kcore_graph500_23_${type}.log
bash bin/graph/kcore_run.sh graph500_25 ${is_raw} 2>&1 | tee -a logs/kcore_graph500_25_${type}.log
bash bin/graph/kcore_run.sh graph500_26 ${is_raw} 2>&1 | tee -a logs/kcore_graph500_26_${type}.log

# louvain
bash bin/graph/louvain_run.sh graph500_22 ${is_raw} 2>&1 | tee -a logs/louvain_graph500_22_${type}.log
bash bin/graph/louvain_run.sh graph500_24 ${is_raw} 2>&1 | tee -a logs/louvain_graph500_24_${type}.log
bash bin/graph/louvain_run.sh graph500_25 ${is_raw} 2>&1 | tee -a logs/louvain_graph500_25_${type}.log

bash bin/graph/louvain_run.sh cit_patents no 2>&1 | tee -a logs/louvain_cit_patents_${type}.log
bash bin/graph/louvain_run.sh uk_2002 no 2>&1 | tee -a logs/louvain_uk_2002_${type}.log
bash bin/graph/louvain_run.sh arabic_2005 no 2>&1 | tee -a logs/louvain_arabic_2005_${type}.log

# lpa
bash bin/graph/lpa_run.sh graph500_22 runConvergence no 2>&1 | tee -a logs/lpa_graph500_22_runConvergence_${type}.log
bash bin/graph/lpa_run.sh graph500_24 runConvergence no 2>&1 | tee -a logs/lpa_graph500_24_runConvergence_${type}.log
bash bin/graph/lpa_run.sh graph500_25 runConvergence no 2>&1 | tee -a logs/lpa_graph500_25_runConvergence_${type}.log

bash bin/graph/lpa_run.sh graph500_22 run ${is_raw} 2>&1 | tee -a logs/lpa_graph500_22_run_${type}.log
bash bin/graph/lpa_run.sh graph500_24 run ${is_raw} 2>&1 | tee -a logs/lpa_graph500_24_run_${type}.log
bash bin/graph/lpa_run.sh graph500_25 run ${is_raw} 2>&1 | tee -a logs/lpa_graph500_25_run_${type}.log

# mce
bash bin/graph/mce_run.sh graph500_23 2>&1 | tee -a logs/mce_graph500_23_${type}.log
bash bin/graph/mce_run.sh graph500_24 2>&1 | tee -a logs/mce_graph500_24_${type}.log
bash bin/graph/mce_run.sh graph500_25 2>&1 | tee -a logs/mce_graph500_25_${type}.log

# modularity
bash bin/graph/modularity_run.sh graph500_23 ${is_raw} 2>&1 | tee -a logs/modularity_graph500_23_${type}.log
bash bin/graph/modularity_run.sh graph500_25 ${is_raw} 2>&1 | tee -a logs/modularity_graph500_25_${type}.log
bash bin/graph/modularity_run.sh graph500_26 ${is_raw} 2>&1 | tee -a logs/modularity_graph500_26_${type}.log

bash bin/graph/modularity_run.sh uk_2002 no 2>&1 | tee -a logs/modularity_uk_${type}.log
bash bin/graph/modularity_run.sh arabic_2005 no 2>&1 | tee -a logs/modularity_arabic_${type}.log
bash bin/graph/modularity_run.sh twitter no 2>&1 | tee -a logs/modularity_twitter_${type}.log

# mssp
bash bin/graph/mssp_run.sh soc_liveJournal 5 ${is_raw} 2>&1 | tee -a logs/mssp_liveJournal_5_${type}.log
bash bin/graph/mssp_run.sh uk_2002 5 ${is_raw} 2>&1 | tee -a logs/mssp_uk_2002_5_${type}.log
bash bin/graph/mssp_run.sh arabic_2005 5 ${is_raw} 2>&1 | tee -a logs/mssp_arabic_2005_5_${type}.log

bash bin/graph/mssp_run.sh soc_liveJournal 50 ${is_raw} 2>&1 | tee -a logs/mssp_liveJournal_50_${type}.log
bash bin/graph/mssp_run.sh uk_2002 50 ${is_raw} 2>&1 | tee -a logs/mssp_uk_2002_50_${type}.log
bash bin/graph/mssp_run.sh arabic_2005 50 ${is_raw} 2>&1 | tee -a logs/mssp_arabic_2005_50_${type}.log

# node2vec
bash bin/graph/node2vec_run.sh cit_patents ${is_raw} ${is_check} 2>&1 | tee -a logs/node2vec_cit_patents_${type}.log
bash bin/graph/node2vec_run.sh soc_liveJournal no ${is_check} 2>&1 | tee -a logs/node2vec_soc_liveJournal_${type}.log
bash bin/graph/node2vec_run.sh uk_2002 no ${is_check} 2>&1 | tee -a logs/node2vec_uk_2002_${type}.log

# ppr
bash bin/graph/ppr_run.sh cit_patents fixMS ${is_raw} 1   2>&1 | tee -a logs/ppr_cit_patents_fixMS_1_${type}.log
bash bin/graph/ppr_run.sh cit_patents fixMS ${is_raw} 5   2>&1 | tee -a logs/ppr_cit_patents_fixMS_5_${type}.log
bash bin/graph/ppr_run.sh cit_patents fixMS ${is_raw} 10  2>&1 | tee -a logs/ppr_cit_patents_fixMS_10_${type}.log
bash bin/graph/ppr_run.sh cit_patents fixMS ${is_raw} 50  2>&1 | tee -a logs/ppr_cit_patents_fixMS_50_${type}.log
bash bin/graph/ppr_run.sh cit_patents fixMS ${is_raw} 100 2>&1 | tee -a logs/ppr_cit_patents_fixMS_100_${type}.log

bash bin/graph/ppr_run.sh uk_2002 fixMS ${is_raw} 1   2>&1 | tee -a logs/ppr_uk_2002_fixMS_1_${type}.log
bash bin/graph/ppr_run.sh uk_2002 fixMS ${is_raw} 5   2>&1 | tee -a logs/ppr_uk_2002_fixMS_5_${type}.log
bash bin/graph/ppr_run.sh uk_2002 fixMS ${is_raw} 10  2>&1 | tee -a logs/ppr_uk_2002_fixMS_10_${type}.log
bash bin/graph/ppr_run.sh uk_2002 fixMS ${is_raw} 50  2>&1 | tee -a logs/ppr_uk_2002_fixMS_50_${type}.log
bash bin/graph/ppr_run.sh uk_2002 fixMS ${is_raw} 100 2>&1 | tee -a logs/ppr_uk_2002_fixMS_100_${type}.log

bash bin/graph/ppr_run.sh arabic_2005 fixMS ${is_raw} 1   2>&1 | tee -a logs/ppr_arabic_2005_fixMS_1_${type}.log
bash bin/graph/ppr_run.sh arabic_2005 fixMS ${is_raw} 5   2>&1 | tee -a logs/ppr_arabic_2005_fixMS_5_${type}.log
bash bin/graph/ppr_run.sh arabic_2005 fixMS ${is_raw} 10  2>&1 | tee -a logs/ppr_arabic_2005_fixMS_10_${type}.log
bash bin/graph/ppr_run.sh arabic_2005 fixMS ${is_raw} 50  2>&1 | tee -a logs/ppr_arabic_2005_fixMS_50_${type}.log
bash bin/graph/ppr_run.sh arabic_2005 fixMS ${is_raw} 100 2>&1 | tee -a logs/ppr_arabic_2005_fixMS_100_${type}.log

bash bin/graph/ppr_run.sh cit_patents fixSS ${is_raw} 2>&1 | tee -a logs/ppr_cit_patents_fixSS_${type}.log
bash bin/graph/ppr_run.sh uk_2002 fixSS ${is_raw} 2>&1 | tee -a logs/ppr_uk_2002_fixSS_${type}.log
bash bin/graph/ppr_run.sh arabic_2005 fixSS ${is_raw} 2>&1 | tee -a logs/ppr_arabic_2005_fixSS_${type}.log

bash bin/graph/ppr_run.sh cit_patents conSS ${is_raw} 2>&1 | tee -a logs/ppr_cit_patents_conSS_${type}.log
bash bin/graph/ppr_run.sh uk_2002 conSS ${is_raw} 2>&1 | tee -a logs/ppr_uk_2002_conSS_${type}.log
bash bin/graph/ppr_run.sh arabic_2005 conSS ${is_raw} 2>&1 | tee -a logs/ppr_arabic_2005_conSS_${type}.log

# pr
bash bin/graph/pr_run.sh cit_patents run ${is_raw} 2>&1 | tee -a logs/pr_cit_patents_run_${type}.log
bash bin/graph/pr_run.sh uk_2002 run ${is_raw} 2>&1 | tee -a logs/pr_uk_2002_run_${type}.log
bash bin/graph/pr_run.sh arabic_2005 run ${is_raw} 2>&1 | tee -a logs/pr_arabic_2005_run_${type}.log

bash bin/graph/pr_run.sh cit_patents runUntilConvergence ${is_raw} 2>&1 | tee -a logs/pr_cit_patents_runUntilConvergence_${type}.log
bash bin/graph/pr_run.sh uk_2002 runUntilConvergence ${is_raw} 2>&1 | tee -a logs/pr_uk_2002_runUntilConvergence_${type}.log
bash bin/graph/pr_run.sh arabic_2005 runUntilConvergence ${is_raw} 2>&1 | tee -a logs/pr_arabic_2005_runUntilConvergence_${type}.log

# scc
bash bin/graph/scc_run.sh cit_patents ${is_raw} 2>&1 | tee -a logs/scc_cit_patents_${type}.log
bash bin/graph/scc_run.sh enwiki_2018 ${is_raw} 2>&1 | tee -a logs/scc_enwiki_2018_${type}.log
bash bin/graph/scc_run.sh arabic_2005 ${is_raw} 2>&1 | tee -a logs/scc_arabic_2005_${type}.log

# sgm
bash bin/graph/sgm_run.sh graph500_19 4dgn Identical no 2>&1 | tee -a logs/sgm_graph500_19_4dgn_Identical_${type}.log
bash bin/graph/sgm_run.sh graph500_19 4sqr Identical no 2>&1 | tee -a logs/sgm_graph500_19_4sqr_Identical_${type}.log
bash bin/graph/sgm_run.sh graph500_19 5tree Identical no 2>&1 | tee -a logs/sgm_graph500_19_5tree_Identical_${type}.log
bash bin/graph/sgm_run.sh graph500_19 6star Identical no 2>&1 | tee -a logs/sgm_graph500_19_6star_Identical_${type}.log

bash bin/graph/sgm_run.sh liveJournal 4dgn Identical no 2>&1 | tee -a logs/sgm_liveJournal_4dgn_Identical_${type}.log
bash bin/graph/sgm_run.sh liveJournal 4sqr Identical no 2>&1 | tee -a logs/sgm_liveJournal_4sqr_Identical_${type}.log
bash bin/graph/sgm_run.sh liveJournal 5tree Identical no 2>&1 | tee -a logs/sgm_liveJournal_5tree_Identical_${type}.log
bash bin/graph/sgm_run.sh liveJournal 6star Identical no 2>&1 | tee -a logs/sgm_liveJournal_6star_Identical_${type}.log

bash bin/graph/sgm_run.sh com_orkut 4dgn Identical no 2>&1 | tee -a logs/sgm_com_orkut_4dgn_Identical_${type}.log
bash bin/graph/sgm_run.sh com_orkut 4sqr Identical no 2>&1 | tee -a logs/sgm_com_orkut_4sqr_Identical_${type}.log
bash bin/graph/sgm_run.sh com_orkut 5tree Identical no 2>&1 | tee -a logs/sgm_com_orkut_5tree_Identical_${type}.log
bash bin/graph/sgm_run.sh com_orkut 6star Identical no 2>&1 | tee -a logs/sgm_com_orkut_6star_Identical_${type}.log

bash bin/graph/sgm_run.sh graph500_19 4dgn unIdentical ${is_raw} 2>&1 | tee -a logs/sgm_graph500_19_unIdentical_4dgn_${type}.log
bash bin/graph/sgm_run.sh graph500_19 4clique unIdentical ${is_raw} 2>&1 | tee -a logs/sgm_graph500_19_unIdentical_4clique_${type}.log
bash bin/graph/sgm_run.sh graph500_19 5clique unIdentical ${is_raw} 2>&1 | tee -a logs/sgm_graph500_19_unIdentical_5clique_${type}.log
bash bin/graph/sgm_run.sh graph500_19 6clique unIdentical ${is_raw} 2>&1 | tee -a logs/sgm_graph500_19_unIdentical_6clique_${type}.log

bash bin/graph/sgm_run.sh liveJournal 4dgn unIdentical ${is_raw} 2>&1 | tee -a logs/sgm_liveJournal_unIdentical_4dgn_${type}.log
bash bin/graph/sgm_run.sh liveJournal 4clique unIdentical ${is_raw} 2>&1 | tee -a logs/sgm_liveJournal_unIdentical_4clique_${type}.log
bash bin/graph/sgm_run.sh liveJournal 5clique unIdentical ${is_raw} 2>&1 | tee -a logs/sgm_liveJournal_unIdentical_5clique_${type}.log
bash bin/graph/sgm_run.sh liveJournal 6clique unIdentical ${is_raw} 2>&1 | tee -a logs/sgm_liveJournal_unIdentical_6clique_${type}.log

bash bin/graph/sgm_run.sh com_orkut 4dgn unIdentical ${is_raw} 2>&1 | tee -a logs/sgm_com_orkut_unIdentical_4dgn_${type}.log
bash bin/graph/sgm_run.sh com_orkut 4clique unIdentical ${is_raw} 2>&1 | tee -a logs/sgm_com_orkut_unIdentical_4clique_${type}.log
bash bin/graph/sgm_run.sh com_orkut 5clique unIdentical ${is_raw} 2>&1 | tee -a logs/sgm_com_orkut_unIdentical_5clique_${type}.log
bash bin/graph/sgm_run.sh com_orkut 6clique unIdentical ${is_raw} 2>&1 | tee -a logs/sgm_com_orkut_unIdentical_6clique_${type}.log

# tc
bash bin/graph/tc_run.sh graph500_22 run ${is_raw} 2>&1 | tee -a logs/tc_graph500_22_run_${type}.log
bash bin/graph/tc_run.sh graph500_23 run ${is_raw} 2>&1 | tee -a logs/tc_graph500_23_run_${type}.log
bash bin/graph/tc_run.sh graph500_24 run ${is_raw} 2>&1 | tee -a logs/tc_graph500_24_run_${type}.log
bash bin/graph/tc_run.sh graph500_25 run ${is_raw} 2>&1 | tee -a logs/tc_graph500_25_run_${type}.log
bash bin/graph/tc_run.sh graph500_26 run ${is_raw} 2>&1 | tee -a logs/tc_graph500_26_run_${type}.log

bash bin/graph/tc_run.sh graph500_22 preCanonical ${is_raw} 2>&1 | tee -a logs/tc_graph500_22_preCanonical_${type}.log
bash bin/graph/tc_run.sh graph500_23 preCanonical ${is_raw} 2>&1 | tee -a logs/tc_graph500_23_preCanonical_${type}.log
bash bin/graph/tc_run.sh graph500_24 preCanonical ${is_raw} 2>&1 | tee -a logs/tc_graph500_24_preCanonical_${type}.log
bash bin/graph/tc_run.sh graph500_25 preCanonical ${is_raw} 2>&1 | tee -a logs/tc_graph500_25_preCanonical_${type}.log
bash bin/graph/tc_run.sh graph500_26 preCanonical ${is_raw} 2>&1 | tee -a logs/tc_graph500_26_preCanonical_${type}.log

# tpr
bash bin/graph/tpr_run.sh twitter_tpr ${is_raw} 2>&1 | tee -a logs/tpr_twitter_${type}.log

# tr
bash bin/graph/tr_run.sh cit_patents run 100  2>&1 | tee -a logs/tr_cit_patents_run_100_${type}.log
bash bin/graph/tr_run.sh cit_patents run 500  2>&1 | tee -a logs/tr_cit_patents_run_500_${type}.log
bash bin/graph/tr_run.sh cit_patents run 1000 2>&1 | tee -a logs/tr_cit_patents_run_1000_${type}.log
bash bin/graph/tr_run.sh uk_2002 run 100  2>&1 | tee -a logs/tr_uk_2002_run_100_${type}.log
bash bin/graph/tr_run.sh uk_2002 run 500  2>&1 | tee -a logs/tr_uk_2002_run_500_${type}.log
bash bin/graph/tr_run.sh uk_2002 run 1000 2>&1 | tee -a logs/tr_uk_2002_run_1000_${type}.log
bash bin/graph/tr_run.sh arabic_2005 run 100  2>&1 | tee -a logs/tr_arabic_2005_run_100_${type}.log
bash bin/graph/tr_run.sh arabic_2005 run 500  2>&1 | tee -a logs/tr_arabic_2005_run_500_${type}.log
bash bin/graph/tr_run.sh arabic_2005 run 1000 2>&1 | tee -a logs/tr_arabic_2005_run_1000_${type}.log

bash bin/graph/tr_run.sh cit_patents runUntilConvergence 100  2>&1 | tee -a logs/tr_cit_patents_runUntilConvergence_100_${type}.log
bash bin/graph/tr_run.sh cit_patents runUntilConvergence 500  2>&1 | tee -a logs/tr_cit_patents_runUntilConvergence_500_${type}.log
bash bin/graph/tr_run.sh cit_patents runUntilConvergence 1000 2>&1 | tee -a logs/tr_cit_patents_runUntilConvergence_1000_${type}.log
bash bin/graph/tr_run.sh uk_2002 runUntilConvergence 100  2>&1 | tee -a logs/tr_uk_2002_runUntilConvergence_100_${type}.log
bash bin/graph/tr_run.sh uk_2002 runUntilConvergence 500  2>&1 | tee -a logs/tr_uk_2002_runUntilConvergence_500_${type}.log
bash bin/graph/tr_run.sh uk_2002 runUntilConvergence 1000 2>&1 | tee -a logs/tr_uk_2002_runUntilConvergence_1000_${type}.log
bash bin/graph/tr_run.sh arabic_2005 runUntilConvergence 100  2>&1 | tee -a logs/tr_arabic_2005_runUntilConvergence_100_${type}.log
bash bin/graph/tr_run.sh arabic_2005 runUntilConvergence 500  2>&1 | tee -a logs/tr_arabic_2005_runUntilConvergence_500_${type}.log
bash bin/graph/tr_run.sh arabic_2005 runUntilConvergence 1000 2>&1 | tee -a logs/tr_arabic_2005_runUntilConvergence_1000_${type}.log

# wce
bash bin/graph/wce_run.sh graph500_24 2>&1 | tee -a logs/wce_graph500_24_${type}.log
bash bin/graph/wce_run.sh graph500_25 2>&1 | tee -a logs/wce_graph500_25_${type}.log
bash bin/graph/wce_run.sh graph500_26 2>&1 | tee -a logs/wce_graph500_26_${type}.log

# wpr
bash bin/graph/wpr_run.sh cage14 static ${is_raw} 2>&1 | tee -a logs/wpr_cage14_static_${type}.log
bash bin/graph/wpr_run.sh GAP_road static ${is_raw} 2>&1 | tee -a logs/wpr_GAP_road_static_${type}.log
bash bin/graph/wpr_run.sh GAP_twitter static ${is_raw} 2>&1 | tee -a logs/wpr_GAP_twitter_static_${type}.log

bash bin/graph/wpr_run.sh cage14 convergence ${is_raw} 2>&1 | tee -a logs/wpr_cage14_convergence_${type}.log
bash bin/graph/wpr_run.sh GAP_road convergence ${is_raw} 2>&1 | tee -a logs/wpr_GAP_road_convergence_${type}.log
bash bin/graph/wpr_run.sh GAP_twitter convergence ${is_raw} 2>&1 | tee -a logs/wpr_GAP_twitter_convergence_${type}.log
