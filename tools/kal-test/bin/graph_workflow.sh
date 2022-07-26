#!/bin/bash
set -e

case "$1" in
-h | --help | ?)
  echo "Usage: <isRaw>"
  echo "1st argument: optimization algorithm or raw: no/yes"
  exit 0
  ;;
esac

if [ $# -ne 1 ]; then
  echo "please input 1 arguments: <isRaw>"
  echo "1st argument: optimization algorithm or raw: no/yes"
  exit 0
fi

is_raw=$1
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

graph_classpath=/opt/graph_classpath/
function ssh_mkdir() {
    server=$1
    dir=$2
    ssh $server "mkdir -p $dir"
}
ssh_mkdir agent1 $graph_classpath
ssh_mkdir agent2 $graph_classpath
ssh_mkdir agent3 $graph_classpath

# tpr
./bin/graph/tpr_run.sh twitter_tpr  ${is_raw} 2>&1 | tee -a logs/tpr_twitter_${type}.log
