#!/bin/bash

SCRIPT_ROOT=$(cd "$(dirname "$0")"; pwd)
hosts=("172.31.17.126" "172.31.20.216")
pem_file=~/services.pem
DEST_DATA_DIR=/home/services/docker_home/_data/simuser-annoy

function push_index() {
    local index_name=$1
    local host=$2
    local dest_dir=$3

    ssh -i ${pem_file} services@${host} "mkdir -p ${dest_dir}" && 
    scp -i ${pem_file} ${ANN_INDEX_FILE} services@${host}:${dest_dir} &&
    scp -i ${pem_file} ${SCORE_FILE} services@${host}:${dest_dir} &&
    scp -i ${pem_file} ${SPEC_FILE} services@${host}:${DEST_DATA_DIR} && 
    curl -m 60 "http://${host}:9400/reload?from=pipeline&index=${index_name}"
}

# collect index
index_name=$1
index_dir=$2
version=`date +"%Y%m%d%H%M%S"`

dest_dir=${DEST_DATA_DIR}/${index_name}/${version}
ANN_INDEX_FILE=${index_dir}/users.ann
SCORE_FILE=${index_dir}/user_score.txt
SPEC_FILE=${index_dir}/${index_name}.spec

if [ ! -f ${ANN_INDEX_FILE} -o ! -f ${SCORE_FILE} ]; then
    exit 1
fi

${SCRIPT_ROOT}/spec_tool -dir=../data/${index_name}/${version} -scorer=distance -dimension=50 -cache_size=1000000 >${SPEC_FILE}
if [ $? -ne 0 ]; then
    exit 1
fi

for host in ${hosts[@]}; do
    push_index ${index_name} ${host} ${dest_dir}
    sleep 900s
done
