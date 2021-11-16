#!/bin/bash

SCRIPT_ROOT=$(cd "$(dirname "$0")"; pwd)
hosts=("172.31.26.178" "172.31.19.240" "172.31.16.149" "172.31.27.243" "172.31.29.140" "172.31.21.39" "172.31.20.79" "172.31.20.236" "172.31.21.120" "172.31.28.152" "172.31.16.121" "172.31.20.217" "172.31.30.214" "172.31.30.54" "172.31.17.120" "172.31.17.214" "172.31.29.229" "172.31.27.133" "172.31.16.166" "172.31.18.101" "172.31.19.68" "172.31.28.28" "172.31.30.251" "172.31.30.255")
pem_file=~/services.pem
DEST_DATA_DIR=/home/services/docker_home/_data/simdoc-annoy
CACHE_SIZE=0

function push_index() {
    local index=$1
    local host=$2
    local dest_dir=$3

    ssh -i ${pem_file} -o "StrictHostKeyChecking no" services@${host} "mkdir -p ${dest_dir}" &&
    scp -i ${pem_file} -o "StrictHostKeyChecking no" ${ANN_INDEX_FILE} services@${host}:${dest_dir} &&
    scp -i ${pem_file} -o "StrictHostKeyChecking no" ${MAPPING_FILE} services@${host}:${dest_dir} &&
    scp -i ${pem_file} -o "StrictHostKeyChecking no" ${SPEC_FILE} services@${host}:${DEST_DATA_DIR}
    #curl -m 60 "http://${host}:9300/reload?from=pipeline&index=${index}"
}

# collect index
index_name=$1
index_dir=$2
version=`date +"%Y%m%d%H%M%S"`

ANN_INDEX_FILE=${index_dir}/docs.ann
MAPPING_FILE=${index_dir}/docs.map
SPEC_FILE=${index_dir}/${index_name}.spec

if [ ! -f ${ANN_INDEX_FILE} -o ! -f ${MAPPING_FILE} ]; then
    exit 1
fi

dest_dir=${DEST_DATA_DIR}/${index_name}/${version}
${SCRIPT_ROOT}/spec_tool -dir="../data/${index_name}/${version}" -scorer=distance -dimension=50 -cache_size=${CACHE_SIZE} >${SPEC_FILE}
if [ $? -ne 0 ]; then
    exit 1
fi

for host in ${hosts[@]}; do
    push_index ${index_name} ${host} ${dest_dir}
done
