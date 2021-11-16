#!/bin/bash

SCRIPT_ROOT=$(cd "$(dirname "$0")"; pwd)
hosts=`curl -s "http://haproxy:haproxy@simdoc-annoy.ha.nb.com:19300/admin?stats;csv;norefresh" | grep "simdoc-annoy,172" | awk -F, '{print $2}' | awk -F':' '{print $1}'`
hosts="${hosts} `curl -s "http://haproxy:haproxy@push-simdoc-annoy.ha.nb.com:19300/admin?stats;csv;norefresh" | grep "push-simdoc-annoy,172" | awk -F, '{print $2}' | awk -F':' '{print $1}'`"
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
    scp -i ${pem_file} -o "StrictHostKeyChecking no" ${SPEC_FILE} services@${host}:${DEST_DATA_DIR} &&
    curl -m 60 "http://${host}:9300/reload?from=pipeline&index=${index}"
}

# collect index
index_name=$1
index_dir=$2
version=`date +"%Y%m%d%H%M%S"`

ANN_INDEX_FILE=${index_dir}/docs_exp.ann
MAPPING_FILE=${index_dir}/docs_exp.map
SPEC_FILE=${index_dir}/${index_name}.spec

if [ ! -f ${ANN_INDEX_FILE} -o ! -f ${MAPPING_FILE} ]; then
    exit 1
fi

dest_dir=${DEST_DATA_DIR}/${index_name}/${version}
${SCRIPT_ROOT}/spec_tool -dir="../data/${index_name}/${version}" -ann_file=docs_exp.ann -mapping_file=docs_exp.map -scorer=distance -dimension=32 -cache_size=${CACHE_SIZE} >${SPEC_FILE}
if [ $? -ne 0 ]; then
    exit 1
fi

for host in ${hosts[@]}; do
    push_index ${index_name} ${host} ${dest_dir}
done
