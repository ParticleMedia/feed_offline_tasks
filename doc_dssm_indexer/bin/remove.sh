#!/bin/bash

hosts=`curl -s "http://haproxy:haproxy@simdoc-annoy.ha.nb.com:19300/admin?stats;csv;norefresh" | grep "simdoc-annoy,172" | awk -F, '{print $2}' | awk -F':' '{print $1}'`
hosts="${hosts} `curl -s "http://haproxy:haproxy@push-simdoc-annoy.ha.nb.com:19300/admin?stats;csv;norefresh" | grep "push-simdoc-annoy,172" | awk -F, '{print $2}' | awk -F':' '{print $1}'`"
pem_file=~/services.pem
DEST_DATA_DIR=/home/services/docker_home/_data/simdoc-annoy

function remove_index() {
    local index=$1
    local host=$2
    ssh -i ${pem_file} -o "StrictHostKeyChecking no" services@${host} "cd ${DEST_DATA_DIR} ; rm -rf ${index} ; rm -rf ${index}.spec"
}

index_name=$1

for host in ${hosts[@]}; do
    remove_index ${index_name} ${host}
done

