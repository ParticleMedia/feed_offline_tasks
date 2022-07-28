#!/bin/bash
set -x

function check_env()
{
    if [ -z $HADOOP_BIN ]; then
        echo "conf: HADOOP_BIN is needed!"
        return 1
    fi
    if [ -z $HDFS_WORK_PATH ]; then
        echo "conf: HDFS_WORK_PATH is needed!"
        return 1
    fi
    if [ -z $HDFS_TMP_PATH ]; then
        echo "conf: HDFS_TMP_PATH is needed!"
        return 1
    fi
    if [ -z $LOCAL_BIN_PATH ]; then
        echo "conf: LOCAL_BIN_PATH is needed!"
        return 1
    fi

    return 0
}

function run_mapred() {
    bash ${LOCAL_BIN_PATH}/run_mapred.sh $@
    return $?
}

function run_distcp() {
    bash ${LOCAL_BIN_PATH}/run_distcp.sh $@
    return $?
}

function output_of() {
    local mapred_conf=$1
    (source ${mapred_conf} && echo ${MAPRED_OUTPUT_PATH})
}

function watch_hdfs_file() {
    local path=$1
    local checktimes=$2

    for ((i=0; i<${checktimes}; i++)); do
        ${HADOOP_BIN} dfs -test -f ${path}
        if [ $? -eq 0 ]; then
            return 0
        fi
        sleep 5m
    done
    return 1
}

function write_to_ups() {
    local data_file=${LOCAL_DATA_PATH}/cluster/${DATE_FLAG}/part-*
    cat ${data_file} | ${LOCAL_BIN_PATH}/write_profile --host=${UPS_HOST} --port=${UPS_PORT} --profile=${UPS_PROFILE} --version=${UPS_VERSION} --batch=${UPS_BATCH} --format=${UPS_FORMAT}
}

function build_annoy() {
    # build annoy index
    local user_embedding_conf=${LOCAL_CONF_PATH}/user_embedding.conf
    local hdfs_dir=`output_of ${user_embedding_conf}`
    local index_dir=${LOCAL_DATA_PATH}/index/${DATE_FLAG}
    local tmp_dir=${LOCAL_DATA_PATH}/user_embedding/${DATE_FLAG}
    if [ "${tmp_dir}" != "/" ]; then
        rm -rf ${tmp_dir}
    fi    
    mkdir -p ${tmp_dir}
    ${HADOOP_BIN} dfs -copyToLocal ${hdfs_dir}/part-* ${tmp_dir}
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    #cat ${tmp_dir}/part-* | python ${LOCAL_BIN_PATH}/build_annoy_index.py ${index_dir}/users.ann
    cat ${tmp_dir}/part-* | ${LOCAL_BIN_PATH}/indexer --dimension=50 --tree=40 --output=${index_dir}/users.ann
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi
    return ${ret}
}

function process() {
    local module_conf=$1
    local ret=0

    # write your own logic here
    local process_cjv_conf=${LOCAL_CONF_PATH}/process_cjv.conf
    run_mapred ${module_conf} ${process_cjv_conf}
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi

    local cluster_conf=${LOCAL_CONF_PATH}/cluster.conf
    run_mapred ${module_conf} ${cluster_conf}
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    if [ "x${WRITE_TO_UPS}" == "xTRUE" ]; then
        write_to_ups
        echo "write_to_ups" >&2
        ret=$?
    fi

    return ${ret}
}

if [ $# -lt 1 ]; then
    echo "usage: "$0" MODULE_CONF [RUN_DATE]"
    exit 1
fi
module_conf_file=$1
RUN_DATE=$2
source $module_conf_file

# date flag
if [ -z "${DATE_FLAG}" ]; then
    DATE_FLAG=`date +"%Y%m%d"`
fi

if [ -n "${RUN_DATE}" ]; then
    DATE_FLAG=${RUN_DATE}
fi

check_env 1>&2
if [ $? -ne 0 ]; then
    exit 1
fi

timestamp=`date +%Y%m%d%H%M%S`

process ${module_conf_file}
ret=$?
if [ ${ret} -ne 0 ]; then
    echo "process failed. ret[${ret}]" 1>&2
    exit ${ret}
fi

if [ -n "${LOG_CLEANUP_DATE}" ]; then
    if [ -n "${LOCAL_LOG_PATH}" ]; then
        rm -f ${LOCAL_LOG_PATH}/*.log.${LOG_CLEANUP_DATE}* &>/dev/null
    fi
    if [ -n "${LOCAL_DATA_PATH}" ]; then
        rm -rf ${LOCAL_DATA_PATH}/cluster/${LOG_CLEANUP_DATE} &>/dev/null
    fi
fi
exit ${ret}
