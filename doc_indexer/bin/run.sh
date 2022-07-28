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
        sleep 1m
    done
    return 1
}

function wait_cpp_data(){
    cpp_path=s3a://cpp-us-pmi/cpp_documents/pdate=${DOC_DATE_FLAG}/phour=${HOUR_FLAG}/_SUCCESS
    watch_hdfs_file ${cpp_path} 50
    return $?
}

function build_annoy() {
    # build annoy index
    local index_dir=${LOCAL_DATA_PATH}/index/${DATE_FLAG}${HOUR_FLAG}
    local embedding_file=${index_dir}/embedding
    cat ${embedding_file} | ${LOCAL_BIN_PATH}/indexer --dimension=50 --tree=100 --prefix=${index_dir}/docs
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi
    return ${ret}
}

function select_docs() {
    local select_hours=72
    local doc_dir=${LOCAL_DATA_PATH}/docs
    local out_dir=${LOCAL_DATA_PATH}/index/${DATE_FLAG}${HOUR_FLAG}
    local file_list=""
    for ((i=1;i<=${select_hours};++i)); do
        local datehourflag=`date +%Y%m%d%H -d "-${i} hours"`
        local filepath=${doc_dir}/${datehourflag}
        if [ -d ${filepath} ]; then
            file_list="${file_list} ${filepath}/part-*"
        fi
    done

    mkdir -p ${out_dir}
    if [ -n "${out_dir}" ]; then
        rm -rf ${out_dir}/*
    fi
    local embedding_file=${out_dir}/embedding
    cat ${file_list} | sort | python ${LOCAL_BIN_PATH}/merge_doc.py >${embedding_file}
}

function process() {
    local module_conf=$1
    local ret=0

    # write your own logic here
    #wait_cpp_data
    #ret=$?
    #if [ ${ret} -ne  0 ]; then
    #    return ${ret}
    #fi

    local filter_doc_conf=${LOCAL_CONF_PATH}/filter_doc.conf
    run_mapred ${module_conf} ${filter_doc_conf}
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi

    select_docs
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    build_annoy
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    if [ "x${PUSH_INDEX}" == "xTRUE" ]; then
        local index_dir=${LOCAL_DATA_PATH}/index/${DATE_FLAG}${HOUR_FLAG}
        local push_timestamp=`date +%s`
        bash -x ${LOCAL_BIN_PATH}/push.sh semantic ${index_dir}
        ret=$?
        if [ ${ret} -ne 0 ]; then
            return ${ret}
        fi
        echo "push index" >&2

        # write trace
        cat ${index_dir}/docs.map | cut -f 1 | ${LOCAL_BIN_PATH}/trace_writer -host=172.31.20.243 -port=9750 -event=semantic.index -ts=${push_timestamp} -batch=100 1>/dev/null
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

if [ -n "${LOG_CLEANUP_DATE}" -a -n "${LOG_CLEANUP_HOUR}" ]; then
    if [ -n "${LOCAL_LOG_PATH}" ]; then
        rm -f ${LOCAL_LOG_PATH}/*.log.${LOG_CLEANUP_DATE}${LOG_CLEANUP_HOUR}* &>/dev/null
    fi
    if [ -n "${LOCAL_DATA_PATH}" ]; then
        rm -rf ${LOCAL_DATA_PATH}/index/${LOG_CLEANUP_DATE}${LOG_CLEANUP_HOUR} &>/dev/null
        rm -rf ${LOCAL_DATA_PATH}/docs/${LOG_CLEANUP_DATE}${LOG_CLEANUP_HOUR} &>/dev/null
    fi
    #${HADOOP_BIN} dfs -rmr -skipTrash ${HDFS_WORK_PATH}/*/${LOG_CLEANUP_DATE}/${LOG_CLEANUP_HOUR} &>/dev/null
fi
exit ${ret}
