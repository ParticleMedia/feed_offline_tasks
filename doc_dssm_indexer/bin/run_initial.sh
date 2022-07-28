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

function fetch_embedding() {
    local index_dir=${LOCAL_DATA_PATH}/index/${DATE_FLAG}${HOUR_FLAG}
    local data_file=${index_dir}/merged_data.txt
    cat ${data_file} | ${LOCAL_BIN_PATH}/embeder --output=${index_dir}/embedding.txt
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi
    return ${ret}
}

function build_annoy() {
    # build annoy index
    local index_dir=${LOCAL_DATA_PATH}/index/${DATE_FLAG}${HOUR_FLAG}
    local embedding_file=${index_dir}/embedding.txt
    cat ${embedding_file} | ${LOCAL_BIN_PATH}/indexer --dimension=64 --tree=20 --prefix=${index_dir}/docs
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
    local data_file=${out_dir}/merged_data.txt
    cat ${file_list} | sort | python ${LOCAL_BIN_PATH}/merge_doc.py >${data_file}
}

function process() {
    local module_conf=$1
    local ret=0

    # write your own logic here
    local filter_doc_conf=${LOCAL_CONF_PATH}/filter_doc_initial.conf
    for ((i=1;i<=59;i++)); do
        export PROCESSING_DATE_FLAG=`date +%Y%m%d -d "-$i hours"`
        export PROCESSING_HOUR_FLAG=`date +%H -d "-$i hours"`
        export PROCESSING_DOC_DATE_FLAG=`date +%Y_%m_%d -d "-$i hours"`
        run_mapred ${module_conf} ${filter_doc_conf}
        ret=$?
        if [ ${ret} -ne  0 ]; then
            return ${ret}
        fi
    done

    #select_docs
    #ret=$?
    #if [ ${ret} -ne 0 ]; then
    #    return ${ret}
    #fi

    #fetch_embedding
    #ret=$?
    #if [ ${ret} -ne 0 ]; then
    #    return ${ret}
    #fi

    #build_annoy
    #ret=$?
    #if [ ${ret} -ne 0 ]; then
    #    return ${ret}
    #fi

    #if [ "x${PUSH_INDEX}" == "xTRUE" ]; then
    #    local index_dir=${LOCAL_DATA_PATH}/index/${DATE_FLAG}${HOUR_FLAG}
    #    local push_timestamp=`date +%s`
    #    bash -x ${LOCAL_BIN_PATH}/push.sh dssm ${index_dir}
    #    ret=$?
    #    if [ ${ret} -ne 0 ]; then
    #        return ${ret}
    #    fi
    #    echo "push index" >&2
    #fi

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
