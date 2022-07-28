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
    local data_dir=$1
    ${HDFS_BIN} dfs -test -d ${data_dir}
    if [ $? -ne 0 ]; then
        echo "[${data_dir}] is not a dictionary" >&2
        return 1
    fi

    ${HDFS_BIN} dfs -cat ${data_dir}/part-* | ${LOCAL_BIN_PATH}/write_profile --host=${UPS_HOST} --port=${UPS_PORT} --profile=${UPS_PROFILE} --version=${UPS_VERSION} --batch=${UPS_BATCH} --format=${UPS_FORMAT} -worker=${UPS_WORKER}
    return $?
}

function do_cluster() {
    # copy embedding to local
    local ret=0
    local hdfs_embedding_dir=$1
    local local_embedding_dir=${LOCAL_DATA_PATH}/embedding/${DATE_FLAG}
    mkdir -p ${local_embedding_dir}
    if [ -n "${local_embedding_dir}" ]; then
        rm -rf ${local_embedding_dir}/* &>/dev/null
    fi    
    ${HDFS_BIN} dfs -copyToLocal ${hdfs_embedding_dir}/part-* ${local_embedding_dir}
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    local local_cluster_dir=${LOCAL_DATA_PATH}/cluster/${DATE_FLAG}
    mkdir -p ${local_cluster_dir}
    if [ -n "${local_cluster_dir}" ]; then
        rm -rf ${local_cluster_dir}/* &>/dev/null
    fi
    cat ${local_embedding_dir}/part-* | ${LOCAL_BIN_PATH}/user_cluster -min-total-click=10 -cluster-count=300 -dimension=50 -iteration=300 -try=2 -sample=30 >${local_cluster_dir}/user_cluster_300.txt
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

    local merge_click_conf=${LOCAL_CONF_PATH}/merge_click.conf
    run_mapred ${module_conf} ${merge_click_conf}
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    local merge_embedding_conf=${LOCAL_CONF_PATH}/merge_embedding.conf
    run_mapred ${module_conf} ${merge_embedding_conf}
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    # do cluster
    local hdfs_embedding_dir=`output_of ${merge_embedding_conf}`
    # do_cluster ${hdfs_embedding_dir}
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    local normalize_conf=${LOCAL_CONF_PATH}/normalize.conf
    run_mapred ${module_conf} ${normalize_conf}
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    if [ "x${WRITE_TO_UPS}" == "xTRUE" ]; then
        echo "write_to_ups" >&2
        local hdfs_profile_dir=`output_of ${normalize_conf}`
        write_to_ups ${hdfs_profile_dir}
        ret=$?
        if [ ${ret} -ne 0 ]; then
            return ${ret}
        fi
    fi
    return ${ret}
}

function cleanup() {
    if [ -n "${LOG_CLEANUP_DAY}" ]; then
        if [ -n "${LOCAL_LOG_PATH}" ]; then
            find ${LOCAL_LOG_PATH}/ -type f -mtime +${LOG_CLEANUP_DAY} -exec rm -f {} \; &>/dev/null
        fi
        if [ -n "${LOCAL_DATA_PATH}" ]; then        
            find ${LOCAL_DATA_PATH}/embedding -type d -mtime +${LOG_CLEANUP_DAY} -exec rm -rf {} \; &>/dev/null
            find ${LOCAL_DATA_PATH}/cluster -type d -mtime +${LOG_CLEANUP_DAY} -exec rm -rf {} \; &>/dev/null
        fi

        local cleanup_date=`date -d "${DATE_FLAG} -${LOG_CLEANUP_DAY} days" +%Y%m%d`
        ${HDFS_BIN} dfs -rmr -skipTrash ${HDFS_WORK_PATH}/*/${cleanup_date} &>/dev/null
    fi
}

if [ $# -lt 1 ]; then
    echo "usage: "$0" MODULE_CONF [RUN_DATE]"
    exit 1
fi
module_conf_file=$1
RUN_DATE=$2

# date flag
if [ -n "${RUN_DATE}" ]; then
    export DATE_FLAG=${RUN_DATE}
fi

source $module_conf_file
check_env 1>&2
if [ $? -ne 0 ]; then
    exit 1
fi

timestamp=`date +%Y%m%d%H%M%S`

process ${module_conf_file}
ret=$?

cleanup
exit ${ret}
