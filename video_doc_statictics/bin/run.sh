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

function process() {
    local module_conf=$1
    local ret=0

    local doc_hdfs_path=${DOC_HDFS_ROOT}/pdate=${DOC_DATE_FLAG}
    ${HDFS_BIN} dfs -test -d ${doc_hdfs_path}/phour=23
    if [ $? -ne 0 ]; then
        return 1
    fi

    local output_file=${LOCAL_DATA_PATH}/statistics_${DATE_FLAG}
    mkdir -p ${LOCAL_DATA_PATH}
    rm -f ${output_file}
    ${HDFS_BIN} dfs -text ${doc_hdfs_path}/phour=*/* | ${LOCAL_BIN_PATH}/doc_city_statistics -pdate=${DATE_FLAG} -write_mysql=${WRITE_MYSQL} -logtostderr=true 1>${output_file}
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    return ${ret}
}

function cleanup() {
    find ${LOCAL_DATA_PATH}/ -type f -mtime +${DATA_CLEANUP_DAYS} -exec rm -f {} \; &>/dev/null
    find ${LOCAL_LOG_PATH}/ -type f -mtime +${LOG_CLEANUP_DAYs} -exec rm -f {} \; &>/dev/null
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

cleanup

exit ${ret}
