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

function local_output_of() {
    local mapred_conf=$1
    (source ${mapred_conf} && echo ${COPY_TO_LOCAL})
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

function do_ctr() {
    local module_conf=$1
    local select_days=$2
    local check_threshold=$3
    local click_threshold=$4

    local ctr_conf=${LOCAL_CONF_PATH}/category_ctr.conf
    ( export __CALC_DAYS__=${select_days} && export __CHECK_THRESHOLD__=${check_threshold} && export __CLICK_THRESHOLD__=${click_threshold} && run_mapred ${module_conf} ${ctr_conf} )
    if [ $? -ne 0 ]; then
        return 1
    fi

    if [ "x${WRITE_TO_UPS}" == "xTRUE" ]; then
        local data_dir=`export __CALC_DAYS__=${select_days} && export __CHECK_THRESHOLD__=${check_threshold} && export __CLICK_THRESHOLD__=${click_threshold} && local_output_of ${ctr_conf}`
        write_to_ups ${data_dir}
        echo "write_to_ups" >&2
        ret=$?
    fi
}

function write_to_ups() {
    local data_dir=$1
    if [ ! -d ${data_dir} ]; then
        echo "[${data_dir}] is not a dictionary" >&2
        return 1
    fi

    cat ${data_dir}/part-* | ${LOCAL_BIN_PATH}/write_profile --host=${UPS_HOST} --port=${UPS_PORT} --profile=${UPS_PROFILE} --version=${UPS_VERSION} --batch=${UPS_BATCH} --format=${UPS_FORMAT}
}

function process() {
    local module_conf=$1
    local ret=0
    local timestamp=`date +"%Y%m%d%H%M%S"`

    # write your own logic here
    local process_cjv_conf=${LOCAL_CONF_PATH}/process_cjv.conf
    run_mapred ${module_conf} ${process_cjv_conf}
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi

    # module_conf, select_days, check_threshold, click_threshold
    do_ctr ${module_conf} 90 10 -1
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
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
    rm -f ${LOCAL_LOG_PATH}/*.log.${LOG_CLEANUP_DATE}* &>/dev/null
    rm -rf ${LOCAL_DATA_PATH}/ctr_*/${LOG_CLEANUP_DATE} &>/dev/null
fi
exit ${ret}
