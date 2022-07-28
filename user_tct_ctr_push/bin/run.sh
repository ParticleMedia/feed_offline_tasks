#!/bin/bash
set -x

function check_env()
{
    if [ -z $HADOOP_BIN ]; then
        echo "conf: HADOOP_BIN is needed!"
        return 1
    fi
    if [ -z $HDFS_BIN ]; then
        echo "conf: HDFS_BIN is missing, use $HADOOP_BIN as default"
        HDFS_BIN=$HADOOP_BIN
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
        ${HDFS_BIN} dfs -test -f ${path}
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
    local ret=0

    local ctr_conf=${LOCAL_CONF_PATH}/category_ctr.conf
    ( export __CALC_DAYS__=${select_days} && export __CHECK_THRESHOLD__=${check_threshold} && export __CLICK_THRESHOLD__=${click_threshold} && run_mapred ${module_conf} ${ctr_conf} )
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi

    if [ "x${WRITE_TO_UPS}" == "xTRUE" ]; then
        local data_dir=`export __CALC_DAYS__=${select_days} && export __CHECK_THRESHOLD__=${check_threshold} && export __CLICK_THRESHOLD__=${click_threshold} && local_output_of ${ctr_conf}`
        write_to_ups ${data_dir}
        ret=$?
        echo "write_to_ups" >&2
    fi
    return $ret
}

function write_to_ups() {
    local data_dir=$1
    if [ ! -d ${data_dir} ]; then
        echo "[${data_dir}] is not a dictionary" >&2
        return 1
    fi

    cat ${data_dir}/part-* | ${LOCAL_BIN_PATH}/write_profile --host=${UPS_HOST} --port=${UPS_PORT} --profile=${UPS_PROFILE} --version=${UPS_VERSION} --batch=${UPS_BATCH} --format=${UPS_FORMAT}
}

function dump_cjv_from_hive() {
    local hdfs_cjv_path=${HDFS_WORK_PATH}/cjv/${DATE_FLAG}
    local hive_sql="SELECT cjv.doc_id, cjv.user_id, unix_timestamp(cjv.trigger_ts), if(cjv.clicked is null, 0, cjv.clicked) FROM mds.mds_push_cjv_pst_daily as cjv WHERE cjv.pdate = '${CJV_DATE_FLAG}' and cjv.source LIKE 'local%'"

    local sql_file=${LOCAL_BIN_PATH}/hive.sql
    local hive_cmd="insert overwrite directory '${hdfs_cjv_path}' row format delimited fields terminated by ',' ${hive_sql};"
    echo ${hive_cmd} >${sql_file}
    ${HDFS_BIN} dfs -rmr -skipTrash ${hdfs_cjv_path} &>/dev/null
    ${HIVE_BIN} --hiveconf mapreduce.job.name=${JOB_NAME_PREFIX}_query_cjv \
         --hiveconf mapreduce.job.queuename=${DEFAULT_JOB_QUEUE} \
         --hiveconf yarn.app.mapreduce.am.resource.mb=8192 \
         --hiveconf tez.am.resource.memory.mb=8192 \
         --hiveconf mapreduce.map.memory.mb=2048 \
         --hiveconf mapreduce.reduce.memory.mb=2048 \
         -f ${sql_file}
    return $?
}

function process() {
    local module_conf=$1
    local ret=0
    local timestamp=`date +"%Y%m%d%H%M%S"`

    # write your own logic here
    dump_cjv_from_hive
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi

    local process_cjv_conf=${LOCAL_CONF_PATH}/process_cjv.conf
    run_mapred ${module_conf} ${process_cjv_conf}
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi

    # module_conf, select_days, check_threshold, click_threshold
    do_ctr ${module_conf} 30 10 -1
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
if [ ${ret} -ne 0 ]; then
    echo "process failed. ret[${ret}]" 1>&2
    exit ${ret}
fi

if [ -n "${LOG_CLEANUP_DATE}" ]; then
    if [ -n "${LOCAL_LOG_PATH}" ]; then
        rm -f ${LOCAL_LOG_PATH}/*.log.${LOG_CLEANUP_DATE}* &>/dev/null
    fi
    if [ -n "${LOCAL_DATA_PATH}" ]; then
        rm -rf ${LOCAL_DATA_PATH}/ctr_*/${LOG_CLEANUP_DATE} &>/dev/null
    fi
fi
if [ -n "${CJV_CLEANUP_DATE}" ]; then
    ${HDFS_BIN} dfs -rmr -skipTrash ${HDFS_WORK_PATH}/click_category/${CJV_CLEANUP_DATE} &>/dev/null
fi
exit ${ret}
