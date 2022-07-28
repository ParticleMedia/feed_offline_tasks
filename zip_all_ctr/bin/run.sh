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

function dump_cjv_from_hive() {
    local hdfs_cjv_path=${HDFS_WORK_PATH}/cjv/${DATE_FLAG}
    #local hdfs_cjv_path=${HDFS_WORK_PATH}/cjv/history
    #local hive_sql="SELECT cjv.doc_id, cjv.nr_key, unix_timestamp(cjv.ts), if(cjv.clicked is null, 0, if((cjv.clicked=1 and cjv.pv_time >= 2000) or (cjv.shared = 1 or cjv.thumbed_up = 1), 1, 0)) FROM warehouse.online_cjv_hourly as cjv WHERE cjv.joined = 1 and cjv.checked = 1 and cjv.ctype = 'news' and cjv.channel_name in ('foryou','local') and cjv.nr_condition LIKE 'local%' and cjv.user_id > 0 and cjv.nr_key is not NULL and cjv.doc_id is not Null and cjv.pdate <= '2020-07-25' and cjv.pdate >= '2020-04-25'"
    local hive_sql="SELECT cjv.doc_id, cjv.nr_key, unix_timestamp(cjv.ts), if(cjv.clicked is null, 0, if((cjv.clicked=1 and cjv.pv_time >= 2000) or (cjv.shared = 1 or cjv.thumbed_up = 1), 1, 0)) FROM warehouse.online_cjv_hourly as cjv WHERE cjv.joined = 1 and cjv.checked = 1 and cjv.ctype = 'news' and cjv.channel_name in ('foryou','local') and cjv.nr_condition LIKE 'local%' and cjv.user_id > 0 and cjv.nr_key is not NULL and cjv.doc_id is not Null and cjv.pdate = '${CJV_DATE_FLAG}'"
    local sql_file=${LOCAL_BIN_PATH}/hive.sql
    local hive_cmd="insert overwrite directory '${hdfs_cjv_path}' row format delimited fields terminated by ',' ${hive_sql};"
    echo ${hive_cmd} >${sql_file}
    ${HDFS_BIN} dfs -rmr -skipTrash ${hdfs_cjv_path} &>/dev/null
    ${HIVE_BIN} --hiveconf mapreduce.job.name=${JOB_NAME_PREFIX}_query_cjv \
         --hiveconf mapreduce.job.queuename=${DEFAULT_JOB_QUEUE} \
         --hiveconf mapreduce.job.priority=VERY_HIGH \
         --hiveconf yarn.app.mapreduce.am.resource.mb=8192 \
         --hiveconf tez.am.resource.memory.mb=8192 \
         --hiveconf mapreduce.map.memory.mb=2048 \
         --hiveconf mapreduce.reduce.memory.mb=2048 \
         -f ${sql_file}
    return $?
}

function do_ctr() {
    local module_conf=$1
    local task_conf=$2
    source ${module_conf}
    source ${task_conf}

    local ctr_conf=${LOCAL_CONF_PATH}/category_ctr.conf
    run_mapred ${module_conf} ${ctr_conf}
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi

    if [ "x${WRITE_TO_LPS}" == "xTRUE" ]; then
        local data_dir=`local_output_of ${ctr_conf}`
        write_to_lps ${data_dir}
        echo "write_to_lps" >&2
        ret=$?
    fi

    return ${ret}

}

function run_ctr() {
    local module_conf=$1
    local task_name=$2
    local timestamp=$3
    
    local task_conf=${LOCAL_CONF_PATH}/${task_name}.conf
    local task_log=${LOCAL_LOG_PATH}/${task_name}.log.${timestamp}
    (do_ctr ${module_conf} ${task_conf} &>${task_log})
    return $?
}

function write_to_lps() {
    local data_dir=$1
    if [ ! -d ${data_dir} ]; then
        echo "[${data_dir}] is not a dictionary" >&2
        return 1
    fi

    cat ${data_dir}/part-* | ${LOCAL_BIN_PATH}/write_tool --host=${LPS_HOST} --port=${LPS_PORT} --profile=${LPS_PROFILE} --batch=${LPS_BATCH} --format=${LPS_FORMAT}
}

function process() {
    local module_conf=$1
    local ret=0
    local timestamp=`date +"%Y%m%d%H%M%S"`
    dump_cjv_from_hive
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi
    # write your own logic here

    local process_cjv_conf=${LOCAL_CONF_PATH}/process_cjv.conf
    run_mapred ${module_conf} ${process_cjv_conf}
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi

    # # run ctrs
    run_ctr ${module_conf} src_ctr_90 ${timestamp} &
    run_ctr ${module_conf} tpcs_ctr_90 ${timestamp} &
    run_ctr ${module_conf} tcat_ctr_90 ${timestamp} &

    for pid in $(jobs -p); do
        wait ${pid} &>/dev/null
        subRet=$?
        if [ ${subRet} -ne 0 ]; then
            ret=${subRet}
        fi
    done
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    # post process
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
    export DATE_FLAG=${RUN_DATE}
    CJV_DATE_FLAG=`date -d ${DATE_FLAG} +%Y-%m-%d`
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
            find ${LOCAL_LOG_PATH}/ -type f -mtime +${LOG_CLEANUP_DAY} -exec rm -f {} \; &>/dev/null
        fi
        if [ -n "${LOCAL_DATA_PATH}" ]; then
            rm -rf ${LOCAL_DATA_PATH}/*_ctr_*/${LOG_CLEANUP_DATE} &>/dev/null        
        fi
fi
exit ${ret}
