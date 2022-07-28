#!/bin/bash
set -x

function check_env() {
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

function check_time_decay_history() {
    local prev_date=$(date --date="${DATE_FLAG} -1 day" +%Y%m%d)
    local prev_time_decay_history_path=${HDFS_WORK_PATH}/time_decay_history/${prev_date}/part-*
    if [ -z ${prev_time_decay_history_path} ]; then
        echo "prev time decay history path is empty"
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

function write_to_ups() {
    local data_dir=$1
    ${HDFS_BIN} dfs -test -d ${data_dir}
    if [ $? -ne 0 ]; then
        echo "[${data_dir}] is not a dictionary" >&2
        return 1
    fi

    ${HDFS_BIN} dfs -cat ${data_dir}/part-* | ${LOCAL_BIN_PATH}/write_profile --host=${UPS_HOST} --port=${UPS_PORT} --profile=${UPS_PROFILE} --version=${UPS_VERSION} --batch=${UPS_BATCH} --format=${UPS_FORMAT} --worker=${UPS_WORKER}
    return $?
}

function dump_cjv_from_hive() {
    local hdfs_cjv_path=${HDFS_WORK_PATH}/cjv/${DATE_FLAG}
    local start_date=`date -d "${DATE_FLAG} -180 days" +%Y-%m-%d`
    local hive_sql="SELECT cjv.doc_id, cjv.user_id, unix_timestamp(cjv.ts), cjv.pv_time, cjv.cv_time, cjv.clicked, cjv.liked, cjv.shared, cjv.thumbed_up, cjv.thumbed_down FROM warehouse.online_cjv_hourly as cjv WHERE cjv.joined = 1 and cjv.checked = 1 and cjv.ctype = 'news' and cjv.channel_name = 'foryou' and cjv.nr_condition not LIKE 'local%' and cjv.nr_condition not in ('statechannel', 'failover_local') and cjv.user_id > 0 and cjv.pdate >= '${start_date}' and cjv.pdate <= '${CJV_DATE_FLAG}'"

    local sql_file=${LOCAL_BIN_PATH}/hive_${DATE_FLAG}.sql
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
         --hiveconf fs.s3a.retry.limit=100 \
         --hiveconf fs.s3a.retry.interval=1000ms \
         --hiveconf fs.s3a.retry.throttle.limit=100 \
         --hiveconf fs.s3a.retry.throttle.interval=1s \
         -S -f ${sql_file}
    return $?
}

function initial_merge() {
    local ret=0

    dump_cjv_from_hive
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    local process_cjv_conf=${LOCAL_CONF_PATH}/process_cjv.conf
    run_mapred ${module_conf} ${process_cjv_conf}
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    local initial_decay_conf=${LOCAL_CONF_PATH}/initial_decay.conf
    run_mapred ${module_conf} ${initial_decay_conf}
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    local cut_chn_conf=${LOCAL_CONF_PATH}/cut_chn.conf
    run_mapred ${module_conf} ${cut_chn_conf}
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    if [ "x${WRITE_TO_UPS}" == "xTRUE" -a -n "${UPS_PROFILE}" ]; then
        local user_hdfs_dir=`output_of ${cut_chn_conf}`
        write_to_ups ${user_hdfs_dir}
        echo "write_to_ups" >&2
        ret=$?
        if [ ${ret} -ne 0 ]; then
            return ${ret}
        fi
    fi

    return ${ret}
}

function daily_update() {
    local ret=0

    check_time_decay_history
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    local dump_fail=0
    while [ $dump_fail -lt 3 ]
    do
        echo $dump_fail
        dump_fail=`expr $dump_fail + 1`

        dump_cjv_from_hive
        ret=$?
        if [ ${ret} -eq 0 ]; then
            break
        fi
    done

    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    local process_cjv_conf=${LOCAL_CONF_PATH}/process_cjv.conf
    run_mapred ${module_conf} ${process_cjv_conf}
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    local process_chn_conf=${LOCAL_CONF_PATH}/process_chn.conf
    run_mapred ${module_conf} ${process_chn_conf}

    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    local merge_chn_conf=${LOCAL_CONF_PATH}/merge_chn.conf
    run_mapred ${module_conf} ${merge_chn_conf}
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    local cut_chn_conf=${LOCAL_CONF_PATH}/cut_chn.conf
    run_mapred ${module_conf} ${cut_chn_conf}
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    if [ "x${WRITE_TO_UPS}" == "xTRUE" -a -n "${UPS_PROFILE}" ]; then
        local user_hdfs_dir=`output_of ${cut_chn_conf}`
        write_to_ups ${user_hdfs_dir}
        echo "write_to_ups" >&2
        ret=$?
        if [ ${ret} -ne 0 ]; then
            return ${ret}
        fi
    fi
    return ${ret}
}

function process() {
    local module_conf=$1
    local ret=0
    local timestamp=`date +"%Y%m%d%H%M%S"`

    # write your own logic here
    #daily_update
    initial_merge
    ret=$?

    # post process
    return ${ret}
}

function cleanup() {
    if [ -n "${LOG_CLEANUP_DAY}" ]; then
        if [ -n "${LOCAL_LOG_PATH}" ]; then
            find ${LOCAL_LOG_PATH}/ -type f -mtime +${LOG_CLEANUP_DAY} -exec rm -f {} \; &>/dev/null
        fi
        if [ -n "${LOCAL_DATA_PATH}" ]; then
            find ${LOCAL_DATA_PATH}/ctr/ -type d -mtime +${LOG_CLEANUP_DAY} -exec rm -rf {} \; &>/dev/null
        fi

        local cleanup_date=`date -d "${DATE_FLAG} -${LOG_CLEANUP_DAY} days" +%Y%m%d`
        ${HDFS_BIN} dfs -rmr -skipTrash ${HDFS_WORK_PATH}/time_decay_history/${cleanup_date} &>/dev/null
        ${HDFS_BIN} dfs -rmr -skipTrash ${HDFS_WORK_PATH}/cut_200_100/${cleanup_date} &>/dev/null
        ${HDFS_BIN} dfs -rmr -skipTrash ${HDFS_WORK_PATH}/chn_sc/${cleanup_date} &>/dev/null
        rm ${LOCAL_BIN_PATH}/hive_*
    fi

    if [ -n "${CJV_CLEANUP_DATE}" ]; then
        ${HDFS_BIN} dfs -rmr -skipTrash ${HDFS_WORK_PATH}/click_category/${CJV_CLEANUP_DATE} &>/dev/null
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
if [ ${ret} -ne 0 ]; then
    echo "process failed. ret[${ret}]" 1>&2
    exit ${ret}
fi

cleanup
exit ${ret}

