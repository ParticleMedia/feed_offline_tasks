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
        sleep 40m
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
    #local hdfs_cjv_path=${HDFS_WORK_PATH}/cjv/history
    local hdfs_cjv_path=${HDFS_WORK_PATH}/cjv/${DATE_FLAG}
    local hive_sql="SELECT cjv.doc_id, cjv.user_id, unix_timestamp(cjv.ts), cjv.pv_time, cjv.cv_time, cjv.clicked, cjv.liked, cjv.shared, cjv.thumbed_up, cjv.thumbed_down FROM warehouse.online_cjv_hourly as cjv WHERE cjv.joined = 1 and cjv.checked = 1 and cjv.ctype = 'news' and cjv.channel_name = 'foryou'  and cjv.nr_condition not LIKE 'local%' and cjv.nr_condition != 'statechannel' and cjv.user_id > 0 and cjv.pdate = '${CJV_DATE_FLAG}'"
    #local hive_sql="SELECT cjv.doc_id, cjv.user_id, unix_timestamp(cjv.ts), cjv.pv_time, cjv.cv_time, cjv.clicked, cjv.liked, cjv.shared, cjv.thumbed_up, cjv.thumbed_down FROM warehouse.online_cjv_hourly as cjv WHERE cjv.joined = 1 and cjv.checked = 1 and cjv.ctype = 'news' and cjv.channel_name = 'foryou' and cjv.nr_condition not LIKE 'local%' and cjv.nr_condition != 'statechannel' and cjv.user_id > 0 and cjv.pdate >= '2020-04-27' and cjv.pdate <= '2020-07-27'"

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


function daily_merge() {
    local ret=0

    local dump_fail=0
    while [ $dump_fail -lt 3 ]
    do
        echo $dump_fail
        dump_fail=`expr $dump_fail + 1`
        dump_cjv_from_hive
        ret=$?
        if [ ${ret} -eq  0 ]; then
            break
        fi
    done

    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi

    local process_cjv_conf=${LOCAL_CONF_PATH}/process_cjv.conf
    run_mapred ${module_conf} ${process_cjv_conf}
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi

    local process_sc_conf=${LOCAL_CONF_PATH}/process_sc.conf
    run_mapred ${module_conf} ${process_sc_conf}

    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi

    local merge_sc_conf=${LOCAL_CONF_PATH}/merge_sc.conf
    run_mapred ${module_conf} ${merge_sc_conf}
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi

    local cut_conf=${LOCAL_CONF_PATH}/cut.conf
    run_mapred ${module_conf} ${cut_conf}
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi

    if [ "x${WRITE_TO_UPS}" == "xTRUE" -a -n "${UPS_PROFILE}" ]; then
        local user_hdfs_dir=`output_of ${cut_conf}`
        write_to_ups ${user_hdfs_dir}
        echo "write_to_ups" >&2
        ret=$?
        if [ ${ret} -ne  0 ]; then
            return ${ret}
        fi
    fi
    return ${ret}
}

function initialize_merge() {
    #run ctr
    local ret=0
    #run_ctr ${module_conf} chn_sc_ctr_90 ${timestamp} &

    local process_89_sc_init_conf=${LOCAL_CONF_PATH}/process_89_sc_init.conf
    run_mapred ${module_conf} ${process_89_sc_init_conf} &

    # dump_cjv_from_hive
    # ret=$?
    # if [ ${ret} -ne  0 ]; then
    #     return ${ret}
    # fi

    # local process_cjv_conf=${LOCAL_CONF_PATH}/process_cjv.conf
    # run_mapred ${module_conf} ${process_cjv_conf}
    # ret=$?
    # if [ ${ret} -ne  0 ]; then
    #     return ${ret}
    # fi

    local process_sc_conf=${LOCAL_CONF_PATH}/process_sc.conf
    run_mapred ${module_conf} ${process_sc_conf}

    for pid in $(jobs -p); do
        wait ${pid} &>/dev/null
        subRet=$?
        if [ ${subRet} -ne 0 ]; then
            return ${subRet}
        fi
    done
    
    local merge_sc_conf=${LOCAL_CONF_PATH}/merge_sc_init.conf
    run_mapred ${module_conf} ${merge_sc_conf}
    ret=$?
    return ${ret}
}


function process() {
    local module_conf=$1
    local ret=0
    local timestamp=`date +"%Y%m%d%H%M%S"`

    daily_merge
    ret=$?


    return ${ret}
}

function cleanup() {
    if [ -n "${LOG_CLEANUP_DAY}" ]; then
        find ${LOCAL_LOG_PATH}/ -type f -mtime +${LOG_CLEANUP_DAY} -exec rm -f {} \; &>/dev/null
        find ${LOCAL_DATA_PATH}/ctr/ -type d -mtime +${LOG_CLEANUP_DAY} -exec rm -rf {} \; &>/dev/null

        local cleanup_date=`date -d "${DATE_FLAG} -${LOG_CLEANUP_DAY} days" +%Y%m%d`
        ${HDFS_BIN} dfs -rmr -skipTrash ${HDFS_WORK_PATH}/time_decay_history/${__FIELD__}/${cleanup_date} &>/dev/null
        ${HDFS_BIN} dfs -rmr -skipTrash ${HDFS_WORK_PATH}/sc/${__FIELD__}/${cleanup_date} &>/dev/null
        ${HDFS_BIN} dfs -rmr -skipTrash ${HDFS_WORK_PATH}/cut_200_100/${__FIELD__}/${cleanup_date} &>/dev/null
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
source $module_conf_file

# date flag
if [ -n "${RUN_DATE}" ]; then
    export DATE_FLAG=${RUN_DATE}
    CJV_DATE_FLAG=`date -d ${DATE_FLAG} +%Y-%m-%d`
fi
if [ -z "${DATE_FLAG}" ]; then
    export DATE_FLAG=`date +"%Y%m%d"`
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
