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

function run_mapred() {
    bash ${LOCAL_BIN_PATH}/run_mapred.sh $@
    return $?
}

function output_of() {
    local mapred_conf=$1
    (source ${mapred_conf} && echo ${MAPRED_OUTPUT_PATH})
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

function dump_from_hive() {
    local output_path=${HDFS_WORK_PATH}/user_media/${DATE_FLAG}
    local hive_sql="SELECT userid, CONCAT_WS('|', COLLECT_SET(DISTINCT(media_id))) FROM (SELECT userid, CONCAT('f_', single_like) AS pageid FROM (SELECT userid, likes FROM fb_profile.parquet_fb_user) AS user_likes LATERAL VIEW EXPLODE(user_likes.likes) t AS single_like) AS user_page JOIN fb_profile.local_life_pages_parquet AS local_life_pages ON user_page.pageid = local_life_pages.id AND local_life_pages.media_id IS NOT NULL GROUP BY userid"
    local sql_file=${LOCAL_BIN_PATH}/dump_from_hive.sql
    local hive_cmd="insert overwrite directory '${output_path}' row format delimited fields terminated by ',' ${hive_sql};"
    echo ${hive_cmd} >${sql_file}
    ${HDFS_BIN} dfs -rmr -skipTrash ${output_path} &>/dev/null
    ${HIVE_BIN} --hiveconf mapreduce.job.name=${JOB_NAME_PREFIX}_dump_from_hive \
         --hiveconf mapreduce.job.queuename=${DEFAULT_JOB_QUEUE} \
         --hiveconf mapreduce.job.priority=VERY_HIGH \
         --hiveconf yarn.app.mapreduce.am.resource.mb=8192 \
         --hiveconf tez.am.resource.memory.mb=8192 \
         --hiveconf mapreduce.map.memory.mb=3072 \
         --hiveconf mapreduce.reduce.memory.mb=3072 \
         -f ${sql_file}
    return $?
}

function daily_update() {
    local ret=0

    local dump_fail=0
    while [ $dump_fail -lt 3 ]
    do
        echo $dump_fail
        dump_fail=`expr $dump_fail + 1`

        dump_from_hive
        ret=$?
        if [ ${ret} -eq 0 ]; then
            break
        fi
    done
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    local normalize_conf=${LOCAL_CONF_PATH}/normalize.conf
    run_mapred ${module_conf} ${normalize_conf}
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    if [ "x${WRITE_TO_UPS}" == "xTRUE" -a -n "${UPS_PROFILE}" ]; then
        local user_hdfs_dir=`output_of ${normalize_conf}`
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
    daily_update
    ret=$?

    # post process
    return ${ret}
}

function cleanup() {
    if [ -n "${LOG_CLEANUP_DAY}" ]; then
        if [ -n "${LOCAL_LOG_PATH}" ]; then
            find ${LOCAL_LOG_PATH}/ -type f -mtime +${LOG_CLEANUP_DAY} -exec rm -f {} \; &>/dev/null
        fi

        local cleanup_date=`date -d "${DATE_FLAG} -${LOG_CLEANUP_DAY} days" +%Y%m%d`
        ${HDFS_BIN} dfs -rmr -skipTrash ${HDFS_WORK_PATH}/normalize/${cleanup_date} &>/dev/null
        rm ${LOCAL_BIN_PATH}/hive_*
    fi

    if [ -n "${CJV_CLEANUP_DATE}" ]; then
        ${HDFS_BIN} dfs -rmr -skipTrash ${HDFS_WORK_PATH}/user_media/${CJV_CLEANUP_DATE} &>/dev/null
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

