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
    local task_conf=$2
    source ${module_conf}
    source ${task_conf}

    local user_ctr_conf=${LOCAL_CONF_PATH}/user_ctr.conf
    run_mapred ${module_conf} ${user_ctr_conf}
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi

    local local_dir=${LOCAL_DATA_PATH}/ctr/${DATE_FLAG}/${__TASK_NAME__}
    local cat_data_dir=${local_dir}/category

    # category ctr
    local category_ctr_file=${cat_data_dir}/category_ctr
    if [ "x"${__NEED_CATEGORY_CTR__} == "xTRUE" ]; then
        local category_ctr_conf=${LOCAL_CONF_PATH}/category_ctr.conf
        run_mapred ${module_conf} ${category_ctr_conf}
        ret=$?
        if [ ${ret} -ne  0 ]; then
            return ${ret}
        fi

        local cat_hdfs_dir=`output_of ${category_ctr_conf}`
        if [ -n "${cat_data_dir}" ]; then
            rm -rf ${cat_data_dir}
        fi        
        mkdir -p ${cat_data_dir}
        ${HDFS_BIN} dfs -copyToLocal ${cat_hdfs_dir}/part-* ${cat_data_dir}
        mv ${cat_data_dir}/part-00000 ${category_ctr_file}
        ret=$?
        if [ ${ret} -ne  0 ]; then
            return ${ret}
        fi
    fi

    if [ "x${WRITE_TO_UPS}" == "xTRUE" -a -n "${UPS_PROFILE}" ]; then
        local user_hdfs_dir=`output_of ${user_ctr_conf}`
        write_to_ups ${user_hdfs_dir}
        echo "write_to_ups" >&2
        ret=$?
        if [ ${ret} -ne  0 ]; then
            return ${ret}
        fi

        # release category ctr dict to nfs
        if [ "x"${__NEED_CATEGORY_CTR__} == "xTRUE" ]; then
            local nfs_dest=/mnt/models/monica/dict/ctr
            if [ -e ${category_ctr_file} ]; then
                local line_cnt=`wc -l ${category_ctr_file} | awk '{print $1}'`
                if [ ${line_cnt} -gt 0 ]; then
                    mkdir -p ${nfs_dest}
                    cp -f ${category_ctr_file} ${nfs_dest}/${__TASK_NAME__}
                fi
            fi
        fi
    fi

    return ${ret}
}

function write_to_ups() {
    local data_dir=$1
    ${HDFS_BIN} dfs -test -d ${data_dir}
    if [ $? -ne 0 ]; then
        echo "[${data_dir}] is not a dictionary" >&2
        return 1
    fi
    local item_limit=150
    if [ -n "${ITEM_LIMIT}" ]; then
        item_limit=${ITEM_LIMIT}
    fi

    #${HDFS_BIN} dfs -cat ${data_dir}/part-* | python ${LOCAL_BIN_PATH}/trunc.py ${item_limit} | ${LOCAL_BIN_PATH}/write_profile --host=${UPS_HOST} --port=${UPS_PORT} --profile=${UPS_PROFILE} --version=${UPS_VERSION} --batch=${UPS_BATCH} --format=${UPS_FORMAT}
    ${HDFS_BIN} dfs -cat ${data_dir}/part-* | python ${LOCAL_BIN_PATH}/trunc.py ${item_limit} | ${LOCAL_BIN_PATH}/write_profile --host=${UPS_HOST} --port=${UPS_PORT} --profile=${UPS_PROFILE} --version=${UPS_VERSION} --batch=${UPS_BATCH} --format=${UPS_FORMAT} --worker=${UPS_WORKER}
    return $?
}

function dump_cjv_from_hive() {
    local hdfs_cjv_path=${HDFS_WORK_PATH}/cjv/${DATE_FLAG}
    local hive_sql="SELECT cjv.doc_id, cjv.user_id, unix_timestamp(cjv.ts), cjv.pv_time, cjv.cv_time, cjv.clicked, cjv.liked, cjv.shared, cjv.thumbed_up, cjv.thumbed_down FROM warehouse.online_cjv_hourly as cjv WHERE cjv.joined = 1 and cjv.checked = 1 and cjv.ctype = 'news' and cjv.channel_name = 'foryou' and cjv.nr_condition not LIKE 'local%' and cjv.nr_condition != 'statechannel' and cjv.user_id > 0 and cjv.pdate = '${CJV_DATE_FLAG}'"
    # local hive_sql="SELECT cjv.doc_id, cjv.user_id, unix_timestamp(cjv.ts), cjv.pv_time, cjv.cv_time, cjv.clicked, cjv.liked, cjv.shared, cjv.thumbed_up, cjv.thumbed_down FROM warehouse.online_cjv_hourly as cjv WHERE cjv.joined = 1 and cjv.checked = 1 and cjv.ctype = 'news' and cjv.channel_name = 'foryou' and cjv.nr_condition not LIKE 'local%' and cjv.nr_condition != 'statechannel' and cjv.user_id > 0 and cjv.pdate >= '2020-06-19' and cjv.pdate <= '2020-06-21'"

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

function run_ctr() {
    local module_conf=$1
    local task_name=$2
    local timestamp=$3

    local task_conf=${LOCAL_CONF_PATH}/${task_name}.conf
    local task_log=${LOCAL_LOG_PATH}/${task_name}.log.${timestamp}
    (do_ctr ${module_conf} ${task_conf} &>${task_log})
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

    # run ctrs
    run_ctr ${module_conf} tcat_ctr_3 ${timestamp} &
    run_ctr ${module_conf} tcat_ctr_7 ${timestamp} &
    run_ctr ${module_conf} tcat_ctr_14 ${timestamp} &
    run_ctr ${module_conf} tcat_ctr_30 ${timestamp} &
    run_ctr ${module_conf} tcat_ctr_90 ${timestamp} &

    #run_ctr ${module_conf} tpcs_ctr_3 ${timestamp} &
    #run_ctr ${module_conf} tpcs_ctr_7 ${timestamp} &
    #run_ctr ${module_conf} tpcs_ctr_14 ${timestamp} &
    #run_ctr ${module_conf} tpcs_ctr_30 ${timestamp} &

    #run_ctr ${module_conf} chn_ctr_3 ${timestamp} &
    #run_ctr ${module_conf} chn_ctr_7 ${timestamp} &
    #run_ctr ${module_conf} chn_ctr_14 ${timestamp} &
    #run_ctr ${module_conf} chn_ctr_30 ${timestamp} &
    #run_ctr ${module_conf} chn_ctr_90 ${timestamp} &

    #run_ctr ${module_conf} tags_ctr_3 ${timestamp} &
    #run_ctr ${module_conf} tags_ctr_7 ${timestamp} &
    #run_ctr ${module_conf} tags_ctr_14 ${timestamp} &
    #run_ctr ${module_conf} tags_ctr_30 ${timestamp} &

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

function cleanup() {
    if [ -n "${LOG_CLEANUP_DAY}" ]; then
        if [ -n "${LOCAL_LOG_PATH}" ]; then
            find ${LOCAL_LOG_PATH}/ -type f -mtime +${LOG_CLEANUP_DAY} -exec rm -f {} \; &>/dev/null
        fi
        if [ -n "${LOCAL_DATA_PATH}" ]; then
            find ${LOCAL_DATA_PATH}/ctr/ -type d -mtime +${LOG_CLEANUP_DAY} -exec rm -rf {} \; &>/dev/null
        fi

        local cleanup_date=`date -d "${DATE_FLAG} -${LOG_CLEANUP_DAY} days" +%Y%m%d`
        ${HDFS_BIN} dfs -rmr -skipTrash ${HDFS_WORK_PATH}/ctr/${cleanup_date} &>/dev/null
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
