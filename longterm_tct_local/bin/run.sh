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

function copy_tf_to_local() {
    local remote_path=$1
    local local_path=$2

    ${HADOOP_BIN} dfs -test -d ${remote_path}
    if [ $? -ne 0 ]; then
        return 1
    fi

    mkdir -p ${local_path}
    if [ -n "${local_path}" ]; then    
        rm -f ${local_path}/* &>/dev/null
    fi
    ${HADOOP_BIN} dfs -copyToLocal ${remote_path}/* ${local_path}
    if [ $? -ne 0 ]; then
        return 1
    fi
    return 0
}

function do_tfidf() {
    local module_conf_file=$1
    local key=$2
    local calc_days=$3
    local click_threshold=$4
    local out_filename=$5

    local output_dir=${LOCAL_DATA_PATH}/tfidf/${DATE_FLAG}
    mkdir -p ${output_dir}

    local tf_conf=${LOCAL_CONF_PATH}/key_category_tf.conf 
    (export __KEY__=${key} && export __CALC_DAYS__=${calc_days} && export __CLICK_THRESHOLD__=${click_threshold} && run_mapred ${module_conf_file} ${tf_conf})
    if [ $? -ne 0 ]; then
        return 1
    fi

    local tf_remote_dir=`(export __KEY__=${key} && export __CALC_DAYS__=${calc_days} && output_of ${tf_conf})`
    local tf_local_dir=${LOCAL_DATA_PATH}/${key}_tf_${calc_days}/${DATE_FLAG}
    copy_tf_to_local ${tf_remote_dir} ${tf_local_dir} &

    local idf_conf=${LOCAL_CONF_PATH}/category_key_cnt.conf 
    (export __KEY__=${key} && export __CALC_DAYS__=${calc_days} && run_mapred ${module_conf_file} ${idf_conf}) 
    if [ $? -ne 0 ]; then
        return 1
    fi

    wait &>/dev/null
    if [ $? -ne 0 ]; then
        return 1
    fi

    local total_key_cnt=`cat ${tf_local_dir}/part-* | wc -l`
    echo ${total_key_cnt} >${output_dir}/total_${key}_cnt_${calc_days}

    # do tfidf locally
    local cate_key_cnt_file=${LOCAL_DATA_PATH}/category_${key}_cnt_${calc_days}/${DATE_FLAG}/part-00000
    cat ${tf_local_dir}/part-* | python ${LOCAL_BIN_PATH}/tfidf_brief.py ${total_key_cnt} ${cate_key_cnt_file} 1>${output_dir}/${out_filename}
    cat ${output_dir}/${out_filename} | python ${LOCAL_BIN_PATH}/tfidf_percentile.py 20 1>${output_dir}/${out_filename}.percentile

    if [ "${tf_local_dir}" != "/" ]; then
        rm -rf ${tf_local_dir}
    fi
    local cate_key_cnt_dir=`dirname ${cate_key_cnt_file}`
    if [ "${cate_key_cnt_dir}" != "/" ]; then
        rm -rf ${cate_key_cnt_dir}
    fi
    ${HADOOP_BIN} dfs -rmr -skipTrash ${tf_remote_dir}
}

function write_to_ups() {
    local data_file=${LOCAL_DATA_PATH}/tfidf/${DATE_FLAG}/user_3m.txt
    if [ ! -f ${data_file} ]; then
        echo "[${data_file}] is not a regular file" >&2
        return 1
    fi

    cat ${data_file} | ${LOCAL_BIN_PATH}/write_profile --host=${UPS_HOST} --port=${UPS_PORT} --profile=${UPS_PROFILE} --version=${UPS_VERSION} --batch=${UPS_BATCH} --format=${UPS_FORMAT}
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

    # key calc_days click_threshold out_filename
    do_tfidf ${module_conf} uid 90 4 user_3m.txt
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi

    if [ "x${WRITE_TO_UPS}" == "xTRUE" ]; then
        write_to_ups
        #echo "write_to_ups" >&2
        ret=$?
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
    if [ -n "${LOCAL_LOG_PATH}" ]; then
        rm -f ${LOCAL_LOG_PATH}/*.log.${LOG_CLEANUP_DATE}* &>/dev/null
    fi
    if [ -n "${LOCAL_DATA_PATH}" ]; then
        rm -rf ${LOCAL_DATA_PATH}/tfidf/${LOG_CLEANUP_DATE} &>/dev/null
    fi
fi
exit ${ret}
