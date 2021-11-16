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
    local data_file=$1
    ${HDFS_BIN} dfs -cat ${data_file} | ${LOCAL_BIN_PATH}/write_profile --host=${UPS_HOST} --port=${UPS_PORT} --profile=${UPS_PROFILE} --version=${UPS_VERSION} --batch=${UPS_BATCH} --format=${UPS_FORMAT}
}

function process() {
    local module_conf=$1
    local ret=0

    if [ -z "${debug}" ]; then
      # write your own logic here
      local daily_conf=${LOCAL_CONF_PATH}/daily.conf
      run_mapred ${module_conf} ${daily_conf}
      ret=$?
      if [ ${ret} -ne  0 ]; then
          return ${ret}
      fi
    fi

    local merge_conf=${LOCAL_CONF_PATH}/merge.conf
    if [ -z "${debug}" ]; then
      run_mapred ${module_conf} ${merge_conf}
    fi
    if [ -n "${debug}" ]; then
      # debug mode
      run_mapred ${module_conf} ${merge_conf} ${RUN_DATE} 00000 debug
    fi
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi

    local active_dir=`output_of ${merge_conf}`
    active_dir=$active_dir"/part-*"
    local tagger_dir=${LOCAL_DATA_PATH}/tagger/${DATE_FLAG}
    mkdir -p ${tagger_dir}
    ${HDFS_BIN} dfs -cat ${active_dir} | python ${LOCAL_BIN_PATH}/user_tagger.py >${tagger_dir}/user_tagger.txt

    if [ "x${WRITE_TO_UPS}" == "xTRUE" ] && [ -z "${debug}" ]; then
        write_to_ups ${active_dir}
        echo "write_to_ups" >&2
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
debug=$3

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
    rm -f ${LOCAL_LOG_PATH}/*.log.${LOG_CLEANUP_DATE}* &>/dev/null
    rm -rf ${LOCAL_DATA_PATH}/*/${LOG_CLEANUP_DATE} &>/dev/null
    ${HADOOP_BIN} dfs -rmr -skipTrash ${HDFS_WORK_PATH}/*/${LOG_CLEANUP_DATE} &>/dev/null
    ${HADOOP_BIN} dfs -rmr -skipTrash ${HDFS_WORK_PATH}/active/${LOG_CLEANUP_DATE} &>/dev/null
fi
exit ${ret}
