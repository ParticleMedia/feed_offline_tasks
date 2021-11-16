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
    return 0
}

function check_mapred_conf() 
{
    if [ -z $MAPRED_JOB_NAME ]; then
        echo "conf: MAPRED_JOB_NAME is needed!"
        return 1
    fi
    if [ -z $MAPRED_INPUT_PATH ]; then
        echo "conf: MAPRED_INPUT_PATH is needed!"
        return 1
    fi
    if [ -z $MAPRED_OUTPUT_PATH ]; then
        echo "conf: MAPRED_OUTPUT_PATH is needed!"
        return 1
    fi
    if [ -z $MAPRED_TMP_PATH ]; then
        echo "conf: MAPRED_TMP_PATH is needed!"
        return 1
    fi
    if [ -z "$MAPPER_CMD" ]; then
        echo "conf: MAPPER_CMD is needed!"
        return 1
    fi
    if [ -z "$REDUCER_CMD" ]; then
        echo "conf: REDUCER_CMD is needed!"
        return 1
    fi
    if [ -z $INPUT_FORMAT ]; then
        echo "conf: INPUT_FORMAT is needed!"
        return 1
    fi
    if [ -z $OUTPUT_FORMAT ]; then
        echo "conf: OUTPUT_FORMAT is needed!"
        return 1
    fi

    # map task num
    if [ -z $MAP_TASKS ]; then
        MAP_TASKS=100
        echo "conf: MAP_TASKS not found! use ${MAP_TASKS} as default!"
    fi
    # map task capacity
    if [ -z $MAP_TASKS_CAPACITY ]; then
        MAP_TASKS_CAPACITY=100
        echo "conf: MAP_TASKS_CAPACITY not found! use ${MAP_TASKS_CAPACITY} as default!"
    fi
    # map task num
    if [ -z $REDUCE_TASKS ]; then
        REDUCE_TASKS=10
        echo "conf: REDUCE_TASKS not found! use ${REDUCE_TASKS} as default!"
    fi
    # map task capacity
    if [ -z $REDUCE_TASKS_CAPACITY ]; then
        REDUCE_TASKS_CAPACITY=10
        echo "conf: REDUCE_TASKS_CAPACITY not found! use ${REDUCE_TASKS_CAPACITY} as default!"
    fi
    # job priority
    if [ -z $JOB_PRIORITY ]; then
        JOB_PRIORITY="VERY_HIGH"
        echo "conf: JOB_PRIORITY not found! use [${JOB_PRIORITY}] as default!"
    fi
    # job queue
    if [ -z $JOB_QUEUE ]; then
        JOB_QUEUE=${DEFAULT_JOB_QUEUE}
        echo "conf: JOB_QUEUE not found! use [${JOB_QUEUE}] as default!"
    fi

    return 0
}

function gen_cache_archieves() 
{
    for archive in ${CACHE_ARCHIVES[@]};  do  
        echo -e "-cacheArchive "$archive" \c"
    done
}

function gen_cache_files() 
{
    for cache_file in ${CACHE_FILES[@]};  do  
        echo -e "-cacheFile "$cache_file" \c"
    done
}

function gen_upload_files() 
{
    for upload_file in ${UPLOAD_FILES[@]};  do  
        echo -e "-file "$upload_file" \c"
    done
}

function run_mapred()
{
    local timestamp=`date +%Y%m%d%H%M%S`
    local input_path=${MAPRED_INPUT_PATH}
    local output_path=${MAPRED_OUTPUT_PATH}
    #local tmp_output_path=${MAPRED_TMP_PATH}/${MAPRED_JOB_NAME}_${timestamp}
    
    local cache_archives=`gen_cache_archieves`
    local cache_files=`gen_cache_files`
    local upload_files=`gen_upload_files`
    local compress_arguments=""
    if [ "x${COMPRESS_OUTPUT}" == "xTRUE" ]; then
        compress_arguments="-D mapreduce.output.fileoutputformat.compress.type=BLOCK -D mapreduce.map.output.compress=true -D mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec -D mapreduce.output.fileoutputformat.compress=true -D mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec"
    fi

    ${HDFS_BIN} dfs -rmr -skipTrash ${output_path} &>/dev/null

    ${HADOOP_BIN} ${STREAMING_CMD} -D mapred.job.name=${MAPRED_JOB_NAME} \
            -D mapred.map.tasks=${MAP_TASKS} \
            -D mapred.job.map.capacity=${MAP_TASKS_CAPACITY} \
            -D mapred.reduce.tasks=${REDUCE_TASKS} \
            -D mapred.job.reduce.capacity=${REDUCE_TASKS_CAPACITY} \
            -D mapred.job.priority="${JOB_PRIORITY}" \
            -D mapred.job.queue.name="${JOB_QUEUE}" \
            ${DEFAULT_ARGUMENTS} \
            ${ARGUMENTS} \
            ${compress_arguments} \
            ${cache_archives} \
            ${cache_files} \
            ${upload_files} \
            -input ${input_path} \
            -output ${output_path} \
            -mapper "${MAPPER_CMD}" \
            -reducer "${REDUCER_CMD}" \
            -inputformat ${INPUT_FORMAT} \
            -outputformat ${OUTPUT_FORMAT} \

    mapred_ret=$?
    if [ ${mapred_ret} -ne 0 ]; then
        return ${mapred_ret}
    fi
    sleep 10s

    #${HDFS_BIN} dfs -rmr -skipTrash ${output_path} &>/dev/null
    #${HDFS_BIN} dfs -mkdir -p ${output_path} &>/dev/null
    #${HDFS_BIN} dfs -mv ${tmp_output_path}/* ${output_path}
    #local mv_ret=$?
    #if [ ${mv_ret} -ne 0 ]; then
    #    return ${mv_ret}
    #fi
    #${HDFS_BIN} dfs -rmr -skipTrash ${tmp_output_path} &>/dev/null

    if [ "x${CHECK_OUTPUT}" = "xTRUE" ]; then
        check_result_size ${output_path}
        local check_ret=$?
        if [ ${check_ret} -ne 0 ]; then
            return ${check_ret}
        fi
    fi
    
    if [ -n "${COPY_TO_LOCAL}" ]; then
        copy_to_local ${output_path} ${COPY_TO_LOCAL}
        local copy_ret=$?
        if [ ${copy_ret} -ne 0 ]; then
            return ${copy_ret}
        fi
        ${HDFS_BIN} dfs -rmr -skipTrash ${output_path} &>/dev/null
    fi
    
    if [ "x${REMOVE_INPUT}" = "xTRUE" ]; then
        remove_input ${input_path}
    fi
    return 0
}

function remove_input()
{
    local input_path=$1
    inputs=${input_path//,/ }
    for path in $inputs; do
        ${HDFS_BIN} dfs -rmr -skipTrash ${path} &>/dev/null
    done
}

function copy_to_local() 
{
    local hdfs_path=$1
    local local_path=$2
    
    rm -rf ${local_path}/* &>/dev/null
    mkdir -p ${local_path}
    ${HDFS_BIN} dfs -copyToLocal ${hdfs_path}/* ${local_path}
    return $?
}

function check_result_size() 
{
    local hdfs_path=$1
    local part_count=`${HDFS_BIN} dfs -ls ${hdfs_path}/part-* | grep "part-" | wc -l`
    if [ $part_count -eq 0 ]; then
        return 1
    fi
    local ret=`${HDFS_BIN} dfs -ls ${hdfs_path}/part-* | awk 'NF>5{print $5}' | grep "^0$" | wc -l`
    return ${ret}
}
    
if [ $# -lt 2 ]; then
    echo "usage: "$0" MODULE_CONF DISTCP_CONF [RUN_DATE]"
    exit 1
fi
module_conf_file=$1
mapred_conf=$2
source $module_conf_file
check_env 1>&2
if [ $? -ne 0 ]; then
    exit 1
fi

RUN_DATE=$3
RUN_HOUR=$4
debug=$5
# date flag
if [ -z "${DATE_FLAG}" ]; then
    DATE_FLAG=`date +"%Y%m%d"`
fi

if [ -n "${RUN_DATE}" ]; then
    DATE_FLAG=${RUN_DATE}
fi
if [ -n "${RUN_HOUR}" ]; then
    HOUR_FLAG=${RUN_HOUR}
fi

source $mapred_conf
check_mapred_conf 1>&2

if [ -n "${debug}" ];then
  MAPRED_INPUT_PATH=${MAPRED_INPUT_PATH_DEBUG}
fi

if [ $? -ne 0 ]; then
    exit 1
fi

run_mapred
exit $?
