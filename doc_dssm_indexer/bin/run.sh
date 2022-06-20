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

function watch_hdfs_file() {
    local path=$1
    local checktimes=$2

    for ((i=0; i<${checktimes}; i++)); do
        ${HADOOP_BIN} dfs -test -f ${path}
        if [ $? -eq 0 ]; then
            return 0
        fi
        sleep 1m
    done
    return 1
}

function run_mapred() {
    bash ${LOCAL_BIN_PATH}/run_mapred.sh $@
    return $?
}

function wait_cpp_data(){
    cpp_path=s3a://cpp-us-pmi/cpp_documents/pdate=${DOC_DATE_FLAG}/phour=${HOUR_FLAG}/_SUCCESS
    watch_hdfs_file ${cpp_path} 50
    return $?
}

function post_body() {
    local index_dir=${LOCAL_DATA_PATH}/index/${DATE_FLAG}${HOUR_FLAG}
    local postbody_file_filter=${index_dir}/postbody_filter_${index_name}.txt
    local docprofile_dir=${LOCAL_DATA_PATH}/docprofiles/${DATE_FLAG}${HOUR_FLAG}

    mkdir -p ${index_dir}
    rm -rf ${index_dir}/*
    #python ${LOCAL_BIN_PATH}/extractDocprofileNew.py ${postbody_file} ${postbody_exp_file} ${postbody_file_filter} ${postbody_exp_file_filter} ${docprofile_dir}
    python ${LOCAL_BIN_PATH}/extractDocprofileNew.py "" ${postbody_file_filter} ${docprofile_dir}
}

function fetch_embedding() {
    local index_dir=${LOCAL_DATA_PATH}/index/${DATE_FLAG}${HOUR_FLAG}
    local postbody_file=${index_dir}/postbody_filter_${index_name}.txt
    local nfs_tf_dir=/mnt/models/kerasmodels
    cat ${postbody_file} | ${LOCAL_BIN_PATH}/embeder --embedding_path=${index_dir}/embedding_${index_name}.txt --user_version_path=${nfs_tf_dir}/user_${index_name}_current_version --doc_version_path=${nfs_tf_dir}/doc_${index_name}_current_version --error_rate_path=${index_dir}/error_rate_${index_name}.txt --is_exp=true
    ret=$?
    return ${ret}
}

function flush_nfs() {
    local index_dir=${LOCAL_DATA_PATH}/index/${DATE_FLAG}${HOUR_FLAG}
    local nfs_foryou_dir=/mnt/models/foryou
    local nfs_tf_dir=/mnt/models/kerasmodels

    cp ${index_dir}/embedding_${index_name}.txt ${nfs_foryou_dir}/embedding_${index_name}.txt
    cp ${index_dir}/${index_name}_docs.map ${nfs_foryou_dir}/${index_name}_docs.map
    cp ${nfs_tf_dir}/doc_${index_name}_current_version ${nfs_tf_dir}/doc_${index_name}_current_version
    cp ${nfs_tf_dir}/doc_${index_name}_current_version ${index_dir}/doc_${index_name}_current_version

    ret=$?
    return ${ret}
}

function check_error_rate() {
    local index_dir=${LOCAL_DATA_PATH}/index/${DATE_FLAG}${HOUR_FLAG}
    local error_rate=$(head -1 ${index_dir}/error_rate_${index_name}.txt)
    ret=$?
    if [ ${ret} -ne 0 ]; then
        echo "read error rate file fail!"
        return ${ret}
    fi

    alert=`echo "$error_rate > 2.0" | bc`
    if [ $alert -eq 1 ]; then
        echo "error rate is greater than 2 percent!"
        ret=3
    fi
    return ${ret}
}

function build_annoy() {
    # build annoy index
    local index_dir=${LOCAL_DATA_PATH}/index/${DATE_FLAG}${HOUR_FLAG}
    local embedding_file=${index_dir}/embedding_${index_name}.txt
    cat ${embedding_file} | ${LOCAL_BIN_PATH}/indexer --dimension=32 --tree=64 --prefix=${index_dir}/${index_name}_docs
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi
    return ${ret}
}

function select_docs() {
    local select_hours=2160 # 90d
    local doc_dir=${LOCAL_DATA_PATH}/docs
    local docid_dir=${LOCAL_DATA_PATH}/docids/${DATE_FLAG}${HOUR_FLAG}
    local file_list=""
    for ((i=1;i<=${select_hours};++i)); do
        local datehourflag=`date +%Y%m%d%H -d "-${i} hours"`
        local filepath=${doc_dir}/${datehourflag}
        if [ -d ${filepath} ]; then
            file_list="${file_list} ${filepath}/part-*"
        fi
    done

    mkdir -p ${docid_dir}
    rm -rf ${docid_dir}/*
    local all_docid_file=${docid_dir}/all_docids.txt
    cat ${file_list} | awk -F "\t" '{print $1}' | sort | uniq >${all_docid_file}
}

function get_docprofiles() {
    local docid_dir=${LOCAL_DATA_PATH}/docids/${DATE_FLAG}${HOUR_FLAG}
    local all_docid_file=${docid_dir}/all_docids.txt
    local split_docid_file=${docid_dir}/split_docids_
    local docprofile_dir=${LOCAL_DATA_PATH}/docprofiles/${DATE_FLAG}${HOUR_FLAG}
    mkdir -p ${docprofile_dir}
    rm -rf ${docprofile_dir}/*
    split -l 30000 ${all_docid_file} ${split_docid_file}

    for docid_file in ${split_docid_file}*; do
        cat ${docid_file} | python ${LOCAL_BIN_PATH}/downloadDoc.py ${docprofile_dir} &
    done

    for pid in $(jobs -p); do
        wait ${pid} &>/dev/null
        subRet=$?
        if [ ${subRet} -ne 0 ]; then
            ret=${subRet}
        fi
    done
    return $?
}

function process() {
    local module_conf=$1
    local ret=0

    # write your own logic here
    #wait_cpp_data
    #ret=$?
    #if [ ${ret} -ne  0 ]; then
    #    return ${ret}
    #fi

    local filter_doc_conf=${LOCAL_CONF_PATH}/filter_doc.conf
    run_mapred ${module_conf} ${filter_doc_conf}
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi

    select_docs
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    get_docprofiles
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    post_body
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    fetch_embedding
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    build_annoy
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    if [ "x${PUSH_INDEX}" == "xTRUE" ]; then
        local index_dir=${LOCAL_DATA_PATH}/index/${DATE_FLAG}${HOUR_FLAG}
        local push_timestamp=`date +%s`
        bash -x ${LOCAL_BIN_PATH}/push.sh $index_name ${index_dir}
        ret=$?
        if [ ${ret} -ne 0 ]; then
            return ${ret}
        fi
        echo "push $index_name" >&2

        cat ${index_dir}/${index_name}_docs.map | cut -f 1 | ${LOCAL_BIN_PATH}/trace_writer -host=172.31.20.243 -port=9750 -event=${index_name}.index -ts=${push_timestamp} -batch=100 1>/dev/null
    fi

    flush_nfs
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    check_error_rate
    ret=$?
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

index_name=dssm_all_video

check_env 1>&2
if [ $? -ne 0 ]; then
    exit 1
fi

timestamp=`date +%Y%m%d%H%M%S`

ts_start=`date +%s`
process ${module_conf_file}
ts_end=`date +%s`

echo "started at `date -d @$ts_start`, finished at `date -d @$ts_end`, cost $((ts_end-ts_start)) seconds"

ret=$?
if [ ${ret} -ne 0 ]; then
    echo "process failed. ret[${ret}]" 1>&2
    exit ${ret}
fi

if [ -n "${LOG_CLEANUP_DATE}" -a -n "${LOG_CLEANUP_HOUR}" ]; then
    rm -f ${LOCAL_LOG_PATH}/*.log.${LOG_CLEANUP_DATE}${LOG_CLEANUP_HOUR}* &>/dev/null
    rm -rf ${LOCAL_DATA_PATH}/index/${LOG_CLEANUP_DATE}${LOG_CLEANUP_HOUR} &>/dev/null
    rm -rf ${LOCAL_DATA_PATH}/docs/${LOG_CLEANUP_DOCS_DATE}${LOG_CLEANUP_HOUR} &>/dev/null
    rm -rf ${LOCAL_DATA_PATH}/docprofiles/${LOG_CLEANUP_DATE}${LOG_CLEANUP_HOUR} &>/dev/null
    #${HADOOP_BIN} dfs -rmr -skipTrash ${HDFS_WORK_PATH}/*/${LOG_CLEANUP_DATE}/${LOG_CLEANUP_HOUR} &>/dev/null
fi
exit ${ret}
