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

function write_to_redis() {
    local data_dir=$1
    local expire=$2
    if [ ! -d ${data_dir} ]; then
        echo "[${data_dir}] is not a dictionary" >&2
        return 1
    fi

    cat ${data_dir}/part-* | ${LOCAL_BIN_PATH}/write_redis_tool -addr="proxy.cfb.redisc.nb.com:6379" -db=0 -expire=${expire}
    return $?
}

function write_to_dps() {
    local datafiles=$1
    local dataset=$2

    cat ${datafiles} | ${LOCAL_BIN_PATH}/write_tool -command=set -dataset=${dataset} -format=json -host=doc-profile-offline.ha.nb.com -port=9600 -batch=100
    return $?
}

function run_mapred_and_write_redis() {
    local task_conf=$1
    local module_conf=$2
    local select_hour=$3
    local postfix=$4
    local expire=$5
    local drop=$6
    local use_chn_v2=false
    if [ $# -ge 7 ]; then
        use_chn_v2=$7
    fi
    if [ "x"${drop}  == "x0" ]; then
        drop=""
    fi
    local ret=0

    (export __SELECT_HOURS__=${select_hour}; export __POSTFIX__=${postfix}; export __DROP__=${drop}; export __USE_CHN_V2__=${use_chn_v2}; run_mapred ${module_conf} ${task_conf})
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi

    if [ "x"${WRITE_TO_REDIS} == "xTRUE" ]; then
        local task_data_dir=`(export __SELECT_HOURS__=${select_hour}; export __POSTFIX__=${postfix}; export __DROP__=${drop}; export __USE_CHN_V2__=${use_chn_v2}; local_output_of ${task_conf})`
        write_to_redis ${task_data_dir} ${expire}
        ret=$?
    fi
    return ${ret}
}

function category_topdoc() {
    local topdoc_conf=${LOCAL_CONF_PATH}/category_topdoc.conf
    run_mapred_and_write_redis ${topdoc_conf} $@
    local ret=$?
    return ${ret}
}

function nonnews_category_topdoc() {
    local topdoc_conf=${LOCAL_CONF_PATH}/nonnews_category_topdoc.conf
    run_mapred_and_write_redis ${topdoc_conf} $@
    local ret=$?
    return ${ret}
}

function nonnews_category_cfb_topdoc() {
    local topdoc_conf=${LOCAL_CONF_PATH}/nonnews_category_cfb_topdoc.conf
    run_mapred_and_write_redis ${topdoc_conf} $@
    local ret=$?
    return ${ret}
}

function nonnews_category_cfb_topdoc_v2() {
    local topdoc_conf=${LOCAL_CONF_PATH}/nonnews_category_cfb_topdoc_v2.conf
    run_mapred_and_write_redis ${topdoc_conf} $@
    local ret=$?
    return ${ret}
}

function chn_topdoc() {
    local module_conf=$1
    local select_hour=$2
    local postfix=$3
    local expire=$4
    local drop=$5
    local use_chn_v2=false
    if [ $# -ge 6 ]; then
        use_chn_v2=$6
    fi
    if [ "x"${drop}  == "x0" ]; then
        drop=""
    fi

    local topdoc_conf=${LOCAL_CONF_PATH}/chn_topdoc.conf
    run_mapred_and_write_redis ${topdoc_conf} $@
    local ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi

    local topdoc_dir=`(export __SELECT_HOURS__=${select_hour}; export __POSTFIX__=${postfix}; export __DROP__=${drop}; export __USE_CHN_V2__=${use_chn_v2}; local_output_of ${topdoc_conf})`
    local nfs_file_dir=${NFS_FORYOU_PATH}
    local nfs_file_name=chn_topdoc_${postfix}${drop}.txt
    if [ "x"${use_chn_v2} == "xtrue" ]; then
        nfs_file_name=chnv2_topdoc_${postfix}${drop}.txt
    fi
    mkdir -p ${nfs_file_dir}
    cat ${topdoc_dir}/part-* > ${nfs_file_dir}/${nfs_file_name}
    ret=$?
    return ${ret}
}

function nonnews_chn_topdoc() {
    local topdoc_conf=${LOCAL_CONF_PATH}/nonnews_chn_topdoc.conf
    run_mapred_and_write_redis ${topdoc_conf} $@
    local ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi
}

function nonnews_chn_cfb_topdoc() {
    local topdoc_conf=${LOCAL_CONF_PATH}/nonnews_chn_cfb_topdoc.conf
    run_mapred_and_write_redis ${topdoc_conf} $@
    local ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi
}

function nonnews_chn_cfb_topdoc_v2() {
    local topdoc_conf=${LOCAL_CONF_PATH}/nonnews_chn_cfb_topdoc_v2.conf
    run_mapred_and_write_redis ${topdoc_conf} $@
    local ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi
}

function nonnews_user_cluster_topdoc() {
    local topdoc_conf=${LOCAL_CONF_PATH}/nonnews_user_cluster_topdoc.conf
    run_mapred_and_write_redis ${topdoc_conf} $@
    #local ret=$?
    #return ${ret}
    return 0
}

function nonnews_user_cluster_topdoc_v2() {
    local topdoc_conf=${LOCAL_CONF_PATH}/nonnews_user_cluster_topdoc_v2.conf
    run_mapred_and_write_redis ${topdoc_conf} $@
    #local ret=$?
    #return ${ret}
    return 0
}

function user_cluster_topdoc() {
    local topdoc_conf=${LOCAL_CONF_PATH}/user_cluster_topdoc.conf
    run_mapred_and_write_redis ${topdoc_conf} $@
    #local ret=$?
    #return ${ret}
    return 0
}

function user_cluster_topdoc_v2() {
    local topdoc_conf=${LOCAL_CONF_PATH}/user_cluster_topdoc_v2.conf
    run_mapred_and_write_redis ${topdoc_conf} $@
    #local ret=$?
    #return ${ret}
    return 0
}

function zip_nl_topdoc() {
    local topdoc_conf=${LOCAL_CONF_PATH}/zip_nl_topdoc.conf
    run_mapred_and_write_redis ${topdoc_conf} $@
    #local ret=$?
    #return ${ret}
    return 0
}

function city_nl_topdoc() {
    local topdoc_conf=${LOCAL_CONF_PATH}/city_nl_topdoc.conf
    run_mapred_and_write_redis ${topdoc_conf} $@
    #local ret=$?
    #return ${ret}
    return 0
}

function dma_nl_topdoc() {
    local topdoc_conf=${LOCAL_CONF_PATH}/dma_nl_topdoc.conf
    run_mapred_and_write_redis ${topdoc_conf} $@
    #local ret=$?
    #return ${ret}
    return 0
}

function state_nl_topdoc() {
    local topdoc_conf=${LOCAL_CONF_PATH}/state_nl_topdoc.conf
    run_mapred_and_write_redis ${topdoc_conf} $@
    #local ret=$?
    #return ${ret}
    return 0
}

function city_nl_hotdoc() {
    local topdoc_conf=${LOCAL_CONF_PATH}/city_nl_hotdoc.conf
    run_mapred_and_write_redis ${topdoc_conf} $@
    #local ret=$?
    #return ${ret}
    return 0
}

function state_nl_hotdoc() {
    local topdoc_conf=${LOCAL_CONF_PATH}/state_nl_hotdoc.conf
    run_mapred_and_write_redis ${topdoc_conf} $@
    #local ret=$?
    #return ${ret}
    return 0
}

function tab_ctr() {
    local tab_ctr_conf=${LOCAL_CONF_PATH}/tab_ctr.conf
    run_mapred_and_write_redis ${tab_ctr_conf} $@
    local ret=$?
    return ${ret}
}

function category_ctr() {
    local module_conf=$1
    local select_hour=$2
    local postfix=$3
    local expire=$4
    local drop=$5

    local ctr_conf=${LOCAL_CONF_PATH}/category_ctr.conf
    (export __SELECT_HOURS__=${select_hour}; export __POSTFIX__=${postfix}; export __DROP__=${drop}; run_mapred ${module_conf} ${ctr_conf})
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi

    if [ "x"${WRITE_TO_REDIS} == "xTRUE" ]; then
        local ctr_dir=`(export __SELECT_HOURS__=${select_hour}; export __POSTFIX__=${postfix}; export __DROP__=${drop}; local_output_of ${ctr_conf})`
        write_to_redis ${ctr_dir} ${expire}
        ret=$?
        if [ ${ret} -ne  0 ]; then
            return ${ret}
        fi

        # release to nfs and ares
        local ctr_nfs_dir=${NFS_DOC_PROFILE_PATH}
        local dest_file=cfb_${postfix}
        if [ -n "${drop}" ]; then
            dest_file=${dest_file}_d${drop}
        fi
        cat ${ctr_dir}/part-* | grep "^docid#" | awk -F'[#\t]' '{print $2"\t"$4}' >${ctr_dir}/${dest_file}.tmp
        mkdir -p ${ctr_nfs_dir}
        cp -f ${ctr_dir}/${dest_file}.tmp ${ctr_nfs_dir}/${dest_file}
        /home/services/.local/bin/ares upload --key ${dest_file}.tmp --path ${ctr_dir}/${dest_file}.tmp
        ret=$?
        if [ ${ret} -ne  0 ]; then
            return ${ret}
        fi

         # release to nfs
        local tcat_ctr_nfs_dir=${NFS_FORYOU_PATH}
        local tcat_ctr_dest_file=tcat_ctr_${postfix}
        if [ -n "${drop}" ]; then
            tcat_ctr_dest_file=${tcat_ctr_dest_file}_d${drop}
        fi
        cat ${ctr_dir}/part-* | grep "^tcat" >${ctr_dir}/${tcat_ctr_dest_file}.tmp
        mkdir -p ${tcat_ctr_nfs_dir}
        cp -f ${ctr_dir}/${tcat_ctr_dest_file}.tmp ${tcat_ctr_nfs_dir}/${tcat_ctr_dest_file}
        ret=$?
        if [ ${ret} -ne  0 ]; then
            return ${ret}
        fi

    fi
    return ${ret}
}

function release_grouped_ctr_to_ares() {
    local data_dir=$1
    local postfix=$2
    local drop=$3
    local ret=0

    local dest_file=grouped_ctr_${postfix}
    if [ -n "${drop}" ]; then
        dest_file=${dest_file}_d${drop}
    fi

    cat ${data_dir}/part-* | grep "^grp#doc#" >${data_dir}/${dest_file}.tmp
    sed -i "s/^grp#doc#//g" ${data_dir}/${dest_file}.tmp
    sed -i "s/#${postfix}//g" ${data_dir}/${dest_file}.tmp

    /home/services/.local/bin/ares upload --key ${dest_file}.tmp --path ${data_dir}/${dest_file}.tmp
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi
    return ${ret}
}

function release_grouped_ctr_high_to_ares() {
    local data_dir=$1
    local postfix=$2
    local drop=$3
    local ret=0

    local dest_file=grouped_ctr_high_${postfix}
    if [ -n "${drop}" ]; then
        dest_file=${dest_file}_d${drop}
    fi

    cat ${data_dir}/part-* | grep "^grp_high#doc#" >${data_dir}/${dest_file}.tmp
    sed -i "s/^grp_high#doc#//g" ${data_dir}/${dest_file}.tmp
    sed -i "s/#${postfix}//g" ${data_dir}/${dest_file}.tmp

    /home/services/.local/bin/ares upload --key ${dest_file}.tmp --path ${data_dir}/${dest_file}.tmp
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi
    return ${ret}
}

function grouped_ctr() {
    local module_conf=$1
    local select_hour=$2
    local postfix=$3
    local expire=$4
    local drop=$5

    local ctr_conf=${LOCAL_CONF_PATH}/grouped_ctr.conf
    (export __SELECT_HOURS__=${select_hour}; export __POSTFIX__=${postfix}; export __DROP__=${drop}; run_mapred ${module_conf} ${ctr_conf})
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi

    if [ "x"${WRITE_TO_REDIS} == "xTRUE" ]; then
        local ctr_dir=`(export __SELECT_HOURS__=${select_hour}; export __POSTFIX__=${postfix}; export __DROP__=${drop}; local_output_of ${ctr_conf})`
        write_to_redis ${ctr_dir} ${expire}
        ret=$?
        if [ ${ret} -ne  0 ]; then
            return ${ret}
        fi

        release_grouped_ctr_to_ares ${ctr_dir} ${postfix} ${drop}
        ret=$?
        if [ ${ret} -ne  0 ]; then
            return ${ret}
        fi

        if [ "0"${drop} -eq 0 ]; then
            # category ctrs
            local cat_dest_file=cat_grouped_ctr_${postfix}.txt
            local nfs_dest_dir=${NFS_FORYOU_PATH}
            cat ${ctr_dir}/part-* | grep -v "^grp#doc#" >${ctr_dir}/${cat_dest_file}.tmp
            mkdir -p ${nfs_dest_dir}
            mv ${ctr_dir}/${cat_dest_file}.tmp ${nfs_dest_dir}/${cat_dest_file}
            ret=$?
        fi
    fi
    return ${ret}
}

function grouped_ctr_high() {
    local module_conf=$1
    local select_hour=$2
    local postfix=$3
    local expire=$4
    local drop=$5

    local ctr_conf=${LOCAL_CONF_PATH}/grouped_ctr_high.conf
    (export __SELECT_HOURS__=${select_hour}; export __POSTFIX__=${postfix}; export __DROP__=${drop}; run_mapred ${module_conf} ${ctr_conf})
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi

    if [ "x"${WRITE_TO_REDIS} == "xTRUE" ]; then
        local ctr_dir=`(export __SELECT_HOURS__=${select_hour}; export __POSTFIX__=${postfix}; export __DROP__=${drop}; local_output_of ${ctr_conf})`
        write_to_redis ${ctr_dir} ${expire}
        ret=$?
        if [ ${ret} -ne  0 ]; then
            return ${ret}
        fi

        release_grouped_ctr_high_to_ares ${ctr_dir} ${postfix} ${drop}
        ret=$?
        if [ ${ret} -ne  0 ]; then
            return ${ret}
        fi

        if [ "0"${drop} -eq 0 ]; then
            # category ctrs
            local cat_dest_file=cat_grouped_ctr_high_${postfix}.txt
            local nfs_dest_dir=${NFS_FORYOU_PATH}
            cat ${ctr_dir}/part-* | grep -v "^grp_high#doc#" >${ctr_dir}/${cat_dest_file}.tmp
            mkdir -p ${nfs_dest_dir}
            mv ${ctr_dir}/${cat_dest_file}.tmp ${nfs_dest_dir}/${cat_dest_file}
            ret=$?
        fi
    fi
    return ${ret}
}

function clustered_ctr() {
    local module_conf=$1
    local select_hour=$2
    local postfix=$3
    local expire=$4
    local drop=$5

    local ctr_conf=${LOCAL_CONF_PATH}/clustered_ctr.conf
    (export __SELECT_HOURS__=${select_hour}; export __POSTFIX__=${postfix}; export __DROP__=${drop}; run_mapred ${module_conf} ${ctr_conf})
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi

    if [ "x"${WRITE_TO_REDIS} == "xTRUE" ]; then
        local ctr_dir=`(export __SELECT_HOURS__=${select_hour}; export __POSTFIX__=${postfix}; export __DROP__=${drop}; local_output_of ${ctr_conf})`
        write_to_redis ${ctr_dir} ${expire}
        ret=$?
        if [ ${ret} -ne  0 ]; then
            return ${ret}
        fi

        # release to ares
        local dest_file=clustered_ctr_${postfix}
        if [ -n "${drop}" ]; then
            dest_file=${dest_file}_d${drop}
        fi
        cat ${ctr_dir}/part-* | awk -F'[#\t]' '{print $2"\t"$4}' >${ctr_dir}/${dest_file}.tmp
        /home/services/.local/bin/ares upload --key ${dest_file}.tmp --path ${ctr_dir}/${dest_file}.tmp
        ret=$?
        if [ ${ret} -ne  0 ]; then
            return ${ret}
        fi
    fi
    return ${ret}
}

function chn_clustered_ctr() {
    local module_conf=$1
    local select_hour=$2
    local postfix=$3
    local expire=$4
    local drop=$5
    if [ $# -ge 6 ]; then
        use_chn_v2=$6
    fi

    if [ "x"${drop}  == "x0" ]; then
        drop=""
    fi

    local ctr_conf=${LOCAL_CONF_PATH}/chn_clustered_ctr.conf
    (export __SELECT_HOURS__=${select_hour}; export __POSTFIX__=${postfix}; export __DROP__=${drop}; export __USE_CHN_V2__=${use_chn_v2}; run_mapred ${module_conf} ${ctr_conf})
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi

    if [ "x"${WRITE_TO_REDIS} == "xTRUE" ]; then
        local ctr_dir=`(export __SELECT_HOURS__=${select_hour}; export __POSTFIX__=${postfix}; export __DROP__=${drop}; export __USE_CHN_V2__=${use_chn_v2}; local_output_of ${ctr_conf})`
        #write_to_redis ${ctr_dir} ${expire}
        ret=$?
        if [ ${ret} -ne  0 ]; then
            return ${ret}
        fi

        # release to nfs
        local nfs_dir=${NFS_FORYOU_PATH}
        local nfs_file_name=chn_clustered_ctr_${postfix}${drop}.txt
        local local_top_chn_file=${ctr_dir}/cluster_top_chn_${postfix}${drop}.txt
        if [ "x"${use_chn_v2} == "xtrue" ]; then
            nfs_file_name=chnv2_clustered_ctr_${postfix}${drop}.txt
            local_top_chn_file=${ctr_dir}/cluster_top_chnv2_${postfix}${drop}.txt
        fi
        local tmp_file_path=${ctr_dir}/tmp
        mkdir -p ${nfs_dir}
        cat ${ctr_dir}/part-* >${tmp_file_path}; cp -f ${tmp_file_path} ${nfs_dir}/${nfs_file_name}
        ret=$?
        if [ ${ret} -ne  0 ]; then
            return ${ret}
        fi

        # select topchn for each cluster
        cat ${tmp_file_path} | ${LOCAL_BIN_PATH}/cluster_topchn -smooth_check=100 -min_click=20 -min_ratio=1.5 -limit=100 1>${local_top_chn_file}
        ret=$?
        if [ ${ret} -ne  0 ]; then
            return ${ret}
        fi

        cp -f ${local_top_chn_file} ${nfs_dir}
        if [ -n "${tmp_file_path}" ]; then
            rm -f ${tmp_file_path} &>/dev/null
        fi        
        ret=$?
    fi
    return ${ret}
}

function local_ctr() {
    local ctr_conf=${LOCAL_CONF_PATH}/local_ctr.conf
    run_mapred_and_write_redis ${ctr_conf} $@
    local ret=$?
    return ${ret}
}

function process() {
    local module_conf=$1
    local timestamp=`date +"%Y%m%d%H%M%S"`
    local ret=0

    # write your own logic here
    local process_cjv_conf=${LOCAL_CONF_PATH}/process_cjv.conf
    run_mapred ${module_conf} ${process_cjv_conf}
    ret=$?
    if [ ${ret} -ne  0 ]; then
        return ${ret}
    fi

    if [ "x"${IGNORE_CTR} == "xtrue" ]; then
        return ${ret}
    fi

    # select_hours redis_key_postfix redis_expire
    # topdocs for 1d
    category_topdoc ${module_conf} 24 1d 7200 &>${LOCAL_LOG_PATH}/cat_topdoc_1d.log.${timestamp} &
    nonnews_category_topdoc ${module_conf} 24 1d 7200 &>${LOCAL_LOG_PATH}/nonnews_cat_topdoc_1d.log.${timestamp} &
    chn_topdoc ${module_conf} 24 1d 7200 &>${LOCAL_LOG_PATH}/chn_topdoc_1d.log.${timestamp} &
    user_cluster_topdoc ${module_conf} 24 1d 7200 &>${LOCAL_LOG_PATH}/user_cluster_topdoc_1d.log.${timestamp} &
    user_cluster_topdoc_v2 ${module_conf} 24 1d 7200 &>${LOCAL_LOG_PATH}/user_cluster_topdoc_v2_1d.log.${timestamp} &
    #zip_nl_topdoc ${module_conf} 24 1d 7200 &>${LOCAL_LOG_PATH}/zip_nl_topdoc_1d.log.${timestamp} &
    city_nl_topdoc ${module_conf} 24 1d 7200 &>${LOCAL_LOG_PATH}/city_nl_topdoc_1d.log.${timestamp} &
    #dma_nl_topdoc ${module_conf} 24 1d 7200 &>${LOCAL_LOG_PATH}/dma_nl_topdoc_1d.log.${timestamp} &
    state_nl_topdoc ${module_conf} 24 1d 7200 &>${LOCAL_LOG_PATH}/state_nl_topdoc_1d.log.${timestamp} &
    #city_nl_hotdoc ${module_conf} 24 1d 7200 &>${LOCAL_LOG_PATH}/city_nl_hotdoc_1d.log.${timestamp} &
    #state_nl_hotdoc ${module_conf} 24 1d 7200 &>${LOCAL_LOG_PATH}/state_nl_hotdoc_1d.log.${timestamp} &

    # nonnews 1d job: less frequency
    if ((10#${HOUR_FLAG} % 3 == 0)); then
        nonnews_category_cfb_topdoc ${module_conf} 24 1d 86400 &>${LOCAL_LOG_PATH}/nonnews_cat_cfb_topdoc_1d.log.${timestamp} &
        nonnews_category_cfb_topdoc_v2 ${module_conf} 24 1d 86400 &>${LOCAL_LOG_PATH}/nonnews_cat_cfb_topdoc_v2_1d.log.${timestamp} &
    elif ((10#${HOUR_FLAG} % 3 == 1)); then
        nonnews_chn_cfb_topdoc ${module_conf} 24 1d 86400 &>${LOCAL_LOG_PATH}/nonnews_chn_cfb_topdoc_1d.log.${timestamp} &
        nonnews_chn_cfb_topdoc_v2 ${module_conf} 24 1d 86400 &>${LOCAL_LOG_PATH}/nonnews_chn_cfb_topdoc_v2_1d.log.${timestamp} &
    elif ((10#${HOUR_FLAG} % 3 == 2)); then
        nonnews_user_cluster_topdoc ${module_conf} 24 1d 86400 &>${LOCAL_LOG_PATH}/nonnews_user_cluster_topdoc_1d.log.${timestamp} &
        nonnews_user_cluster_topdoc_v2 ${module_conf} 24 1d 86400 &>${LOCAL_LOG_PATH}/nonnews_user_cluster_topdoc_v2_1d.log.${timestamp} &
    fi

    # ctrs for 1d
    category_ctr ${module_conf} 24 1d 7200 &>${LOCAL_LOG_PATH}/cat_ctr_1d.log.${timestamp} &
    local_ctr ${module_conf} 24 1d 7200 &>${LOCAL_LOG_PATH}/local_ctr_1d.log.${timestamp} &
    grouped_ctr ${module_conf} 24 1d 7200 &>${LOCAL_LOG_PATH}/grouped_ctr_1d.log.${timestamp} &
    grouped_ctr_high ${module_conf} 24 1d 7200 &>${LOCAL_LOG_PATH}/grouped_ctr_high_1d.log.${timestamp} &
    clustered_ctr ${module_conf} 24 1d 7200 &>${LOCAL_LOG_PATH}/clustered_ctr_1d.log.${timestamp} &
    tab_ctr ${module_conf} 24 1d 7200 &>${LOCAL_LOG_PATH}/tab_ctr_1d.log.${timestamp} &

    # ctrs for 2d
    if ((10#${HOUR_FLAG} % 2 == 0)); then
        tab_ctr ${module_conf} 48 2d 14400 &>${LOCAL_LOG_PATH}/tab_ctr_2d.log.${timestamp} &
    fi

    # ctrs for 3d
    if ((10#${HOUR_FLAG} % 3 == 0)); then
        category_ctr ${module_conf} 72 3d 21600 &>${LOCAL_LOG_PATH}/cat_ctr_3d.log.${timestamp} &
    elif ((10#${HOUR_FLAG} % 3 == 1)); then
        grouped_ctr ${module_conf} 72 3d 21600 &>${LOCAL_LOG_PATH}/grouped_ctr_3d.log.${timestamp} &
        grouped_ctr_high ${module_conf} 72 3d 21600 &>${LOCAL_LOG_PATH}/grouped_ctr_high_3d.log.${timestamp} &
        #local_ctr ${module_conf} 72 3d 21600 &>${LOCAL_LOG_PATH}/local_ctr_3d.log.${timestamp} &
    elif ((10#${HOUR_FLAG} % 3 == 2)); then
        clustered_ctr ${module_conf} 72 3d 21600 &>${LOCAL_LOG_PATH}/clustered_ctr_3d.log.${timestamp} &
    fi

    # ctrs and topchns and factors for 7d
    if ((10#${HOUR_FLAG} % 6 == 1)); then
        chn_clustered_ctr ${module_conf} 168 7d 43200 &>${LOCAL_LOG_PATH}/chn_clustered_ctr_7d.log.${timestamp} &
    elif ((10#${HOUR_FLAG} % 6 == 2)); then
        nonnews_chn_topdoc ${module_conf} 168 7d 43200 &>${LOCAL_LOG_PATH}/nonnews_chn_topdoc_7d.log.${timestamp} &
    elif ((10#${HOUR_FLAG} % 6 == 5)); then
        chn_clustered_ctr ${module_conf} 168 7d 43200 0 true &>${LOCAL_LOG_PATH}/chnv2_clustered_ctr_7d.log.${timestamp} &
    fi
    # nonnews 7day job: less frequency
    if ((10#${HOUR_FLAG} % 12 == 2)); then
        nonnews_category_cfb_topdoc ${module_conf} 168 7d 86400 &>${LOCAL_LOG_PATH}/nonnews_cat_cfb_topdoc_7d.log.${timestamp} &
        nonnews_category_cfb_topdoc_v2 ${module_conf} 168 7d 86400 &>${LOCAL_LOG_PATH}/nonnews_cat_cfb_topdoc_v2_7d.log.${timestamp} &
    elif ((10#${HOUR_FLAG} % 12 == 3)); then
        nonnews_chn_cfb_topdoc ${module_conf} 168 7d 86400 &>${LOCAL_LOG_PATH}/nonnews_chn_cfb_topdoc_7d.log.${timestamp} &
        nonnews_chn_cfb_topdoc_v2 ${module_conf} 168 7d 86400 &>${LOCAL_LOG_PATH}/nonnews_chn_cfb_topdoc_v2_7d.log.${timestamp} &
    elif ((10#${HOUR_FLAG} % 12 == 4)); then
        nonnews_user_cluster_topdoc ${module_conf} 168 7d 86400 &>${LOCAL_LOG_PATH}/nonnews_user_cluster_topdoc_7d.log.${timestamp} &
        nonnews_user_cluster_topdoc_v2 ${module_conf} 168 7d 86400 &>${LOCAL_LOG_PATH}/nonnews_user_cluster_topdoc_v2_7d.log.${timestamp} &
    fi

    # ctr for 30d
    if ((10#${HOUR_FLAG} % 24 == 2)); then
        nonnews_user_cluster_topdoc ${module_conf} 720 30d 259200 &>${LOCAL_LOG_PATH}/nonnews_user_cluster_topdoc_30d.log.${timestamp} &
        nonnews_user_cluster_topdoc_v2 ${module_conf} 720 30d 259200 &>${LOCAL_LOG_PATH}/nonnews_user_cluster_topdoc_v2_30d.log.${timestamp} &
    elif ((10#${HOUR_FLAG} % 24 == 3)); then
        nonnews_chn_cfb_topdoc ${module_conf} 720 30d 259200 &>${LOCAL_LOG_PATH}/nonnews_chn_cfb_topdoc_30d.log.${timestamp} &
        nonnews_chn_cfb_topdoc_v2 ${module_conf} 720 30d 259200 &>${LOCAL_LOG_PATH}/nonnews_chn_cfb_topdoc_v2_30d.log.${timestamp} &
    elif ((10#${HOUR_FLAG} % 24 == 4)); then
        nonnews_category_cfb_topdoc ${module_conf} 720 30d 259200 &>${LOCAL_LOG_PATH}/nonnews_cat_cfb_topdoc_30d.log.${timestamp} &
        nonnews_category_cfb_topdoc_v2 ${module_conf} 720 30d 259200 &>${LOCAL_LOG_PATH}/nonnews_cat_cfb_topdoc_v2_30d.log.${timestamp} &
    fi

    # if ((10#${HOUR_FLAG} % 24 == 12)); then
    #     nonnews_user_cluster_topdoc_v2 ${module_conf} 720 30d 259200 &>${LOCAL_LOG_PATH}/nonnews_user_cluster_topdoc_v2_30d.log.${timestamp} &
    # elif ((10#${HOUR_FLAG} % 24 == 13)); then
    #     nonnews_chn_cfb_topdoc_v2 ${module_conf} 720 30d 259200 &>${LOCAL_LOG_PATH}/nonnews_chn_cfb_topdoc_v2_30d.log.${timestamp} &
    # elif ((10#${HOUR_FLAG} % 24 == 14)); then
    #     nonnews_category_cfb_topdoc_v2 ${module_conf} 720 30d 259200 &>${LOCAL_LOG_PATH}/nonnews_cat_cfb_topdoc_v2_30d.log.${timestamp} &
    # fi

    for pid in $(jobs -p); do
        wait ${pid} &>/dev/null
        local subret=$?
        if [ ${subret} -ne 0 ]; then
            ret=${subret}
        fi
    done
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    return ${ret}
}

function cleanup() {
    if [ -n "${LOCAL_LOG_PATH}" ]; then
        find ${LOCAL_LOG_PATH}/ -type f -mtime +1 -exec rm -f {} \; &>/dev/null
    fi    
    for file in `ls ${LOCAL_DATA_PATH}`; do
        local clean_path="${LOCAL_DATA_PATH}/${file}"
        if [ -d ${clean_path} ]; then
            find ${clean_path}/ -type d -mtime +1 -exec rm -rf {} \; &>/dev/null
        fi
    done

    if [ -n "${CJV_CLEANUP_DATE}" ]; then
        ${HDFS_BIN} dfs -rmr -skipTrash ${HDFS_WORK_PATH}/*/${CJV_CLEANUP_DATE} &>/dev/null
    fi
}

if [ $# -lt 1 ]; then
    echo "usage: "$0" MODULE_CONF [RUN_DATE]"
    exit 1
fi
module_conf_file=$1
RUN_DATE=$2
RUN_HOUR=$3
IGNORE_CTR=$4

# date flag
if [ -n "${RUN_DATE}" -a -n "${RUN_HOUR}" ]; then
    export DATE_FLAG=${RUN_DATE}
    export HOUR_FLAG=${RUN_HOUR}
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
