#!/bin/bash
date_str=20200923
cluster_cnt=30
try_cnt=3

hdfs_embedding_dir=s3a://pm-hdfs2/user/wangcong/user_cluster/merge_embedding/${date_str}
local_embedding_dir=../data/embedding/${date_str}
local_cluster_dir=../data/cluster/${date_str}

#mkdir -p ${local_embedding_dir}; rm -rf ${local_embedding_dir}/* &>/dev/null
#hdfs dfs -copyToLocal ${hdfs_embedding_dir}/part-* ${local_embedding_dir}

mkdir -p ${local_cluster_dir}
cat ${local_embedding_dir}/part-* | ./user_cluster -min-total-click=20 -cluster-count=${cluster_cnt} -dimension=50 -iteration=300 -sample=30 -try=${try_cnt} >${local_cluster_dir}/user_cluster_${cluster_cnt}.txt
