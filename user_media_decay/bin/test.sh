# #test process_sc.conf
# ########################
# source /home/services/ningkang/tasks/user_nlu_sc_fy_nl/conf/process_sc.conf &>/dev/null
# echo ${MAPPER_CMD}
# echo ${REDUCER_CMD}
# ##test mapper
# head /home/services/ningkang/tasks/user_nlu_sc_fy_nl/clc_sample1 |${MAPPER_CMD}
# #filter by cv: works
# #normalize: works
# #not filter by ts: works

# ##test reducer
# head /home/services/ningkang/tasks/user_nlu_sc_fy_nl/clc_sample1 |${MAPPER_CMD}|sort|${REDUCER_CMD}
# #decay logic: works
# ########################

# #test process_sc.conf
# ########################
# source /home/services/ningkang/tasks/user_nlu_sc_fy_nl/conf/process_89_sc_init.conf &>/dev/null
# echo ${MAPPER_CMD}
# echo ${REDUCER_CMD}
# ##test mapper
# cat /home/services/ningkang/tasks/user_nlu_sc_fy_nl/clc_sample1 /home/services/ningkang/tasks/user_nlu_sc_fy_nl/clc_sample2|${MAPPER_CMD}
# #filter by cv: works
# #normalize: works
# #filter by min_ts: works
# #succesfully merge same user: works
# #time decay correct


# ##test reducer
# cat /home/services/ningkang/tasks/user_nlu_sc_fy_nl/clc_sample1 /home/services/ningkang/tasks/user_nlu_sc_fy_nl/clc_sample2|${MAPPER_CMD}|sort|${REDUCER_CMD}
# #decay logic: works
# ########################