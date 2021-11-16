insert overwrite directory 's3a://pm-hdfs2/user/ningkang/user_domain_decay/cjv/20211115' row format delimited fields terminated by ',' SELECT cjv.doc_id, cjv.user_id, unix_timestamp(cjv.ts), cjv.pv_time, cjv.cv_time, cjv.clicked, cjv.liked, cjv.shared, cjv.thumbed_up, cjv.thumbed_down FROM warehouse.online_cjv_hourly as cjv WHERE cjv.joined = 1 and cjv.checked = 1 and cjv.ctype = 'news' and cjv.channel_name = 'foryou' and cjv.nr_condition not LIKE 'local%' and cjv.nr_condition != 'statechannel' and cjv.user_id > 0 and cjv.pdate = '2021-11-15';
