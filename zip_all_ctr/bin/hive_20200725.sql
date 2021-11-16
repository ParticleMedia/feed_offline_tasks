insert overwrite directory 's3a://pm-hdfs2/user/wangcong/zip_all_ctr/cjv/history' row format delimited fields terminated by ',' SELECT cjv.doc_id, cjv.nr_key, unix_timestamp(cjv.ts), if(cjv.clicked is null, 0, if((cjv.clicked=1 and cjv.pv_time >= 2000) or (cjv.shared = 1 or cjv.thumbed_up = 1), 1, 0)) FROM warehouse.online_cjv_parquet_hourly as cjv WHERE cjv.joined = 1 and cjv.checked = 1 and cjv.ctype = 'news' and cjv.channel_name in ('foryou','local') and cjv.nr_condition LIKE 'local%' and cjv.user_id > 0 and cjv.nr_key is not NULL and cjv.doc_id is not Null and cjv.pdate <= '2020-07-25' and cjv.pdate >= '2020-04-25';
