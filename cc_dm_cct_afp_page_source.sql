ALTER TABLE
  ${hive.databases.data.name}.${hive.tables.cc_dm_cct_afp_page_source.name}
ADD 
  IF NOT EXISTS PARTITION (ds = '${bizdate}') LOCATION '${bizdate}';
INSERT OVERWRITE TABLE ${hive.databases.data.name}.${hive.tables.cc_dm_cct_afp_page_source.name} PARTITION (ds = '${bizdate}')

SELECT t1.page_id,
       t7.page_url,
       t7.title,
	   CASE WHEN t1.pv IS NULL THEN 0 ELSE t1.pv END,
	   CASE WHEN t1.uv IS NULL THEN 0 ELSE t1.uv END,
	   CASE WHEN t5.pc IS NULL THEN 0 ELSE t5.pc END,
	   CASE WHEN t5.uc IS NULL THEN 0 ELSE t5.uc END,
	   CASE WHEN t4.jump_uc IS NULL THEN 0 ELSE t4.jump_uc END,
	   CASE WHEN t4.jump_num IS NULL THEN 0 ELSE t4.jump_num END,
	   CASE WHEN t6.ipv IS NULL THEN 0 ELSE t6.ipv END,
	   CASE WHEN t2.product_num IS NULL THEN 0 ELSE t2.product_num END,
       CASE WHEN t9.pay_fee IS NULL THEN 0.0 ELSE t9.pay_fee END,
       CASE WHEN t9.pay_count IS NULL THEN 0 ELSE t9.pay_count END,
       CASE WHEN t9.cck_commission IS NULL THEN 0 ELSE t9.cck_commission END,
       CASE WHEN t9.cck_count IS NULL THEN 0 ELSE t9.cck_count END,
       CASE WHEN t3.page_share_cnt IS NULL THEN 0 ELSE t3.page_share_cnt END,
       CASE WHEN t3.page_share_user_cnt IS NULL THEN 0 ELSE t3.page_share_user_cnt END,
       CASE WHEN t8.product_share_user_cnt IS NULL THEN 0 ELSE t8.product_share_user_cnt END,
       '${bizdate}' AS date,
       CASE WHEN t9.pay_fee1 IS NULL THEN 0.0 ELSE t9.pay_fee1 END,
       CASE WHEN t9.pay_count1 IS NULL THEN 0 ELSE t9.pay_count1 END,
       CASE WHEN t9.cck_commission1 IS NULL THEN 0 ELSE t9.cck_commission1 END,
       CASE WHEN t9.cck_count1 IS NULL THEN 0 ELSE t9.cck_count1 END
FROM
    (SELECT url,
            page_id,
		    count(*) AS pv,
		    count(DISTINCT ipaddress) AS uv
    FROM ${hive.databases.ods.name}.${hive.tables.cc_ods_log_cct_wap_afp.name}
    WHERE ds = '${bizdate}'
      AND (pid=14 or pid=-1)
    GROUP BY  url,page_id) t1
LEFT JOIN
    (SELECT t1.page_id,
            count(DISTINCT product_id) AS product_num
       FROM
         (SELECT *
            FROM origin_common.cc_ods_fs_afp_cct_product
           WHERE ds='${bizdate}') t1
       JOIN
         (SELECT DISTINCT id,
                   module_id
            FROM origin_common.cc_ods_fs_afp_page
            LATERAL VIEW explode(split(regexp_replace(regexp_replace(module_sort,'\\\\[',''),'\\\\]',''), ',')) num AS module_id
           WHERE ds='${bizdate}'
            AND module_sort!='') t2 ON t1.page_id=t2.id
             AND t1.module_id=t2.module_id
           GROUP BY t1.page_id) t2 on t1.page_id=t2.page_id
LEFT JOIN
    (SELECT split(c2.track,':_:')[1] AS page_id,
            COUNT(*) AS page_share_cnt,
            COUNT(distinct c1.user_id) AS page_share_user_cnt
       FROM
        (SELECT hash_value,
                app_flag,
                user_id
           FROM ${hive.databases.ods.name}.${hive.tables.cc_ods_log_gwapp_click_hourly.name}
          WHERE ds = '${bizdate}'
            AND module = 'afp'
            AND (zone = 'cctfloaticonshare'
              OR zone = 'headsharecctafp'
              OR zone = 'footersharecctafp')
            AND (app_flag = 'cct' OR app_flag = '')) c1
    JOIN
        (SELECT hash_value,
                track
           FROM ${hive.databases.ods.name}.${hive.tables.cc_ods_fs_gwapp_hash_track_hourly.name}) c2 ON c1.hash_value = c2.hash_value
    GROUP BY  split(c2.track, ':_:')[1]) t3 ON t1.page_id=t3.page_id
LEFT JOIN
    (SELECT split(c4.track,':_:')[1] AS page_id,
           COUNT(distinct c3.user_id) AS product_share_user_cnt
    FROM
        (SELECT hash_value,
               app_flag,
               user_id
        FROM ${hive.databases.ods.name}.${hive.tables.cc_ods_log_gwapp_click_hourly.name}
        WHERE ds = '${bizdate}'
          AND module = 'afp'
          AND ( instr(zone, 'share') != 0
             OR instr(zone, 'Share') != 0 )
          AND zone <> 'cctfloaticonshare'
          AND zone <> 'headsharecctafp'
          AND zone <> 'footersharecctafp'
          AND (app_flag = 'cct' OR app_flag = '')) c3
    JOIN
        (SELECT hash_value,
                track
        FROM ${hive.databases.ods.name}.${hive.tables.cc_ods_fs_gwapp_hash_track_hourly.name}) c4 ON c3.hash_value = c4.hash_value
    GROUP BY  split(c4.track, ':_:')[1]) t8 on t8.page_id=t1.page_id
LEFT JOIN
    (SELECT c8.page_id,
           sum(if(c8.cck_uid1=c8.cck_uid2,c8.item_price,0)) AS pay_fee,
		   sum(if(c8.cck_uid1!=c8.cck_uid2,c8.item_price,0)) AS pay_fee1,
		   --->sum(if(c8.cck_uid1=c8.cck_uid2,c8.cck_commission,0)) AS cck_commission,
           sum(cck_commission) AS cck_commission,
		   sum(if(c8.cck_uid1!=c8.cck_uid2,c8.cck_commission,0)) AS cck_commission1,
		   count(distinct if(c8.cck_uid1=c8.cck_uid2,c8.order_sn,0))-max(if(c8.cck_uid1!=c8.cck_uid2,1,0)) AS pay_count,
		   count(distinct if(c8.cck_uid1!=c8.cck_uid2,c8.order_sn,0))-max(if(c8.cck_uid1=c8.cck_uid2,1,0)) AS pay_count1,
		   count(distinct if(c8.cck_uid1=c8.cck_uid2,c8.cck_uid1,0))-max(if(c8.cck_uid1!=c8.cck_uid2,1,0)) AS cck_count,
		   count(distinct if(c8.cck_uid1!=c8.cck_uid2,c8.cck_uid1,0))-max(if(c8.cck_uid1=c8.cck_uid2,1,0)) AS cck_count1
    FROM
        (SELECT c7.page_id,
                c5.order_sn AS order_sn,
                c5.cck_uid AS cck_uid1,
				if(c9.cck_uid is null ,0,c9.cck_uid) as cck_uid2,
                c5.item_price AS item_price,
                c5.cck_commission AS cck_commission
        FROM
            (SELECT cck_uid,
                    third_tradeno AS order_sn,
                    cast(item_price/100 AS decimal(20,2)) AS item_price,
                    cast(cck_commission/100 AS decimal(20,2)) AS cck_commission,
					uid
               FROM origin_common.cc_ods_dwxk_wk_sales_deal_ctime
              WHERE ds = '${bizdate}' ) c5
        JOIN
            (SELECT cck_uid,
                    platform
               FROM origin_common.cc_ods_dwxk_fs_wk_cck_user
               WHERE ds = '${bizdate}'
                 AND platform = 14 ) c6 ON c5.cck_uid = c6.cck_uid
        JOIN
            (SELECT order_sn,
                    ad_material_id AS page_id
               FROM origin_common.cc_ods_log_gwapp_order_track_hourly
              WHERE ds >= '${bizdate-1}'
                AND ds <= '${bizdate}'
                AND ad_type = 'special-activity' ) c7 ON c5.order_sn = c7.order_sn
		LEFT JOIN
		    (SELECT *
			   FROM origin_common.cc_ods_fs_tui_relation) c9 on c9.cct_uid=c5.uid) c8
    GROUP BY  c8.page_id
) t9 on t9.page_id=t1.page_id
LEFT JOIN
    (SELECT a1.page_id,
            COUNT(distinct a1.jump_type, a1.jump_id) AS jump_num,
            SUM(a1.uc) jump_uc
    FROM
        (SELECT '${bizdate}' AS date,
                page_id,
                jump_type,
                jump_id,
                COUNT(DISTINCT user_id) AS uc
        FROM ${hive.databases.data.name}.${hive.tables.cc_mid_cct_afp_click_union.name}
        WHERE ds='${bizdate}'
        GROUP BY  page_id, jump_type, jump_id) a1
    GROUP BY  a1.page_id) t4 ON t4.page_id=t1.page_id
LEFT JOIN
    (SELECT page_id,
            COUNT(*) AS pc,
            COUNT(DISTINCT user_id) AS uc
       FROM ${hive.databases.data.name}.${hive.tables.cc_mid_cct_afp_click_union.name}
      WHERE ds='${bizdate}'
    GROUP BY page_id) t5 ON t5.page_id=t1.page_id
LEFT JOIN
    (SELECT a3.page_id,
            COUNT(DISTINCT a4.user_id ) ipv
       FROM
           (SELECT DISTINCT page_id,
                            product_id
              FROM ${hive.databases.ods.name}.${hive.tables.cc_ods_fs_afp_cct_product.name}
             WHERE ds='${bizdate}') a3
       LEFT JOIN
           (SELECT *
              FROM ${hive.databases.ods.name}.${hive.tables.cc_ods_log_cctui_product_coupon_detail_hourly.name}
             WHERE ds='${bizdate}') a4 ON a3.product_id=a4.product_id
    GROUP BY a3.page_id) t6 ON t6.page_id=t1.page_id
LEFT JOIN
    (SELECT *
       FROM ${hive.databases.ods.name}.${hive.tables.cc_ods_fs_afp_page.name}
      WHERE ds='${bizdate}') t7 ON t7.id=t1.page_id
