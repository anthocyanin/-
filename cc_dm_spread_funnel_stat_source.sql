ALTER TABLE
  ${hive.databases.data.name}.${hive.tables.cc_dm_spread_funnel_stat_source.name}
ADD
  IF NOT EXISTS PARTITION (ds = '${bizdate}') LOCATION '${bizdate}';
INSERT OVERWRITE TABLE ${hive.databases.data.name}.${hive.tables.cc_dm_spread_funnel_stat_source.name} PARTITION (ds = '${bizdate}')

SELECT max(p1.date),
       p1.title,
	   p2.types,
       sum(p1.page_pv),
       sum(COALESCE(p1.page_share_pv,0)),
       sum(COALESCE(p1.detail_pv,0)),
	   sum(COALESCE(p1.spread_pv,0)),
	   0 as material_click,
	   sum(COALESCE(p1.material_pv,0)),
	   COALESCE(sum(COALESCE(p1.material_pv,0))/sum(COALESCE(p1.detail_pv,0)),0.00) as material_show_rate,
	   sum(COALESCE(p1.promotion_pv,0)),
	   sum(COALESCE(p1.wechatPro_pv,0)),
	   sum(COALESCE(p1.circleFriendPro_pv,0)),
	   COALESCE((sum(COALESCE(p1.wechatPro_pv,0))+sum(COALESCE(p1.circleFriendPro_pv,0)))/sum(COALESCE(p1.detail_pv,0)),0.00) as promotion_rate,
	   sum(COALESCE(p1.pQrCode_pv,0)),
	   sum(COALESCE(p1.wechatPQC_pv,0)),
	   sum(COALESCE(p1.circleFriendPQC_pv,0)),
	   COALESCE((sum(COALESCE(p1.wechatPQC_pv,0))+sum(COALESCE(p1.circleFriendPQC_pv,0)))/sum(COALESCE(p1.detail_pv,0)),0.00) as pQrCode_rate
  FROM
    (SELECT '${bizdate}' AS date,
            concat('分类-',t2.title) AS title,
            t1.user_id,
            t1.page_pv,
            0 as page_share_pv,
            t3.detail_pv,
			t3.spread_pv,
			t3.promotion_pv,
			t3.wechatPro_pv,
			t3.circleFriendPro_pv,
			t3.pQrCode_pv,
			t3.wechatPQC_pv,
			t3.circleFriendPQC_pv,
			t3.material_pv
    FROM
        (SELECT module,
                zone,
                ad_id,
				user_id,
                COUNT(*) AS page_pv
           FROM origin_common.cc_ods_log_cctapp_click_hourly
          WHERE ds='${bizdate}'
            AND module ='category-list'
            AND zone='dataList'
            AND length(ad_id)>4
            AND source in ('cct','cctui')
          GROUP BY module,zone,ad_id,user_id) t1
    JOIN
        (SELECT *
           FROM origin_common.cc_ods_fs_cck_ad_material_images
          WHERE ds='${bizdate}'
            AND title in ('精选品牌','日用百货','食品酒水','护肤彩妆','母婴用品','数码家电','服饰鞋包')) t2 ON t1.ad_id=t2.ad_material_id
    LEFT JOIN
        (SELECT a1.category,
		        a1.user_id,
                SUM(IF(a1.zone='enter',pv,0)) AS detail_pv,
				SUM(IF(a1.zone='spread',pv,0)) AS spread_pv,
				SUM(IF(a1.zone='promotion',pv,0)) AS promotion_pv,
				SUM(IF(a1.zone='wechatPro',pv,0)) AS wechatPro_pv,
				SUM(IF(a1.zone='circleFriendPro',pv,0)) AS circleFriendPro_pv,
				SUM(IF(a1.zone='pQrCode',pv,0)) AS pQrCode_pv,
				SUM(IF(a1.zone='wechatPQC',pv,0)) AS wechatPQC_pv,
				SUM(IF(a1.zone='circleFriendPQC',pv,0)) AS circleFriendPQC_pv,
				SUM(IF(a1.module='detail_material' and a1.zone='show',pv,0)) AS material_pv
           FROM
              (SELECT module,
                      zone,
                      split(query,'\u0002')[0] AS category,
					  user_id,
                      count(*) AS pv
                 FROM origin_common.cc_ods_log_cctapp_click_hourly
                WHERE ds='${bizdate}'
                  AND module IN ('detail','detail_app','detail_material')
                  AND zone in ('enter','spread','promotion','wechatPro','circleFriendPro','pQrCode','wechatPQC','circleFriendPQC','show')
                  AND ad_type='category'
                  AND source in ('cct','cctui')
                  AND split(query,'\u0002')[0]!=''
                GROUP BY module,zone,split(query,'\u0002')[0],user_id) a1
         GROUP BY a1.category,a1.user_id) t3 ON t2.title=t3.category AND t1.user_id=t3.user_id

    UNION ALL

    SELECT  '${bizdate}' AS date,
            concat('金刚-',t6.title) AS title,
            t6.user_id,
            t6.page_pv,
            t3.page_share_pv,
            t3.detail_pv,
			t3.spread_pv,
			t3.promotion_pv,
			t3.wechatPro_pv,
			t3.circleFriendPro_pv,
			t3.pQrCode_pv,
			t3.wechatPQC_pv,
			t3.circleFriendPQC_pv,
			t3.material_pv
    FROM
    (SELECT t2.title,
            t1.user_id,
            t1.page_pv
      FROM
      (SELECT MODULE,
              ZONE,
              ad_id,
              user_id,
              COUNT(*) AS page_pv
         FROM origin_common.cc_ods_log_cctapp_click_hourly
         WHERE ds='${bizdate}'
           AND MODULE ='cct-home-king'
           AND ZONE='bannerList'
           AND SOURCE IN ('cct','cctui')
         GROUP BY MODULE,ZONE,ad_id,user_id) t1
    JOIN
      (SELECT ad_material_id,
              title
         FROM origin_common.cc_ods_fs_cck_ad_material_banners
        WHERE ds='${bizdate}'
          AND title IN ('楚楚助农','楚楚自营','网易严选','京东自营','高佣精选','海外购')) t2 ON t1.ad_id=t2.ad_material_id
    UNION ALL
    SELECT '新人专区' AS title,
           user_id,
           count(*) AS page_pv
      FROM origin_common.cc_ods_log_cctapp_click_hourly
      WHERE ds='${bizdate}'
        AND MODULE ='noob'
        AND ZONE='show'
        AND SOURCE IN ('cct','cctui')
      GROUP BY MODULE,ZONE,user_id) t6
    LEFT JOIN
    (SELECT  a4.title,
             a4.user_id,
                SUM(IF(a4.zone='enter',pv,0)) AS detail_pv,
				SUM(IF(a4.zone='spread',pv,0)) AS spread_pv,
				SUM(IF(a4.zone='promotion',pv,0)) AS promotion_pv,
				SUM(IF(a4.zone='wechatPro',pv,0)) AS wechatPro_pv,
				SUM(IF(a4.zone='circleFriendPro',pv,0)) AS circleFriendPro_pv,
				SUM(IF(a4.zone='pQrCode',pv,0)) AS pQrCode_pv,
				SUM(IF(a4.zone='wechatPQC',pv,0)) AS wechatPQC_pv,
				SUM(IF(a4.zone='circleFriendPQC',pv,0)) AS circleFriendPQC_pv,
				SUM(IF(a4.module='detail_material'  and a4.zone='show',pv,0)) AS material_pv,
				SUM(IF(a4.module='new'  and a4.zone='new-share',pv,0)) AS page_share_pv
    FROM
        (SELECT a3.module,a3.zone,
                a2.title,
                a3.user_id,
                count(*) AS pv
           FROM
              (SELECT a1.title,
                      a1.page_id
                 FROM
                    (SELECT ad_material_id,
                            title,
                            split(regexp_extract(query,'act_html(.*?).json', 1),'_')[1] AS page_id,
                            ROW_NUMBER() OVER(PARTITION BY title ORDER BY  ad_material_id DESC) AS sort_num
                       FROM origin_common.cc_ods_fs_cck_ad_material_banners
                      WHERE ds='${bizdate}'
                        AND title IN ('楚楚助农','楚楚自营','海外购')
                        AND template='AFP') a1
               WHERE a1.sort_num=1) a2
                JOIN
                    (SELECT *
                    FROM origin_common.cc_ods_log_cctapp_click_hourly
                    WHERE ds='${bizdate}'
                            AND module IN ('detail','detail_app','detail_material')
                            AND zone in ('enter','spread','promotion','wechatPro','circleFriendPro','pQrCode','wechatPQC','circleFriendPQC','show')
                            AND ad_type ='special-activity'
                            AND source in ('cct','cctui')) a3
                        ON a3.ad_material_id=a2.page_id
          GROUP BY a3.module,a3.zone,a2.title,a3.user_id
        UNION ALL
        SELECT module,zone,
              '京东自营' AS title,
               user_id,
               count(*) AS pv
          FROM origin_common.cc_ods_log_cctapp_click_hourly
         WHERE ds='${bizdate}'
           AND module IN ('detail','detail_app','detail_material')
           AND zone in ('enter','spread','promotion','wechatPro','circleFriendPro','pQrCode','wechatPQC','circleFriendPQC','show')
           AND ad_type ='special-activity'
           AND source in ('cct','cctui')
           AND ad_material_id IN (43291,43321,43348,43347,43322,43349,43350,43351,43352)
         GROUP BY module,zone,'京东自营',user_id
        UNION ALL
        SELECT module,zone,
               '网易严选' AS title,
               user_id,
               count(*) AS pv
          FROM origin_common.cc_ods_log_cctapp_click_hourly
         WHERE ds='${bizdate}'
           AND module IN ('detail','detail_app','detail_material')
           AND zone in ('enter','spread','promotion','wechatPro','circleFriendPro','pQrCode','wechatPQC','circleFriendPQC','show')
           AND ad_type ='special-activity'
           AND source in ('cct','cctui')
           AND ad_material_id IN (43269,43278,43282,43280,43276,43281,43279)
         GROUP BY module,zone,'网易严选',user_id
        UNION ALL
        SELECT module,zone,
               if(ad_type='cct-new-people-buy.productList','新人专区','高佣精选') AS title,
               user_id,
               count(*) AS pv
          FROM origin_common.cc_ods_log_cctapp_click_hourly
         WHERE ds='${bizdate}'
           AND module IN ('detail','detail_app','detail_material','new')
           AND zone in ('enter','spread','promotion','wechatPro','circleFriendPro','pQrCode','wechatPQC','circleFriendPQC','show','new-share')
           AND source in ('cct','cctui')
           AND ad_type  in ('cct-new-people-buy.productList')
         GROUP BY module,zone,ad_type,user_id
        UNION ALL
        SELECT module,zone,
               '高佣精选' AS title,
               user_id,
               count(*) AS pv
          FROM origin_common.cc_ods_log_cctapp_click_hourly
         WHERE ds='${bizdate}'
           AND module IN ('detail','detail_app','detail_material','new')
           AND zone in ('enter','spread','promotion','wechatPro','circleFriendPro','pQrCode','wechatPQC','circleFriendPQC','show','new-share')
           AND source in ('cct','cctui')
           AND ad_type ='special-activity'
           AND ad_material_id=48070
         GROUP BY module,zone,ad_type,user_id
        UNION ALL
        SELECT module,zone,
               '新人专区' AS title,
               user_id,
               count(*) AS pv
          FROM origin_common.cc_ods_log_cctapp_click_hourly
         WHERE ds='${bizdate}'
           AND module IN ('new')
           AND zone in ('new-share')
           AND source in ('cct','cctui')
         GROUP BY module,zone,user_id) a4
    GROUP BY a4.title,a4.user_id) t3 on t3.title=t6.title and t3.user_id=t6.user_id

    UNION ALL

    SELECT  '${bizdate}' AS date,
            concat('首页-',t1.title) AS title,
            t1.user_id,
            t1.page_pv,
             0 as page_share_pv,
            t2.detail_pv,
            t2.spread_pv,
			t2.promotion_pv,
			t2.wechatPro_pv,
			t2.circleFriendPro_pv,
			t2.pQrCode_pv,
			t2.wechatPQC_pv,
			t2.circleFriendPQC_pv,
			t2.material_pv
    FROM
    (SELECT (CASE WHEN zone='banner' THEN '首页banner'
                  WHEN zone='banner_fix' THEN '通栏banner'
                  WHEN zone='splash_screen_click' THEN '闪屏banner'
                  ELSE '弹窗' END) AS title,
             user_id,
            COUNT(*) AS page_pv
       FROM origin_common.cc_ods_log_cctapp_click_hourly
      WHERE ds='${bizdate}'
        AND module ='index'
        AND source in ('cct','cctui')
        AND zone in ('banner','popup','banner_fix','splash_screen_click')
     GROUP BY module,zone,user_id) t1
    LEFT JOIN
    (SELECT a1.title,
	        a3.user_id,
           SUM(detail_pv) AS detail_pv,
           SUM(spread_pv) AS spread_pv,
           SUM(promotion_pv) AS promotion_pv,
           SUM(wechatPro_pv) AS wechatPro_pv,
           SUM(circleFriendPro_pv) AS circleFriendPro_pv,
           SUM(pQrCode_pv) AS pQrCode_pv,
           SUM(wechatPQC_pv) AS wechatPQC_pv,
		   SUM(circleFriendPQC_pv) AS circleFriendPQC_pv,
		   SUM(material_pv) AS material_pv
      FROM
         (SELECT DISTINCT (CASE WHEN ad_key='cct-home-page' then '首页banner'
                       WHEN ad_key='cct-home-slide' then '通栏banner'
                       WHEN ad_key='nimation-second-page' then '闪屏banner'
                       ELSE '弹窗' END) AS title,
                 split(regexp_extract(query,'act_html(.*?).json', 1),'_')[1] AS page_id
            FROM
               (SELECT *
                  FROM origin_common.cc_ods_fs_cck_xb_policies_hourly
                 WHERE ad_key IN ('cct-home-slide','cct-home-page-alert','cct-home-page','animation-second-page')
                   AND zone='bannerList'
                   AND end_time>${bizdate_ts}
                   AND begin_time<${gmtdate_ts}
                   AND status!='delete') t1
            JOIN
               (SELECT ad_material_id,
                       query
                  FROM origin_common.cc_ods_fs_cck_ad_material_banners
                 WHERE template='AFP'
                   AND title NOT LIKE '%测试%'
                   AND title NOT LIKE '%试用%'
               UNION  ALL
               SELECT ad_material_id,
                      query
                 FROM origin_common.cc_ods_fs_cck_ad_material_images
                WHERE template='AFP'
                  AND title NOT LIKE '%测试%'
                  AND title NOT LIKE '%试用%') t2 ON t1.ad_material_id=t2.ad_material_id) a1
    LEFT JOIN
         (SELECT a2.ad_material_id AS page_id,
                 a2.user_id,
                SUM(IF(a2.zone='enter',pv,0)) AS detail_pv,
				SUM(IF(a2.zone='spread',pv,0)) AS spread_pv,
				SUM(IF(a2.zone='promotion',pv,0)) AS promotion_pv,
				SUM(IF(a2.zone='wechatPro',pv,0)) AS wechatPro_pv,
				SUM(IF(a2.zone='circleFriendPro',pv,0)) AS circleFriendPro_pv,
				SUM(IF(a2.zone='pQrCode',pv,0)) AS pQrCode_pv,
				SUM(IF(a2.zone='wechatPQC',pv,0)) AS wechatPQC_pv,
				SUM(IF(a2.zone='circleFriendPQC',pv,0)) AS circleFriendPQC_pv,
				SUM(IF(a2.module='detail_material' and a2.zone='show',pv,0)) AS material_pv
            FROM
               (SELECT module,zone,ad_material_id,user_id,count(*) AS pv
                  FROM origin_common.cc_ods_log_cctapp_click_hourly
                 WHERE ds='${bizdate}'
                   AND ad_type ='special-activity'
                   AND module IN ('detail','detail_app','detail_material')
                   AND zone in ('enter','spread','promotion','wechatPro','circleFriendPro','pQrCode','wechatPQC','circleFriendPQC','show')
                   AND source in ('cct','cctui')
                 GROUP BY module,zone,ad_material_id,user_id) a2
           GROUP BY a2.ad_material_id,a2.user_id) a3 on a1.page_id=a3.page_id
    GROUP BY a1.title,a3.user_id) t2 on t1.title=t2.title and t1.user_id=t2.user_id

    UNION ALL

SELECT  '${bizdate}' AS date,
            concat('首页-',regexp_replace(t1.ad_type,'seckill-tab','秒杀')) AS title,
			t1.user_id,
            t1.page_pv,
            t5.page_share_pv,
            t2.detail_pv,
            t2.spread_pv,
			t2.promotion_pv,
			t2.wechatPro_pv,
			t2.circleFriendPro_pv,
			t2.pQrCode_pv,
			t2.wechatPQC_pv,
			t2.circleFriendPQC_pv,
			t2.material_pv
     FROM
        (SELECT concat('seckill-tab-',ad_id) as ad_type,
		        user_id,
                count(*) AS page_pv
           FROM origin_common.cc_ods_log_cctapp_click_hourly
          WHERE ds='${bizdate}'
            AND module in ('screenings-info')
			AND zone='dataList'
            AND source in ('cct','cctui')
            AND ad_id not like '%-%'
          GROUP BY ad_id,user_id) t1
    LEFT JOIN
        (SELECT module as ad_type,
		        user_id,
                count(*) AS page_share_pv
           FROM origin_common.cc_ods_log_cctapp_click_hourly
          WHERE ds='${bizdate}'
            AND module LIKE 'seckill-tab%'
            AND module NOT IN ('seckill-tab-new')
            AND module NOT LIKE 'seckill-tab-old%'
			AND zone='productList-share'
            AND source in ('cct','cctui')
          GROUP BY module,user_id) t5 on t1.ad_type=t5.ad_type and t1.user_id=t5.user_id
    LEFT JOIN
        (SELECT regexp_replace(a1.ad_type,'.productList','') AS ad_type,
		        a1.user_id,
                SUM(IF(a1.zone='enter',pv,0)) AS detail_pv,
				SUM(IF(a1.zone='spread',pv,0)) AS spread_pv,
				SUM(IF(a1.zone='promotion',pv,0)) AS promotion_pv,
				SUM(IF(a1.zone='wechatPro',pv,0)) AS wechatPro_pv,
				SUM(IF(a1.zone='circleFriendPro',pv,0)) AS circleFriendPro_pv,
				SUM(IF(a1.zone='pQrCode',pv,0)) AS pQrCode_pv,
				SUM(IF(a1.zone='wechatPQC',pv,0)) AS wechatPQC_pv,
				SUM(IF(a1.zone='circleFriendPQC',pv,0)) AS circleFriendPQC_pv,
				SUM(IF(a1.module='detail_material' and a1.zone='show',pv,0)) AS material_pv
           FROM
              (SELECT ad_type,
                      module,
                      zone,
					  user_id,
                      count(*) AS pv
                 FROM origin_common.cc_ods_log_cctapp_click_hourly
                WHERE ds='${bizdate}'
                  AND module IN ('detail','detail_app','detail_material')
                  AND zone in ('enter','spread','promotion','wechatPro','circleFriendPro','pQrCode','wechatPQC','circleFriendPQC','show')
                  AND source in ('cct','cctui')
                  AND ad_type like 'seckill-tab%'
                GROUP BY ad_type,module,zone,user_id) a1
          GROUP BY regexp_replace(a1.ad_type,'.productList',''),a1.user_id) t2 on t1.ad_type=t2.ad_type and t1.user_id=t2.user_id

    UNION ALL

    SELECT '${bizdate}' AS date,
           concat('首页-','搜索页') AS title,
           t1.user_id,
           t1.page_pv,
           0 as page_share_pv,
           t2.detail_pv,
           t2.spread_pv,
		   t2.promotion_pv,
			t2.wechatPro_pv,
			t2.circleFriendPro_pv,
			t2.pQrCode_pv,
			t2.wechatPQC_pv,
			t2.circleFriendPQC_pv,
			t2.material_pv
    FROM
        (SELECT zone,
		        user_id,
                count(*) AS page_pv
           FROM origin_common.cc_ods_log_cctapp_click_hourly
          WHERE ds='${bizdate}'
            AND module='index'
            AND source in ('cct','cctui')
            AND zone='search'
          GROUP BY zone,user_id) t1
    LEFT JOIN
        (SELECT a1.ad_type,
		        a1.user_id,
                SUM(IF(a1.zone='enter',pv,0)) AS detail_pv,
				SUM(IF(a1.zone='spread',pv,0)) AS spread_pv,
				SUM(IF(a1.zone='promotion',pv,0)) AS promotion_pv,
				SUM(IF(a1.zone='wechatPro',pv,0)) AS wechatPro_pv,
				SUM(IF(a1.zone='circleFriendPro',pv,0)) AS circleFriendPro_pv,
				SUM(IF(a1.zone='pQrCode',pv,0)) AS pQrCode_pv,
				SUM(IF(a1.zone='wechatPQC',pv,0)) AS wechatPQC_pv,
				SUM(IF(a1.zone='circleFriendPQC',pv,0)) AS circleFriendPQC_pv,
				SUM(IF(a1.module='detail_material' and a1.zone='show',pv,0)) AS material_pv
           FROM
              (SELECT ad_type,
                      module,
                      zone,
					  user_id,
                      count(*) AS pv
                 FROM origin_common.cc_ods_log_cctapp_click_hourly
                WHERE ds='${bizdate}'
                  AND module IN ('detail','detail_app','detail_material')
                  AND zone in ('enter','spread','promotion','wechatPro','circleFriendPro','pQrCode','wechatPQC','circleFriendPQC','show')
                  AND ad_type ='search'
                  AND source in ('cct','cctui')
                GROUP BY ad_type,module,zone,user_id) a1
         GROUP BY a1.ad_type,a1.user_id) t2 ON t1.zone=t2.ad_type and t1.user_id=t2.user_id ) p1
JOIN
(SELECT DISTINCT t4.types,
                t4.cct_uid
  FROM
    (SELECT cct_uid,
           (CASE WHEN cck_vip_status=0 AND cck_vip_level=0 THEN 1
                 WHEN cck_vip_status=0 AND cck_vip_level=1 THEN 2
                 WHEN cck_vip_status=1 THEN 3 ELSE 0 END) AS types
      FROM origin_common.cc_ods_fs_tui_relation
   UNION ALL
   SELECT cct_uid,
          4 AS types
     FROM origin_common.cc_ods_fs_tui_relation) t4) p2 on p1.user_id=p2.cct_uid
group by p1.title,p2.types

