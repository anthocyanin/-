USE origin_common;

ALTER TABLE cc_cct_newzone1
ADD IF NOT EXISTS PARTITION (ds = '${bizdate}')
LOCATION '${bizdate}';

INSERT OVERWRITE TABLE origin_common.cc_cct_newzone1
PARTITION (ds = '${bizdate}')

SELECT '${bizdate}' as ds,
	   t4.icon as icon,
	   t7.area_pv as area_pv,
	   t7.area_pv_c as area_pv_c,
	   t7.area_pv_j as area_pv_j,
	   t7.area_pv_v as area_pv_v,
	   t7.area_uv as area_uv,
	   t7.area_uv_c as area_uv_c,
	   t7.area_uv_j as area_uv_j,
	   t7.area_uv_v as area_uv_v,
	   t7.product_detail_page_uv as product_detail_page_uv,
	   t7.product_detail_page_uv_c as product_detail_page_uv_c,
	   t7.product_detail_page_uv_j as product_detail_page_uv_j,
	   t7.product_detail_page_uv_v as product_detail_page_uv_v,
	   p3.pay_cnt as pay_cnt,
	   p3.pay_cnt_c as pay_cnt_c,
	   p3.pay_cnt_j as pay_cnt_j,
	   p3.pay_cnt_v as pay_cnt_v,
       t1.getcoin AS getcoin,
       t2.addcoin AS addcoin,
       t3.paycoin AS paycoin,
	   p3.pay_fee as pay_fee,
	   p3.cck_commission as cck_commission,
	   t6.c33 as area_share_uv,
	   t6.c44 as vip_fans_page_share_uv,
	   t5.areaproduct_share_pv as areaproduct_share_pv,
	   t5.areaproduct_share_uv as areaproduct_share_uv,
	   t6.c5 as vip_fans_page_product_share_pv,
	   t6.c55 as vip_fans_page_product_share_uv
FROM
  (SELECT max(ds) as ds,
          count(user_id) AS getcoin
   FROM cc_coupon_user
   WHERE ds='${bizdate}'
     AND template_id=13980981 ) t1
INNER JOIN
  (SELECT max(ds) as ds,
          count(order_sn) AS addcoin
   FROM cc_order_coupon_addtime
   WHERE ds='${bizdate}'
     AND template_id=13980981) t2 ON t1.ds=t2.ds
INNER JOIN
  (SELECT max(ds) as ds,
          count(order_sn) AS paycoin
   FROM cc_order_coupon_paytime
   WHERE ds='${bizdate}'
     AND template_id=13980981) t3 ON t1.ds=t3.ds
INNER JOIN
--->icon点击次数
(SELECT max(ds) as ds,
       count(*) as icon
FROM cc_ods_log_cctapp_click_hourly
WHERE ds='${bizdate}'
  AND MODULE = 'cct-home-king'
  AND ZONE = 'bannerList'
  AND ad_id='393145'
  AND SOURCE IN ('cct','cctui')) t4 on t1.ds=t4.ds
INNER JOIN
--->专区商品分享pvuv
(SELECT max(t7.ds) as ds,
       sum(t7.pv) as areaproduct_share_pv,
	   count(distinct t7.user_id) as areaproduct_share_uv
FROM
  (SELECT user_id
   FROM origin_common.cc_ods_log_cctapp_click_hourly
   WHERE ds='${bizdate}'
     AND MODULE ='noob'
     AND ZONE='show'
     AND SOURCE IN ('cct',
                    'cctui')
   GROUP BY user_id) t6
INNER JOIN
  (SELECT user_id,max(ds) as ds,
          count(*) AS pv,
		  count(1) as uv
   FROM origin_common.cc_ods_log_cctapp_click_hourly
   WHERE ds='${bizdate}'
     AND MODULE='detail'
     AND ZONE='spread'
     AND ad_type='cct-new-people-buy.productList'
     AND SOURCE IN ('cct',
                    'cctui')
     AND platform IN ('android',
                      'ios')
   GROUP BY user_id) t7 ON t6.user_id=t7.user_id) t5 on t1.ds=t5.ds
INNER JOIN   
   
   (--->支付订单数-支付总金额-佣金总金额
SELECT max(t1.ds) as ds,
       cast(sum(t1.item_price/100) AS decimal(10,2)) AS pay_fee,
       count(DISTINCT t1.third_tradeno) AS pay_cnt,
	   count(distinct if(m.cck_vip_status=1,t1.third_tradeno,null)) as pay_cnt_v,
	   count(distinct if(m.cck_vip_status=0 and m.cck_vip_level=0,t1.third_tradeno,null)) as pay_cnt_c,
	   count(distinct if(m.cck_vip_status=0 and m.cck_vip_level=1,t1.third_tradeno,null)) as pay_cnt_j,
	   cast(sum(t1.cck_commission/100) AS decimal(10,2)) AS cck_commission
FROM
  (SELECT ds,
          third_tradeno,
          cck_uid,
          UID,
          product_id,
          item_price,
          cck_commission
   FROM origin_common.cc_ods_dwxk_wk_sales_deal_ctime
   WHERE ds='${bizdate}') t1
INNER JOIN
  (SELECT cct_uid,
          guider_uid,
          cck_vip_status,
          cck_vip_level
   FROM origin_common.cc_ods_fs_tui_relation) m ON t1.uid=m.cct_uid
INNER JOIN
  (SELECT cck_uid
   FROM origin_common.cc_ods_dwxk_fs_wk_cck_user
   WHERE ds='${bizdate}'
     AND platform=14) t2 ON m.guider_uid=t2.cck_uid
INNER JOIN
  (SELECT order_sn,
          SOURCE
   FROM origin_common.cc_ods_log_gwapp_order_track_hourly
   WHERE ds >= '${bizdate-3}'
     AND ds <= '${bizdate}'
     AND source='cctui') t3 ON t1.third_tradeno=t3.order_sn
INNER JOIN
  (SELECT order_sn,
          product_id,
          max(track_id) AS track_id
   FROM origin_common.cc_ods_log_new_order_hourly
   WHERE ds >= '${bizdate-3}'
     AND ds <= '${bizdate}'
     AND length(track_id)>0
   GROUP BY order_sn,
            product_id)t4 ON t1.third_tradeno = t4.order_sn
AND t1.product_id=t4.product_id
INNER JOIN
  (SELECT hash_value
   FROM origin_common.cc_ods_fs_gwapp_hash_track_hourly
   WHERE split(track, ':_:')[0]='cct-new-people-buy.productList') t5 ON t4.track_id=t5.hash_value) p3    
on t1.ds=p3.ds 
INNER JOIN
(SELECT 
    max(ds) as ds,
	count(if(module='share-goods' and zone='VIP',user_id,null)) as c5,
	count(distinct if(module='share-goods' and zone='VIP',user_id,null)) as c55,
	--->专区分享uv(VIP)
	count(distinct if(MODULE in ('share-friends','share-friends-circle','share-save','share-cancel') 
	       and zone='new',user_id,null)) as c33,
	--->VIP粉丝页面分享uv(VIP)
	count(distinct if(module='invite' and zone='VIP',user_id,null)) as c44
FROM origin_common.cc_ods_log_cctapp_click_hourly
WHERE ds='${bizdate}'
  AND MODULE in ('share-goods','invite','share-friends','share-friends-circle','share-save','share-cancel')
  AND ZONE in ('VIP','new')
  and source in ('cct','cctui')
  and platform in ('android','ios')) t6 on t1.ds=t6.ds
  
  INNER JOIN
  (
  select max(t1.ds) as ds,
		   sum(t1.c1) AS area_pv,
		   sum(if(m.cck_vip_status=1,t1.c1,0)) as area_pv_v,
		   sum(if(m.cck_vip_status=0 and m.cck_vip_level=1,t1.c1,0)) as area_pv_j,
		   sum(if(m.cck_vip_status=0 and m.cck_vip_level=0,t1.c1,0)) as area_pv_c,
		   sum(t1.c11) AS area_uv,
		   sum(if(m.cck_vip_status=1,t1.c11,0)) as area_uv_v,
		   sum(if(m.cck_vip_status=0 and m.cck_vip_level=1,t1.c11,0)) as area_uv_j,
		   sum(if(m.cck_vip_status=0 and m.cck_vip_level=0,t1.c11,0)) as area_uv_c,
		   sum(t1.c22) as product_detail_page_uv,
		   sum(if(m.cck_vip_status=1,t1.c22,0)) as product_detail_page_uv_v,
		   sum(if(m.cck_vip_status=0 and m.cck_vip_level=1,t1.c22,0)) as product_detail_page_uv_j,
		   sum(if(m.cck_vip_status=0 and m.cck_vip_level=0,t1.c22,0)) as product_detail_page_uv_c
		   
	  from
  (SELECT 
		max(ds) as ds,
		user_id,
		--->专区pv
		--->C端用户-积分用户-VIP用户
		sum(if(ZONE='show',1,0)) AS c1,
		--->专区uv
		---> C端用户-积分用户-VIP用户
		max(if(ZONE='show',1,0)) AS c11,
		--->商品详情页uv
		---> C端用户-积分用户-VIP用户
		max(if(ZONE='goods',1,0)) AS c22
		FROM cc_ods_log_cctapp_click_hourly
		where ds='${bizdate}'
		and source in ('cct','cctui')
		and platform in ('android','ios') 
		and module in ('noob')
		and zone in ('show', 'goods')
	group by user_id) t1
  
  INNER JOIN
  (SELECT cct_uid,
          guider_uid,
          cck_vip_status,
          cck_vip_level
   FROM origin_common.cc_ods_fs_tui_relation) m ON t1.user_id=m.cct_uid
INNER JOIN
  (SELECT cck_uid
   FROM origin_common.cc_ods_dwxk_fs_wk_cck_user
   WHERE ds='${bizdate}'
     AND platform=14) t2 ON m.guider_uid=t2.cck_uid) t7 on t1.ds=t7.ds
