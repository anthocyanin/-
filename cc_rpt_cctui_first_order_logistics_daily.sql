USE ${hive.databases.rpt};

ALTER TABLE ${table_rpt_cctui_first_order_logistics_daily}
DROP IF EXISTS PARTITION (ds = '${bizdate}');

ALTER TABLE ${table_rpt_cctui_first_order_logistics_daily}
ADD IF NOT EXISTS PARTITION (ds = '${bizdate}')
LOCATION '${bizdate}';

INSERT OVERWRITE TABLE ${hive.databases.rpt}.${table_rpt_cctui_first_order_logistics_daily}
PARTITION (ds = '${bizdate}')

SELECT
  O1.cck_uid as cck_uid,
  O1.shop_id as shop_id,
  if(O5.shop_title is NULL , '', O5.shop_title) as shop_title,
  O1.vip_time as vip_time,
  O1.order_sn as order_sn,
  O1.delivery_name as delivery_name,
  O1.delivery_mobilephone as delivery_mobilephone,
  O1.delivery_address as delivery_address,
  O1.delivery_province_name as delivery_province_name,
  O1.delivery_city_name as delivery_city_name,
  O1.pay_time as pay_time,
  O1.bb_time as bb_time,
  O1.hw_type as hw_type,
  O1.sx_flags as sx_flags,
  if(O2.delivery_time is NULL , 0, O2.delivery_time) as delivery_time,
  if(O3.ship_com is NULL , '', O3.ship_com) as ship_com,
  if(O3.ship_sn  is NULL , '', O3.ship_sn) as ship_sn,
  if(O3.update_time is NULL , 0, O3.update_time) as update_time,
  if(O3.content is NULL  , '', O3.content) as content,
  if(O3.sign_push_time is NULL , 0, O3.sign_push_time) as sign_push_time,
  from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS created_on,
  from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS updated_on,
  O1.order_type as order_type,
  O1.cct_uid as cct_uid,
  O1.real_name as real_name,
  if(O4.refund_time is NULL , 0, O4.refund_time) as refund_time

FROM
--楚楚推每天新用户
(
  SELECT
    cck_uid,
    cct_uid,
    vip_time,
    real_name,
    order_sn,
    order_type,
    pay_time,
    shop_id,
    delivery_name,
    delivery_mobilephone,
    delivery_address,
    delivery_province_name,
    delivery_city_name,
    bb_time,
    hw_type,
    sx_flags
  FROM ${hive.databases.rpt}.${table_rpt_cctui_first_order_daily}
  WHERE ds ='${bizdate}' and is_first = 1 and is_over = 0
) O1
LEFT OUTER JOIN
(
-- 发货时间
  SELECT
     order_sn,
     delivery_time
   FROM origin_common.cc_order_user_delivery_time
   WHERE ds >= '${bizdate-30}' and ds <= '${bizdate}'
) O2
ON O1.order_sn = O2.order_sn
LEFT OUTER JOIN
(
  SELECT
    order_sn,
    ship_sn,
    (case
      when ship_com = 'zhongtong' then '中通快递'
      when ship_com = 'ems' then 'EMS快递'
      when ship_com = 'yuantong' then '圆通快递'
      when ship_com = 'yunda' then '韵达快递'
      when ship_com = 'shentong' then '申通快递'
      when ship_com = 'shunfeng' then '顺丰快递'
      when ship_com = 'chengji' then '城际快递'
      when ship_com = 'aae' then 'AAE快递'
      when ship_com = 'anjie' then '安捷快递'
      when ship_com = 'aoshuo' then '奥硕物流'
      when ship_com = 'aramex' then 'Aramex国际快递'
      when ship_com = 'chengshi100' then '城市100'
      when ship_com = 'chuanxi' then '传喜快递'
      when ship_com = 'citylink' then 'CityLinkExpress'
      when ship_com = 'coe' then '东方快递'
      when ship_com = 'datian' then '大田物流'
      when ship_com = 'debangwuliu' then '德邦物流'
      when ship_com = 'dhl' then 'DHL快递'
      when ship_com = 'disifang' then '递四方速递'
      when ship_com = 'dpex' then 'DPEX快递'
      when ship_com = 'dsu' then 'D速快递'
      when ship_com = 'ees' then '百福东方物流'
      when ship_com = 'eyoubao' then 'E邮宝'
      when ship_com = 'fardar' then 'Fardar'
      when ship_com = 'fedex' then '国际Fedex'
      when ship_com = 'feibao' then '飞豹快递'
      when ship_com = 'feihang' then '原飞航物流'
      when ship_com = 'fkd' then '飞康达快递'
      when ship_com = 'gdyz' then '广东邮政物流'
      when ship_com = 'gnxb' then '邮政国内小包'
      when ship_com = 'gongsuda' then '共速达物流|快递'
      when ship_com = 'guotong' then '国通快递'
      when ship_com = 'haihong' then '山东海红快递'
      when ship_com = 'hebeijianhua' then '河北建华快递'
      when ship_com = 'henglu' then '恒路物流'
      when ship_com = 'huaqi' then '华企快递'
      when ship_com = 'huayu' then '天地华宇物流'
      when ship_com = 'huitong' then '汇通快递'
      when ship_com = 'hwhq' then '海外环球快递'
      when ship_com = 'jiaji' then '佳吉快运'
      when ship_com = 'jiayi' then '佳怡物流'
      when ship_com = 'jiayunmei' then '加运美快递'
      when ship_com = 'jiete' then '捷特快递'
      when ship_com = 'jinda' then '金大物流'
      when ship_com = 'jingguang' then '京广快递'
      when ship_com = 'jinyue' then '晋越快递'
      when ship_com = 'jixianda' then '急先达物流'
      when ship_com = 'jldt' then '嘉里大通物流'
      when ship_com = 'kangli' then '康力物流'
      when ship_com = 'kuaijie' then '快捷快递'
      when ship_com = 'kuayue' then '跨越快递'
      when ship_com = 'lejiedi' then '乐捷递快递'
      when ship_com = 'lianhaotong' then '联昊通快递'
      when ship_com = 'lijisong' then '成都立即送快递'
      when ship_com = 'longbang' then '龙邦快递'
      when ship_com = 'menduimen' then '门对门快递'
      when ship_com = 'mingliang' then '明亮物流'
      when ship_com = 'nengda' then '港中能达快递'
      when ship_com = 'ocs' then 'OCS快递'
      when ship_com = 'quanchen' then '全晨快递'
      when ship_com = 'quanfeng' then '全峰快递'
      when ship_com = 'quanritong' then '全日通快递'
      when ship_com = 'quanyi' then '全一快递'
      when ship_com = 'rufeng' then '如风达快递'
      when ship_com = 'saiaodi' then '赛澳递'
      when ship_com = 'santai' then '三态速递'
      when ship_com = 'shengan' then '圣安物流'
      when ship_com = 'shengfeng' then '盛丰物流'
      when ship_com = 'shenghui' then '盛辉物流'
      when ship_com = 'suijia' then '穗佳物流'
      when ship_com = 'sure' then '速尔快递'
      when ship_com = 'tiantian' then '天天快递'
      when ship_com = 'tnt' then 'TNT快递'
      when ship_com = 'ups' then 'UPS快递'
      when ship_com = 'usps' then 'USPS快递'
      when ship_com = 'weitepai' then '微特派'
      when ship_com = 'xinbang' then '新邦物流'
      when ship_com = 'xinfeng' then '信丰快递'
      when ship_com = 'xiyoute' then '希优特快递'
      when ship_com = 'yad' then '源安达快递'
      when ship_com = 'yafeng' then '亚风快递'
      when ship_com = 'yibang' then '一邦快递'
      when ship_com = 'yinjie' then '银捷快递'
      when ship_com = 'yousu' then '优速快递'
      when ship_com = 'ytfh' then '北京一统飞鸿快递'
      when ship_com = 'yuancheng' then '远成物流'
      when ship_com = 'yuefeng' then '越丰快递'
      when ship_com = 'yuntong' then '运通中港快递'
      when ship_com = 'zhaijisong' then '宅急送快递'
      when ship_com = 'zhengzhoujianhua' then '郑州建华快递'
      when ship_com = 'zhima' then '芝麻开门快递'
      when ship_com = 'zhongtian' then '济南中天万运'
      when ship_com = 'zhongtie' then '中铁快运'
      when ship_com = 'zhongxinda' then '忠信达快递'
      when ship_com = 'zhongyou' then '中邮物流'
      when ship_com = 'wanxiangwuliu' then '万象物流'
      when ship_com = 'yamaxun' then '亚马逊物流'
      when ship_com = 'jd' then '京东快递'
      when ship_com = 'ririshunwuliu' then '日日顺物流'
      when ship_com = 'sxhongmajia' then '山西红马甲'
      when ship_com = 'nanjingshengbang' then '晟邦物流'
      when ship_com = 'pjbest' then '品骏快递'
      when ship_com = 'feiyuanvipshop' then '飞远配送'
      when ship_com = 'sccod' then '丰程物流'
      when ship_com = 'jiuyescm' then '九曳供应链'
      when ship_com = 'annengwuliu' then '安能物流'
      when ship_com = 'huiwen' then '汇文快递'
      when ship_com = 'suning' then '苏宁快递'
      when ship_com = 'dongjun' then '东骏快捷物流'
      when ship_com = 'huangmajia' then '黄马甲物流'
      when ship_com = 'lntjs' then '特急送'
      when ship_com = 'chinaicip' then '卓志速运'
      when ship_com = 'wlps' then '物流配送'
      when ship_com = 'lianbangkuaidi' then '联邦快递'
      else '未知快递'
     end ) AS ship_com,
    content,
    update_time,
    sign_push_time
  FROM origin_common.cc_ods_logistics_record
  WHERE ds='${bizdate}'
) O3
ON O1.order_sn = O3.order_sn
LEFT OUTER JOIN
(
  SELECT
    order_sn,
    min(create_time) as refund_time
  FROM origin_common.cc_ods_fs_refund_order
  WHERE create_time>='${bizdate_ts}'
  GROUP BY order_sn
) O4
ON O1.order_sn = O4.order_sn
LEFT OUTER JOIN
(
  SELECT
    shop_id,
    max(shop_title) as shop_title
  FROM data.cc_dw_fs_products_shops
  WHERE product_c1!=-11
  GROUP BY shop_id
) O5
ON O1.shop_id = O5.shop_id
