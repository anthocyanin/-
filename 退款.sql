SELECT
  '${bizdate}',
  p1.category1,
  order_cnt,
  refund_suc_cnt,
  refund_create_cnt,
  shop_cname1
FROM
( 
  SELECT 
    category1, COUNT(*) AS order_cnt
  FROM
  ( 
    SELECT 
      shop_id, category1
    FROM
      origin_common.cc_ods_fs_business_basic
  ) t1
  JOIN
  ( 
    SELECT 
      shop_id, order_sn
    FROM
      origin_common.cc_order_user_pay_time
    WHERE
      ds = '${bizdate}' and source_channel=2
  ) t2 
  ON t1.shop_id = t2.shop_id
  GROUP BY category1
) p1
LEFT JOIN
( 
  SELECT
    category1,
    COUNT(DISTINCT t3.order_sn) AS refund_suc_cnt
  FROM
  ( 
    SELECT
      shop_id, category1
    FROM
      origin_common.cc_ods_fs_business_basic
  ) t1
  JOIN
  ( 
    SELECT
      shop_id, order_sn
    FROM
      origin_common.cc_order_user_pay_time
    WHERE ds >= '${bizdate-60}' and source_channel=2
  ) t2
  ON t1.shop_id = t2.shop_id
  JOIN
  (
    SELECT
      order_sn
    FROM
      origin_common.cc_ods_fs_refund_order
    WHERE
      status = 1 AND FROM_UNIXTIME(stop_time, 'yyyyMMdd') = '${bizdate}'
  ) t3
  ON t2.order_sn = t3.order_sn
  GROUP BY category1
) p2
ON p1.category1 = p2.category1
LEFT JOIN
(
  SELECT
    category1,
    COUNT(DISTINCT t3.order_sn) AS refund_create_cnt
  FROM
  (
    SELECT
      shop_id, category1
    FROM
      origin_common.cc_ods_fs_business_basic
  ) t1
  JOIN
  (
    SELECT
      shop_id, order_sn
    FROM
      origin_common.cc_order_user_pay_time
    WHERE ds >= '${bizdate-60}' and source_channel=2
  ) t2
  ON t1.shop_id = t2.shop_id
  JOIN
  (
    SELECT
      order_sn
    FROM
      origin_common.cc_ods_fs_refund_order
    WHERE
      FROM_UNIXTIME(create_time, 'yyyyMMdd') = '${bizdate}'
  ) t3
  ON t2.order_sn = t3.order_sn
  GROUP BY category1
) p3
ON p1.category1 = p3.category1
LEFT JOIN
(
  select
    distinct shop_c1, shop_cname1
  from
    data.cc_dw_fs_products_shops
)p4
ON p1.category1 = p4.shop_c1
