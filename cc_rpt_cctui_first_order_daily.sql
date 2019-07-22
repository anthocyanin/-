USE ${hive.databases.rpt};

ALTER TABLE ${table_rpt_cctui_first_order_daily}
ADD IF NOT EXISTS PARTITION (ds = '${bizdate}')
LOCATION '${bizdate}';

INSERT OVERWRITE TABLE ${hive.databases.rpt}.${table_rpt_cctui_first_order_daily}
PARTITION (ds = '${bizdate}')

SELECT
  P1.cck_uid as cck_uid,
  P1.vip_time as vip_time,
  P1.real_name as real_name,
  P1.phone as phone,
  P1.is_first as is_first,
  P1.order_sn as order_sn,
  P1.pay_time as pay_time,
  P1.shop_id as shop_id,
  p1.delivery_name as delivery_name,
  P1.delivery_mobilephone as delivery_mobilephone,
  P1.delivery_address as delivery_address,
  P1.delivery_province_name as delivery_province_name,
  P1.delivery_city_name as delivery_city_name,
  P1.bb_time as bb_time,
  P1.hw_type as hw_type,
  P1.sx_flags as sx_flags,
  if(P2.order_sn is NULL , 0, 1) as is_over,
  from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS created_on,
  from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS updated_on,
  P1.order_type as order_type,
  P1.cct_uid as cct_uid
FROM
(
  SELECT
    O1.cck_uid as cck_uid,
    O1.cct_uid as cct_uid,
    O1.pay_time as vip_time,
    O1.real_name as real_name,
    O1.phone as phone,
    if(O3.order_sn is NULL , 0, 1) as is_first,
    if(O3.order_sn is NULL , '', O3.order_sn) as order_sn,
    if(O3.cct_uid is NULL , '无订单', if(O1.cct_uid = O3.cct_uid , '自购', '销售')) as order_type,
    if(O3.pay_time is NULL , 0, O3.pay_time) as pay_time,
    if(O3.shop_id is NULL , 0, O3.shop_id) as shop_id,
    if(O3.delivery_name is NULL , '', O3.delivery_name) as delivery_name,
    if(O3.delivery_mobilephone is NULL , '', O3.delivery_mobilephone) as delivery_mobilephone,
    if(O3.delivery_address is NULL , '', O3.delivery_address) as delivery_address,
    if(O3.delivery_province_name is NULL , '', O3.delivery_province_name) as delivery_province_name,
    if(O3.delivery_city_name is NULL , '', O3.delivery_city_name) as delivery_city_name,
    if(O3.bb_time is NULL , 0, O3.bb_time) as bb_time,
    if(O3.hw_type is NULL , 0, O3.hw_type) as hw_type,
    if(O3.sx_flags is NULL , 0, O3.sx_flags) as sx_flags
  FROM
  --楚楚推每天新用户
  (
    select
      s1.cck_uid                  as cck_uid,
      s1.pay_time                 as pay_time,
      s1.real_name                as real_name,
      s1.phone                    as phone,
      s2.cct_uid                  as cct_uid
    from
    (
      select
        cck_uid,
        pay_time,
        real_name,
        phone
      from origin_common.cc_ods_dwxk_fs_wk_business_info
      where ds='${bizdate}' and pay_status=1 and pay_time>='${bizdate_ts}' and pay_time<'${gmtdate_ts}'
    ) s1
    inner join
    (
      select
        cck_uid,
        cct_uid
      from origin_common.cc_ods_dwxk_fs_wk_cck_user
      where ds='${bizdate}' and platform=14
    ) s2
    on s1.cck_uid=s2.cck_uid
    UNION ALL
    select
      cck_uid,
      vip_time,
      real_name,
      phone,
      cct_uid
    from ${hive.databases.rpt}.${table_rpt_cctui_first_order_daily}
    where  ds = '${bizdate-1}' and is_first = 0 and is_over = 0
  ) O1
  LEFT OUTER JOIN
  (
    SELECT
      t1.order_sn,
      t1.shop_id,
      t1.pay_time,
      t1.delivery_name,
      t1.delivery_mobilephone,
      t1.delivery_address,
      if(t5.province_name is NULL , '', t5.province_name) as delivery_province_name,
      if(t5.city_name is NULL , '', t5.city_name) as delivery_city_name,
      if(t2.delivery_time is NULL , 0, t2.delivery_time) as bb_time,
      if(t3.hw_type is NULL , 0, t3.hw_type) as hw_type,
      if(t4.sx_flags is NULL , 0, t4.sx_flags) as sx_flags,
      t6.cck_uid as cck_uid,
      t6.cct_uid as cct_uid

    FROM
    (
      -- 支付时间
      SELECT
        order_sn,
        shop_id,
        pay_time,
        delivery_address,
        delivery_name,
        delivery_mobilephone,
        area_id
      FROM origin_common.cc_order_user_pay_time_origin
      WHERE ds = '${bizdate}'
    ) t1
    LEFT OUTER JOIN
    (
    --报备时间
      select
        f1.order_sn       as order_sn,
        f2.delivery_time  as delivery_time
      from
      (
        select
          a.shop_id  as shop_id,
          a.order_sn as order_sn,
          a.pay_time as pay_time
        from origin_common.cc_order_user_pay_time_origin a
        where a.ds = '${bizdate}' and a.shop_id not in (select b.id as shop_id from origin_common.cc_ods_fs_shop b where b.shop_op_type=1 or b.id in (17931,18871))
      ) f1
      inner join
      (
        select
          shop_id,
          report_stime,
          if(real_etime>0,real_etime,report_etime) as report_etime,
          delivery_time
        from origin_common.cc_ods_fs_delivery_white_batch
        where report_status=0
      ) f2
      on f1.shop_id=f2.shop_id
      where f1.pay_time>=f2.report_stime and f1.pay_time<=f2.report_etime
      union all
      select
        distinct f1.order_sn       as order_sn,
        f2.delivery_time           as delivery_time
      from
      (
        select
          a.order_sn            as order_sn,
          a.product_id          as product_id,
          a.pay_time            as pay_time
        from origin_common.cc_order_products_user_pay_time a
        where a.ds = '${bizdate}' and a.shop_id not in (select b.id as shop_id from origin_common.cc_ods_fs_shop b where b.shop_op_type=1 or b.id in (17931,18871))
        union
        select
          a.order_sn            as order_sn,
          a.product_id          as product_id,
          a.pay_time            as pay_time
        from origin_common.cc_ods_order_gift_products_user_pay_time a
        where a.ds = '${bizdate}' and a.shop_id not in (select b.id as shop_id from origin_common.cc_ods_fs_shop b where b.shop_op_type=1 or b.id in (17931,18871))
      ) f1
      inner join
      (
        select
          s2.product_id    as product_id,
          s1.report_stime  as report_stime,
          s1.report_etime  as report_etime,
          s1.delivery_time as delivery_time
        from
        (
          select
            id,
            report_stime,
            if(real_etime>0,real_etime,report_etime) as report_etime,
            delivery_time
          from origin_common.cc_ods_fs_delivery_white_batch
          where report_status=1
        ) s1
        inner join
        (
          select
            batch_id,
            product_id
          from origin_common.cc_ods_fs_delivery_white
        ) s2
        on s1.id=s2.batch_id
      ) f2
      on f1.product_id=f2.product_id
      where f1.product_id=f2.product_id and f1.pay_time>=f2.report_stime and f1.pay_time<=f2.report_etime
    ) t2
    ON t1.order_sn = t2.order_sn
    LEFT OUTER JOIN
    (
    --海外店铺
      SELECT
        id as shop_id,
        cn_name,
        if(shop_op_type=1 or id in (17931,18871),1,0) as hw_type
      FROM origin_common.cc_ods_fs_shop
    ) t3
    ON t1.shop_id = t3.shop_id
    LEFT OUTER JOIN
    (
      SELECT
        s1.order_sn as order_sn,
        s1.product_id as product_id,
        if(s2.product_id is null,0,1) as sx_flags
      FROM
      (
        SELECT
          order_sn,
          product_id
        FROM origin_common.cc_order_products_user_pay_time
        WHERE ds = '${bizdate}'
        union
        SELECT
          order_sn,
          product_id
        FROM origin_common.cc_ods_order_gift_products_user_pay_time
        WHERE ds = '${bizdate}'
      ) s1
      LEFT JOIN
      (
        SELECT
          distinct product_id as product_id
        FROM data.cc_dw_fs_products_shops
        WHERE product_c3=122666001 or product_c3=50010566
      ) s2
      ON s1.product_id=s2.product_id
    ) t4
    ON t1.order_sn = t4.order_sn
    LEFT OUTER JOIN
    (
      SELECT
        area_id,
        province_name,
        city_name
      FROM origin_common.cc_area_city_province
    ) t5
    ON t1.area_id = t5.area_id
    JOIN
    (
      SELECT
      max(cck_uid) as cck_uid,
      max(uid) as cct_uid,
      third_tradeno as order_sn
    FROM origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    WHERE ds = '${bizdate}'
    GROUP BY third_tradeno
    ) t6
    ON t1.order_sn = t6.order_sn
  ) O3
  ON O1.cck_uid = O3.cck_uid
  UNION ALL
  SELECT
    cck_uid,
    cct_uid,
    vip_time,
    real_name,
    phone,
    is_first,
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
  WHERE ds = '${bizdate-1}' AND is_first = 1 AND is_over = 0
) P1
LEFT OUTER JOIN
(
  SELECT
    max(cck_uid) as cck_uid,
    third_tradeno as order_sn
  FROM origin_common.cc_ods_wk_sales_deal_account_mtime
  WHERE ds = '${bizdate}'
  GROUP BY third_tradeno
) P2
ON P1.order_sn = P2.order_sn and P1.cck_uid = P2.cck_uid
