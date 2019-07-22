USE ${hive.databases.rpt};

ALTER TABLE ${table_rpt_dwxk_product_info_daily}
DROP IF EXISTS PARTITION( ds= '${bizdate}');

ALTER TABLE ${table_rpt_dwxk_product_info_daily}
ADD IF NOT EXISTS PARTITION (ds = '${bizdate}')
LOCATION '${bizdate}';

INSERT OVERWRITE TABLE ${hive.databases.rpt}.${table_rpt_dwxk_product_info_daily}
PARTITION (ds = '${bizdate}')

SELECT
  '${bizdate}'                     as date,
  t1.product_id                    as product_id,
  t11.product_name                 as product_name,
  t11.shop_id                      as shop_id,
  t11.shop_name                    as shop_name,
  t11.product_c1                   as product_c1,
  t11.product_c2                   as product_c2,
  t11.product_c3                   as product_c3,
  t11.product_cname1               as product_cname1,
  t11.product_cname2               as product_cname2,
  t11.product_cname3               as product_cname3,
  t11.shop_type                    as shop_type,
  COALESCE(t1.pay_cnt_30,0)        as pay_cnt_30,
  COALESCE(t1.cck_commission_30,0) as cck_commission_30,
  COALESCE(t1.pay_fee_30,0)        as pay_fee_30,
  COALESCE(t2.pay_cnt,0)           as pay_cnt,
  COALESCE(t2.cck_commission,0)    as cck_commission,
  COALESCE(t2.pay_fee,0)           as pay_fee,
  COALESCE(t3.add_cnt,0)           as add_cnt,
  COALESCE(t3.add_fee,0)           as add_fee,
  COALESCE(t4.ipv_30,0)            as ipv_30,
  COALESCE(t4.ipv,0)               as ipv,
  COALESCE(t5.refund_cnt,0)        as refund_cnt,
  COALESCE(t6.refund_cnt_30,0)     as refund_cnt_30,
  COALESCE(t7.bad_cnt,0)           as bad_cnt,
  COALESCE(t7.eva_rate_cnt,0)      as eva_rate_cnt,
  COALESCE(t7.bad_cnt_30,0)        as bad_cnt_30,
  COALESCE(t7.eva_rate_cnt_30,0)   as eva_rate_cnt_30,
  COALESCE(t8.order_count_ship_success,0)     as order_count_ship_success,
  COALESCE(t8.ship_time_sum,0)                as ship_time_sum,
  COALESCE(t9.order_count_delivery_success,0) as order_count_delivery_success,
  COALESCE(t9.delivery_time_sum,0)            as delivery_time_sum,
  COALESCE(t10.pay_uv_30d,0)       as pay_uv_30d,
  COALESCE(t10.again_pay_uv_30d,0) as again_pay_uv_30d,
  COALESCE(t12.fx_cnt,0)           as fx_cnt,
  COALESCE(t12.fx_user_cnt,0) as fx_user_cnt,
  COALESCE(t2.sale_num,0)          as sale_num,
  COALESCE(t13.used_money,0)       as platform_coupon_fee,
  COALESCE(t14.pb_price,0)         as pb_price,
  COALESCE(t2.pay_fee-t2.commission*0.9-t14.pb_price,0)     as gross_profit
FROM
(
  SELECT
    product_id,
    count(distinct third_tradeno) as pay_cnt_30,
    sum(cck_commission/100) as cck_commission_30,
    sum(item_price/100) as pay_fee_30
  FROM ${hive.databases.ori}.cc_ods_dwxk_wk_sales_deal_ctime
  WHERE ds>='${bizdate-29}' and ds<='${bizdate}'
  GROUP BY product_id
) t1
LEFT JOIN
(
  SELECT
    product_id,
    count(distinct third_tradeno) as pay_cnt,
    sum(sale_num) as sale_num,
    sum(cck_commission/100) as cck_commission,
    sum(item_price/100) as pay_fee,
    sum(commission/100) as commission
  FROM ${hive.databases.ori}.cc_ods_dwxk_wk_sales_deal_ctime
  WHERE ds='${bizdate}'
  GROUP BY product_id
) t2
ON t1.product_id = t2.product_id
LEFT JOIN
(
  SELECT
    product_id,
    count(distinct order_sn) as add_cnt,
    sum(product_discount_price*product_count) as add_fee
  FROM ${hive.databases.ori}.cc_order_products_user_add_time
  WHERE ds='${bizdate}' and !(source_channel&1=0 and source_channel&2=0 and source_channel&4=0)
  GROUP BY product_id
) t3
ON t1.product_id = t3.product_id
LEFT JOIN
(
  select
    s1.product_id as product_id,
    sum(if(s1.ds='${bizdate}',1,0)) as ipv,
    count(1) as ipv_30
  from
  (
    select
      ds,
      product_id
    from ${hive.databases.ori}.cc_ods_log_cctui_product_coupon_detail_hourly
    where ds>= '${bizdate-29}' and ds <= '${bizdate}' and detail_type='item'
    union all
    select
      ds,
      product_id
    from ${hive.databases.ori}.cc_ods_log_gwapp_product_detail_hourly
    where ds>= '${bizdate-29}' and ds <= '${bizdate}'
  ) s1
  group by s1.product_id
) t4
ON t1.product_id = t4.product_id
LEFT JOIN
(
  select
    s2.product_id as product_id,
    count(distinct s1.order_sn) as refund_cnt
  from
  (
    select
      order_sn
    from origin_common.cc_ods_fs_refund_order
    where stop_time>='${bizdate_ts}' and stop_time<'${gmtdate_ts}' and status =1
  ) s1
  inner join
  (
    select
      product_id,
      third_tradeno as order_sn
    FROM ${hive.databases.ori}.cc_ods_dwxk_wk_sales_deal_ctime
    WHERE ds>='${bizdate-30}' and ds<='${bizdate}'
  ) s2
  on s1.order_sn=s2.order_sn
  group by s2.product_id
) t5
ON t1.product_id = t5.product_id
LEFT JOIN
(
  select
    s2.product_id as product_id,
    count(distinct s1.order_sn) as refund_cnt_30
  from
  (
    select
      order_sn
    from origin_common.cc_ods_fs_refund_order
    where stop_time>='${bizdate_ts-29}' and stop_time<'${gmtdate_ts}' and status =1
  ) s1
  inner join
  (
    select
      product_id,
      third_tradeno as order_sn
    FROM ${hive.databases.ori}.cc_ods_dwxk_wk_sales_deal_ctime
    WHERE ds>='${bizdate-60}' and ds<='${bizdate}'
  ) s2
  on s1.order_sn=s2.order_sn
  group by s2.product_id
) t6
ON t1.product_id = t6.product_id
LEFT JOIN
(
  SELECT
    s1.product_id as product_id,
    sum(if(s1.ds='${bizdate}' and s1.star_num=1,1,0)) as bad_cnt,
    sum(if(s1.ds='${bizdate}',1,0))                   as eva_rate_cnt,
    sum(if(s1.star_num=1,1,0))                        as bad_cnt_30,
    count(1)                                          as eva_rate_cnt_30
  FROM
  (
    SELECT
      ds,
      order_sn,
      product_id,
      star_num
    FROM origin_common.cc_rate_star
    WHERE ds>='${bizdate-29}' and ds <= '${bizdate}' and rate_id > 0 and order_sn != '170213194354LFo017564wk'
  ) s1
  INNER JOIN
  (
    SELECT
      distinct third_tradeno as order_sn
    FROM ${hive.databases.ori}.cc_ods_dwxk_wk_sales_deal_ctime
    WHERE ds>='${bizdate-60}' and ds<='${bizdate}'
  ) s2
  ON s1.order_sn=s2.order_sn
  GROUP BY s1.product_id
) t7
ON t1.product_id = t7.product_id
LEFT JOIN
(--30日签收订单数,签收时间
  select
    s2.product_id,
    count(s1.order_sn) as order_count_ship_success,
    sum(s1.ship_time)  as ship_time_sum
  from
  (
    select
      order_sn,
      (update_time - create_time) as ship_time
    from data.cc_cct_product_ship_info
    where ds>='${bizdate-29}' and ds <= '${bizdate}'
  ) s1
  inner join
  (
    select
      distinct product_id,
      third_tradeno as order_sn
    from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where ds>='${bizdate-60}' and ds <= '${bizdate}'
  ) as s2
  on s1.order_sn = s2.order_sn
  group by s2.product_id
) as t8
on t1.product_id = t8.product_id
left join
(-- 30日发货订单数，发货时间
  select
    s2.product_id,
    count(s1.order_sn) as order_count_delivery_success,
    sum(s1.delivery_time - s2.create_time) as delivery_time_sum
  from
    (
    select
      order_sn,
      delivery_time
    from origin_common.cc_order_user_delivery_time
    where ds>='${bizdate-29}' and ds <= '${bizdate}'
  ) as s1
  inner join
  (
    select
      distinct product_id,
      third_tradeno as order_sn,
      create_time
    from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where ds>='${bizdate-40}' and ds <= '${bizdate}'
  ) as s2
  on s1.order_sn = s2.order_sn
  group by s2.product_id
) as t9
on t1.product_id=t9.product_id
left join
(--30日购买用户数，独立购买用户数
  select
    n.product_id  as product_id,
    count(n.user_id) as pay_uv_30d,
    sum(if(n.pay_count>=2,1,0)) as again_pay_uv_30d
  from
  (
    select
      s1.product_id as product_id,
      s2.user_id as user_id,
      count(s1.order_sn) as pay_count
    from
    (
      select distinct
	product_id,
	third_tradeno as order_sn
      from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
      where ds>='${bizdate-29}' and ds <= '${bizdate}' and app_id =2
    ) s1
    inner join
    (
      select
	order_sn,
	user_id
      from origin_common.cc_order_user_pay_time
      where ds>='${bizdate-29}' and ds <= '${bizdate}' and source_channel = 2
    ) s2
    on s1.order_sn = s2.order_sn
    group by s1.product_id,s2.user_id
  ) n
  group by n.product_id
) t10
on t1.product_id = t10.product_id
LEFT JOIN
(
  select
   n1.product_id as product_id,
   n5.cn_title as product_name,
   n7.c1       as product_c1,
   n3.c2       as product_c2,
   n4.c3       as product_c3,
   n7.name as product_cname1,
   n3.name as product_cname2,
   n4.name as product_cname3,
   n1.shop_id as shop_id,
   n6.shop_name as shop_name,
   (case when n1.shop_id in (18164,18335,17801,18532,19141,19268,19405,19347,20471) then '自营'
         when n1.shop_id = 17791 then '京东'
         when n1.shop_id = 18455 then '网易严选'
         when n1.shop_id = 18470 then '冰冰购'
    else 'pop' end) as shop_type
  from
  (
    select
      distinct shop_id as shop_id,
      product_id as product_id,
      cid as category_id
    from origin_common.cc_product
    where ds='${bizdate}'
  ) n1
  left join
  (
    select
      s3.last_cid as last_cid,
      s4.name     as name,
      s3.c2       as c2
    from origin_common.cc_category_cascade s3
    join origin_common.cc_ods_fs_category s4
    on s3.c2 = s4.cid
    where s3.ds='${bizdate}'
  ) n3
  on n1.category_id = n3.last_cid
  left join
  (
    select
      s5.last_cid as last_cid,
      s6.name     as name,
      s5.c3       as c3
    from origin_common.cc_category_cascade s5
    join origin_common.cc_ods_fs_category s6
    on s5.c3 = s6.cid
    where s5.ds='${bizdate}'
  ) n4
  on n1.category_id = n4.last_cid
  left join
  (
    select
      s5.last_cid as last_cid,
      s6.name     as name,
      s5.c1       as c1
    from origin_common.cc_category_cascade s5
    join origin_common.cc_ods_fs_category s6
    on s5.c1 = s6.cid
    where s5.ds='${bizdate}'
  ) n7
  on n1.category_id = n7.last_cid
  left join
  (
    select
      product_id,
      cn_title
    from data.cc_dw_fs_product_max_version
  ) n5
  on n1.product_id = n5.product_id
  left join
  (
    select
      id    as shop_id,
      cn_name  as shop_name
    from origin_common.cc_ods_fs_shop
  ) n6
  on n1.shop_id=n6.shop_id
) t11
on t1.product_id = t11.product_id
LEFT JOIN
(
  select
    m3.product_id as product_id,
    count(m1.user_id) as fx_cnt,
    count(distinct m1.user_id) as fx_user_cnt
  from
  (
    select
      ad_material_id as ad_id,
      user_id
    from origin_common.cc_ods_log_cctapp_click_hourly
    where ds = '${bizdate}' and ad_type in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
    union all
    select
      ad_id,
      user_id
    from origin_common.cc_ods_log_cctapp_click_hourly
    where ds = '${bizdate}' and ad_type not in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
    union all
    select
      s2.ad_id,
      s1.user_id
    from
    (
      select
        ad_material_id,
        user_id
      from origin_common.cc_ods_log_cctapp_click_hourly
      where ds = '${bizdate}' and module='vip' and ad_type in ('single_product','9_cell') and zone in ('material_group-share','material_moments-share')
    ) s1
    inner join
    (
      select
         distinct ad_material_id as ad_material_id,
         ad_id
      from data.cc_dm_gwapp_new_ad_material_relation_hourly
      where ds = '${bizdate}'
    ) s2
    on s1.ad_material_id = s2.ad_material_id
    ) as m1
    inner join
    (
      select
        ad_id,
        item_id
      from origin_common.cc_ods_fs_dwxk_ad_items_daily
    ) m2
    on m1.ad_id = m2.ad_id
    inner join
    (
      select
        item_id,
        app_item_id as product_id
      from origin_common.cc_ods_dwxk_fs_wk_items
    ) m3
    on m3.item_id = m2.item_id
    group by m3.product_id
) t12
on t1.product_id=t12.product_id
LEFT JOIN
(
  select
    v1.product_id as product_id,
    sum(v1.used_money) as used_money
  from
  (
    select
      u1.product_id as product_id,
      u1.order_sn as order_sn,
      u1.use_coupon_rate*u2.used_money as used_money
    from
    (
      select
        s1.product_id as product_id,
        s1.order_sn as order_sn,
        coalesce(s1.add_fee/s2.add_fee,0) as use_coupon_rate
      from
      (
        select
          product_id as product_id,
          order_sn as order_sn,
          sum(product_discount_price*product_count) as add_fee
        from ${hive.databases.ori}.cc_order_products_user_add_time
        where ds='${bizdate}' and !(source_channel&1=0 and source_channel&2=0 and source_channel&4=0)
        group by product_id,order_sn
      ) s1
      inner join
      (
        select
          order_sn as order_sn,
          sum(product_discount_price*product_count) as add_fee
        from ${hive.databases.ori}.cc_order_products_user_add_time
        where ds='${bizdate}' and !(source_channel&1=0 and source_channel&2=0 and source_channel&4=0)
        group by order_sn
      ) s2
      on s1.order_sn=s2.order_sn
    ) u1
    inner join
    (
      select
        s3.order_sn as order_sn,
        sum(s3.used_money) as used_money
      from
      (
        select
          h2.order_sn as order_sn,
          h2.used_money as used_money
        from
        (
          select
            id
          from ${hive.databases.ori}.cc_ods_fs_coupon_temp
          where platform='ccj_cct' and range<>'shop' and shop_id=0
        ) h1
        inner join
        (
          select
            template_id,
            order_sn,
            used_money
          from ${hive.databases.ori}.cc_order_coupon_paytime
          where ds='${bizdate}'
        ) h2
        on h1.id=h2.template_id
      ) s3
      group by order_sn
    ) u2
    on u1.order_sn=u2.order_sn
  ) v1
  group by product_id
) t13
on t1.product_id=t13.product_id
LEFT JOIN
(
    select
      n1.product_id as product_id,
      sum(n2.pb_price*n1.ob_count) as pb_price
    from
    (
      select
        s1.pb_id      as pb_id,
        s1.order_sn   as order_sn,
        s1.product_id as product_id,
        s1.sku_id     as sku_id,
        s1.ob_count   as ob_count
      from
      (
        select
          distinct ob_order_sn as order_sn,
          ob_pid as product_id,
          ob_sku_id as sku_id,
          ob_count as ob_count,
          ob_pb_id as pb_id
        from ${hive.databases.ori}.cc_ods_op_order_batches
        where  ob_ctime>='${bizdate_ts-1}' and ds='${bizdate}'
      ) s1
      inner join
      (
        select
           third_tradeno,
           product_sku_id
        from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where ds='${bizdate}'
      ) s2
      on s1.order_sn=s2.third_tradeno and s1.sku_id=s2.product_sku_id
    ) n1
    inner join
    (
      select
        pb_id,
        pb_price
      from origin_common.cc_ods_fs_op_product_batches
    ) n2
    on n1.pb_id=n2.pb_id
    group by n1.product_id
) t14
on t1.product_id=t14.product_id
