USE ${hive.databases.rpt};

ALTER TABLE ${table_rpt_dwxk_shop_info_daily}
DROP IF EXISTS PARTITION( ds= '${bizdate}');

ALTER TABLE ${table_rpt_dwxk_shop_info_daily}
ADD IF NOT EXISTS PARTITION (ds = '${bizdate}')
LOCATION '${bizdate}';

INSERT OVERWRITE TABLE ${hive.databases.rpt}.${table_rpt_dwxk_shop_info_daily}
PARTITION (ds = '${bizdate}')

SELECT 
  '${bizdate}'                     as date,
  t1.shop_id                       as shop_id,
  t1.shop_name                     as shop_name,
  t2.cname1                        as shop_cname1,
  t2.cname2                        as shop_cname2,
  t1.shop_type                     as shop_type,
  COALESCE(t4.pay_cnt_30,0)        as pay_cnt_30,
  COALESCE(t1.cck_commission_30,0) as cck_commission_30,
  COALESCE(t1.pay_fee_30,0)        as pay_fee_30,
  COALESCE(t5.pay_cnt,0)           as pay_cnt,
  COALESCE(t1.cck_commission,0)    as cck_commission,
  COALESCE(t1.pay_fee,0)           as pay_fee,
  COALESCE(t6.add_cnt,0)           as add_cnt,
  COALESCE(t1.add_fee,0)           as add_fee,
  COALESCE(t1.ipv_30,0)            as ipv_30,
  COALESCE(t1.ipv,0)               as ipv,
  COALESCE(t7.refund_cnt,0)        as refund_cnt,
  COALESCE(t8.refund_cnt_30,0)     as refund_cnt_30,
  COALESCE(t1.bad_cnt,0)           as bad_cnt,
  COALESCE(t1.eva_rate_cnt,0)      as eva_rate_cnt,
  COALESCE(t1.bad_cnt_30,0)        as bad_cnt_30,
  COALESCE(t1.eva_rate_cnt_30,0)   as eva_rate_cnt_30,
  COALESCE(t9.order_count_ship_success,0)     as order_count_ship_success,
  COALESCE(t9.ship_time_sum,0)                as ship_time_sum,
  COALESCE(t10.order_count_delivery_success,0) as order_count_delivery_success,
  COALESCE(t10.delivery_time_sum,0)            as delivery_time_sum,
  COALESCE(t11.pay_uv_30d,0)                   as pay_uv_30d,
  COALESCE(t11.again_pay_uv_30d,0)             as again_pay_uv_30d,
  COALESCE(t1.fx_cnt,0)                       as fx_cnt,
  COALESCE(t1.saled_prd_cnt,0)                as saled_prd_cnt,
  COALESCE(t3.online_prd_cnt,0)               as online_prd_cnt,
  COALESCE(t3.new_prd_cnt,0)                  as new_prd_cnt,
  COALESCE(t7.refund_time,0)                  as refund_time,
  COALESCE(t12.elapsed_time,0)                as elapsed_time,
  COALESCE(t12.task_cnt,0)                    as task_cnt
FROM
(
  SELECT
    shop_id,
    max(shop_name)                 as shop_name,
    max(shop_type)                 as shop_type,
    sum(cck_commission_30)         as cck_commission_30,
    sum(pay_fee_30)                as pay_fee_30,
    sum(cck_commission)            as cck_commission,
    sum(pay_fee)                   as pay_fee,
    sum(add_fee)                   as add_fee,
    sum(ipv_30)                    as ipv_30,
    sum(ipv)                       as ipv,
    sum(bad_cnt)                   as bad_cnt,
    sum(eva_rate_cnt)              as eva_rate_cnt,
    sum(bad_cnt_30)                as bad_cnt_30,
    sum(eva_rate_cnt_30)           as eva_rate_cnt_30,
    sum(fx_cnt)                       as fx_cnt,
    sum(if(pay_cnt>0,1,0))            as saled_prd_cnt
  FROM report.cc_rpt_dwxk_product_info_daily
  WHERE ds='${bizdate}'
  GROUP BY shop_id
) t1
LEFT JOIN
(
  SELECT
    shop_id,
    cname1, 
    cname2
  FROM data.cc_mid_shop_cname
  WHERE ds='${bizdate}'
) t2
ON t1.shop_id = t2.shop_id
LEFT JOIN
(
    SELECT
        app_shop_id as shop_id,
        count(distinct ad_id) as online_prd_cnt,
        sum(if(start_time>='${bizdate_ts}' and start_time<'${gmtdate_ts}',1,0)) as new_prd_cnt
    FROM 
        origin_common.cc_ods_fs_dwxk_ad_items_daily
    WHERE 
        audit_status=1 
    and 
        status>0 
    and 
        start_time<'${gmtdate_ts}' 
    and 
        end_time>='${bizdate_ts}'
    GROUP BY 
        app_shop_id
) t3
ON t1.shop_id = t3.shop_id
LEFT JOIN
(
  SELECT
    s2.shop_id as shop_id,
    count(distinct s1.third_tradeno) as pay_cnt_30
  FROM
  ( 
    SELECT
      product_id,
      third_tradeno
    FROM ${hive.databases.ori}.cc_ods_dwxk_wk_sales_deal_ctime
    WHERE ds>='${bizdate-29}' and ds<='${bizdate}'
  ) s1
  inner join
  (
    SELECT
      app_item_id as product_id,
      shop_id
    FROM ${hive.databases.ori}.cc_ods_dwxk_fs_wk_items
  ) s2
  on s1.product_id=s2.product_id
  GROUP BY s2.shop_id
) t4
on t1.shop_id = t4.shop_id
LEFT JOIN
(
  SELECT
    s2.shop_id as shop_id,
    count(distinct s1.third_tradeno) as pay_cnt
  FROM
  (
    SELECT
      product_id,
      third_tradeno
    FROM ${hive.databases.ori}.cc_ods_dwxk_wk_sales_deal_ctime
    WHERE ds='${bizdate}'
  ) s1
  inner join
  (
    SELECT
      app_item_id as product_id,
      shop_id
    FROM ${hive.databases.ori}.cc_ods_dwxk_fs_wk_items
  ) s2
  on s1.product_id=s2.product_id
  GROUP BY s2.shop_id 
) t5
ON t1.shop_id = t5.shop_id
LEFT JOIN
( 
  SELECT
    s2.shop_id as shop_id,
    count(distinct s1.order_sn) as add_cnt
  FROM
  (
    SELECT
      distinct product_id,
      order_sn
    FROM ${hive.databases.ori}.cc_order_products_user_add_time
    WHERE ds='${bizdate}' and !(source_channel&1=0 and source_channel&2=0 and source_channel&4=0)
  ) s1
  inner join
  (
    SELECT
      app_item_id as product_id,
      shop_id
    FROM ${hive.databases.ori}.cc_ods_dwxk_fs_wk_items
  ) s2
  on s1.product_id=s2.product_id
  GROUP BY s2.shop_id
) t6
ON t1.shop_id = t6.shop_id
LEFT JOIN
(
  select
    s2.shop_id as shop_id,
    count(distinct s1.order_sn) as refund_cnt,
    sum(s1.stop_time-s1.create_time) as refund_time
  from
  (
    select
      order_sn,
      create_time,
      stop_time
    from origin_common.cc_ods_fs_refund_order
    where stop_time>='${bizdate_ts}' and stop_time<'${gmtdate_ts}' and status =1
  ) s1
  inner join
  (
    select
      order_sn,
      shop_id
    FROM ${hive.databases.ori}.cc_order_user_pay_time
    WHERE ds>='${bizdate-30}' and ds<='${bizdate}' and source_channel=2
  ) s2
  on s1.order_sn=s2.order_sn
  group by s2.shop_id
) t7
ON t1.shop_id = t7.shop_id
LEFT JOIN
(
  select
    s2.shop_id as shop_id,
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
      order_sn,
      shop_id
    FROM ${hive.databases.ori}.cc_order_user_pay_time
    WHERE ds>='${bizdate-60}' and ds<='${bizdate}' and source_channel=2
  ) s2
  on s1.order_sn=s2.order_sn
  group by s2.shop_id
) t8
ON t1.shop_id = t8.shop_id
LEFT JOIN
(--30日签收订单数,签收时间
  select
    s2.shop_id as shop_id,
    count(s1.order_sn) as order_count_ship_success,
    sum(s1.ship_time)  as ship_time_sum
  from
  (
    select
      distinct order_sn as order_sn,
      (update_time - create_time) as ship_time
    from data.cc_cct_product_ship_info
    where ds>='${bizdate-29}' and ds <= '${bizdate}'
  ) s1
  inner join
  (
    select
      order_sn,
      shop_id
    from origin_common.cc_order_user_pay_time
    where ds>='${bizdate-60}' and ds <= '${bizdate}' and source_channel=2
  ) as s2
  on s1.order_sn = s2.order_sn
  group by s2.shop_id
) as t9
on t1.shop_id = t9.shop_id
left join
(-- 30日发货订单数，发货时间
  select
    s1.shop_id,
    count(s1.order_sn) as order_count_delivery_success,
    sum(s1.delivery_time - s2.create_time) as delivery_time_sum
  from
    (
    select
      order_sn,
      shop_id,
      delivery_time
    from origin_common.cc_order_user_delivery_time
    where ds>='${bizdate-29}' and ds <= '${bizdate}'
  ) as s1
  inner join
  (
    select
      distinct third_tradeno as order_sn,
      create_time
    from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where ds>='${bizdate-40}' and ds <= '${bizdate}'
  ) as s2
  on s1.order_sn = s2.order_sn
  group by s1.shop_id
) as t10
on t1.shop_id=t10.shop_id
left join
(--30日购买用户数，独立购买用户数
  select
    n.shop_id  as shop_id,
    count(n.user_id) as pay_uv_30d,
    sum(if(n.pay_count>=2,1,0)) as again_pay_uv_30d
  from
  (
    select
      s2.shop_id as shop_id,
      s2.user_id as user_id,
      count(s1.order_sn) as pay_count
    from
    (
      select 
        distinct third_tradeno as order_sn
      from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
      where ds>='${bizdate-29}' and ds <= '${bizdate}' and app_id =2
    ) s1
    inner join
    (
      select
        order_sn,
        user_id,
        shop_id
      from origin_common.cc_order_user_pay_time
      where ds>='${bizdate-29}' and ds <= '${bizdate}' and source_channel = 2
    ) s2
    on s1.order_sn = s2.order_sn
    group by s2.shop_id,s2.user_id
  ) n
  group by n.shop_id
) t11
on t1.shop_id = t11.shop_id
LEFT JOIN
(
  select
    s1.shop_id as shop_id,
    sum(s2.elapsed_time) as elapsed_time,
    count(distinct s1.id) as task_cnt
  from
  (
    select
      id,
      shop_id
    from origin_common.cc_ods_fs_task
    where closed_time>='${bizdate_ts}' and closed_time<'${gmtdate_ts}' and status =2
  ) s1
  inner join
  (
    select
      task_id,
      elapsed_time
    FROM ${hive.databases.ori}.cc_ods_fs_action_log
  ) s2
  on s1.id=s2.task_id
  group by s1.shop_id
) t12
ON t1.shop_id = t12.shop_id


