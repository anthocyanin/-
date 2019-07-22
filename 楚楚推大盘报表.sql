SELECT
  t1.date                       as date,
  t1.cck_commission             as cck_commission,
  t1.pay_fee                    as pay_fee,
  t1.order_cnt                  as pay_cnt,
  t3.inner_cck_commission       as inner_cck_commission,
  t3.inner_pay_fee              as inner_pay_fee,
  t3.inner_order_cnt            as inner_order_cnt,
  t1.cck_rate                   as cck_rate,
  t1.saled_cck_cnt              as saled_cck_cnt,
  t1.saled_prd_cnt              as saled_prd_cnt,
  t2.total_fee                  as total_fee,
  t3.inner_total_fee            as inner_total_fee,
  t2.saled_shop_cnt             as saled_shop_cnt,
  t2.pay_user_cnt               as pay_user_cnt,
  t4.total_add_fee              as total_add_fee,
  t4.order_add_cnt              as order_add_cnt,
  t5.inner_total_fee            as inner_total_add_fee,
  t5.inner_order_add_cnt        as inner_order_add_cnt,
  t4.add_user_cnt               as add_user_cnt,
  t3.inner_vip_cck_commission   as inner_vip_cck_commission,
  t3.inner_vip_pay_fee          as inner_vip_pay_fee,
  t3.inner_vip_order_cnt        as inner_vip_order_cnt,
  t3.inner_vip_total_fee        as inner_vip_total_fee,
  t5.inner_vip_total_fee        as inner_vip_total_add_fee,
  t5.inner_vip_order_add_cnt    as inner_vip_order_add_cnt,
  t6.inner_detail_pv            as inner_detail_pv,
  t6.out_detail_pv              as out_detail_pv,
  t7.fx_prd_cnt                 as fx_prd_cnt,
  t7.fx_cck_cnt                 as fx_cck_cnt,
  t7.fx_cnt                     as fx_cnt,
  t8.inner_vip_pay_cck_cnt      as inner_vip_pay_cck_cnt,
  t9.out_vip_pay_cck_cnt        as out_vip_pay_cck_cnt,
  t10.inner_p_vip_pay_cck_cnt   as inner_p_vip_pay_cck_cnt,
  t4.total_all_add_fee          as total_all_add_fee,
  t11.used_money                as used_money,
  t11.cct_used_money            as cct_used_money,
  t1.cct_commission             as cct_commission,
  t1.cct_ori_fee                as cct_ori_fee,
  t1.cct_pay_fee                as cct_pay_fee,
  t1.cct_order_cnt              as cct_order_cnt,
  t12.cnt_num                   as fx_cnt_new,
  t12.cnt_user                  as fx_user_cnt_new
FROM
(
  select
    '${bizdate}'                                            as date,--日期
    cast(sum(h1.cck_commission/100) as decimal(20,2))       as cck_commission,--楚客直接佣金
    cast(sum(h1.item_price/100) as decimal(20,2))           as pay_fee,--支付金额
    count(distinct h1.third_tradeno)                        as order_cnt,--订单数
    count(distinct h1.cck_uid)                              as saled_cck_cnt,--有成交楚客数
    count(distinct h1.product_id)                           as saled_prd_cnt,--有成交商品数
    cast(avg(h1.cck_rate/10) as decimal(20,2))              as cck_rate,--平均楚客佣金率
    cast(sum(if(h3.order_sn is not null,h1.commission,0)/100) as decimal(20,2))           as cct_commission,--自营佣金额
    cast(sum(if(h3.order_sn is not null,h3.pb_price,0)) as decimal(20,2))                as cct_ori_fee,--自营供货价
    cast(sum(if(h3.order_sn is not null,h1.item_price,0)/100) as decimal(20,2))          as cct_pay_fee,--自营支付金额
    count(distinct if(h3.order_sn is not null,h1.third_tradeno,null))                     as cct_order_cnt--自营订单数
  from
  (
    select
      cck_uid,
      third_tradeno,
      product_sku_id,
      product_id,
      item_price,
      cck_rate,
      cck_commission,
      commission,
      sale_num
    from ${hive.databases.ori}.cc_ods_dwxk_wk_sales_deal_ctime
    where ds='${bizdate}'
  ) h1
  inner join
  (
    select
      cck_uid
    from ${hive.databases.ori}.cc_ods_dwxk_fs_wk_cck_user
    where ds='${bizdate}' and platform=14
  ) h2
  on h1.cck_uid=h2.cck_uid
  left join
  (
    select
      s1.order_sn as order_sn,
      s1.sku_id   as sku_id,
      sum(s2.pb_price*s1.ob_count) as pb_price
    from
    (
      select
        distinct 
        ob_pb_id as ob_pb_id,
        ob_order_sn as order_sn,
        ob_sku_id as sku_id,
        ob_count
      from origin_common.cc_ods_op_order_batches
      where ds='${bizdate}' and ob_ctime>='${bizdate_ts-1}'
    ) s1
    inner join
    (
      select
        pb_id,
        pb_price
      from origin_common.cc_ods_fs_op_product_batches
    ) s2
    on s1.ob_pb_id=s2.pb_id
    group by s1.order_sn,s1.sku_id
  ) h3
  on h1.third_tradeno=h3.order_sn and h1.product_sku_id=h3.sku_id
) t1
LEFT JOIN
(
  select
    '${bizdate}'                              as date,
    count(distinct g1.shop_id)                as saled_shop_cnt,--有成交店铺数
    count(distinct g1.user_id)                as pay_user_cnt,--支付用户数
    cast(sum(g1.total_fee) as decimal(20,2))  as total_fee--
  from
  (
    select
      order_sn,
      user_id,
      shop_id,
      total_fee
    from ${hive.databases.ori}.cc_order_user_pay_time
    where ds='${bizdate}'
  ) g1
  inner join
  (
    select
      distinct order_sn as order_sn,
      split(order_trace,':')[1] as cck_uid
    from ${hive.databases.ori}.cc_order_products_user_add_time
    where ds='${bizdate}' and order_trace!=''
  ) g2
  on g1.order_sn=g2.order_sn
  inner join
  (
    select
      cck_uid
    from ${hive.databases.ori}.cc_ods_dwxk_fs_wk_cck_user
    where ds='${bizdate}' and platform=14
  ) g3
  on g2.cck_uid=g3.cck_uid
) t2
ON t1.date=t2.date
LEFT JOIN
(
  select
    '${bizdate}'                                                                                             as date,
    cast(sum(if(s3.source='cctui',s1.cck_commission/100,0)) as decimal(20,2))                                as inner_cck_commission,
    cast(sum(if(s3.source='cctui',s1.item_price/100,0)) as decimal(20,2))                                    as inner_pay_fee,
    sum(if(s3.source='cctui',1,0))                                                                           as inner_order_cnt,
    cast(sum(if(s3.source='cctui' and s2.cck_vip_status=1,s1.cck_commission/100,0)) as decimal(20,2))        as inner_vip_cck_commission,
    cast(sum(if(s3.source='cctui' and s2.cck_vip_status=1,s1.item_price/100,0)) as decimal(20,2))            as inner_vip_pay_fee,
    sum(if(s3.source='cctui' and s2.cck_vip_status=1,1,0))                                                   as inner_vip_order_cnt,
    cast(sum(if(s3.source='cctui',s2.total_fee,0)) as decimal(20,2))                                         as inner_total_fee,
    cast(sum(if(s3.source='cctui' and s2.cck_vip_status=1,s2.total_fee,0)) as decimal(20,2))                 as inner_vip_total_fee
  from
  (
    select
      h1.third_tradeno  as third_tradeno,
      h1.item_price     as item_price,
      h1.cck_commission as cck_commission
    from
    (
      select
        cck_uid,
        third_tradeno,
        sum(item_price) as item_price,
        sum(cck_commission) as cck_commission
      from ${hive.databases.ori}.cc_ods_dwxk_wk_sales_deal_ctime
      where ds='${bizdate}'
      group by cck_uid,third_tradeno
    ) h1
    inner join
    (
      select
        cck_uid
      from ${hive.databases.ori}.cc_ods_dwxk_fs_wk_cck_user
      where ds='${bizdate}' and platform=14
    ) h2
    on h1.cck_uid=h2.cck_uid
  ) s1
  left join
  (
    select
      distinct order_sn as order_sn,
      source
    from ${hive.databases.ori}.cc_ods_log_gwapp_order_track_hourly
    where ds>='${bizdate-2}' and ds<='${bizdate}'
  ) s3
  on s1.third_tradeno=s3.order_sn
  left join
  (
    select
      e1.order_sn as order_sn,
      e1.user_id  as user_id,
      e1.shop_id  as shop_id,
      e1.total_fee as total_fee,
      e2.cck_vip_status as cck_vip_status
    from
    (
      select
        order_sn,
        user_id,
        shop_id,
        total_fee
      from ${hive.databases.ori}.cc_order_user_pay_time
      where ds='${bizdate}'
    ) e1
    left join
    (
      select
        cct_uid,
        cck_uid,
        cck_vip_status
      from ${hive.databases.ori}.cc_ods_fs_tui_relation
    ) e2
    on e1.user_id=e2.cct_uid
  ) s2
  on s1.third_tradeno=s2.order_sn
) t3
ON t1.date=t3.date
LEFT JOIN
(
  select
    '${bizdate}'                as date,
    count(distinct g1.order_sn) as order_add_cnt,
    count(distinct g1.user_id)  as add_user_cnt,
    sum(g2.total_fee)           as total_add_fee,
    sum(g2.total_all_fee)       as total_all_add_fee
  from
  (
    select
      order_sn,
      user_id
    from ${hive.databases.ori}.cc_order_user_add_time
    where ds='${bizdate}'
  ) g1
  inner join
  (
    select
      order_sn,
      split(order_trace,':')[1] as cck_uid,
      sum(product_count*product_discount_price) as total_fee,
      sum(product_count*product_price) as total_all_fee
    from ${hive.databases.ori}.cc_order_products_user_add_time
    where ds='${bizdate}' and order_trace!=''
    group by order_sn,split(order_trace,':')[1]
  ) g2
  on g1.order_sn=g2.order_sn
  inner join
  (
    select
      cck_uid
    from ${hive.databases.ori}.cc_ods_dwxk_fs_wk_cck_user
    where ds='${bizdate}' and platform=14
  ) g3
  on g2.cck_uid=g3.cck_uid
) t4
ON t1.date=t4.date
LEFT JOIN
(
  select
    '${bizdate}'                                                                                    as date,
    cast(sum(if(s2.source='cctui',s1.total_fee,0)) as decimal(20,2))                                as inner_total_fee,
    sum(if(s2.source='cctui',1,0))                                                                  as inner_order_add_cnt,
    cast(sum(if(s2.source='cctui' and s3.cck_vip_status=1,s1.total_fee,0)) as decimal(20,2))         as inner_vip_total_fee,
    sum(if(s2.source='cctui' and s3.cck_vip_status=1,1,0))                                           as inner_vip_order_add_cnt
  from
  (
    select
      g1.order_sn as order_sn,
      g1.user_id  as user_id,
      g2.total_fee as total_fee
    from
    (
      select
        order_sn,
        user_id
      from ${hive.databases.ori}.cc_order_user_add_time
      where ds='${bizdate}' and source_channel=2
    ) g1
    inner join
    (
      select
        order_sn,
        split(order_trace,':')[1] as cck_uid,
        sum(product_count*product_discount_price) as total_fee
      from ${hive.databases.ori}.cc_order_products_user_add_time
      where ds='${bizdate}' and order_trace!=''
      group by order_sn,split(order_trace,':')[1]
    ) g2
    on g1.order_sn=g2.order_sn
    inner join
    (
      select
        cck_uid
      from ${hive.databases.ori}.cc_ods_dwxk_fs_wk_cck_user
      where ds='${bizdate}' and platform=14
    ) g3
    on g2.cck_uid=g3.cck_uid
  ) s1
  left join
  (
    select
      distinct order_sn as order_sn,
      source
    from ${hive.databases.ori}.cc_ods_log_gwapp_order_track_hourly
    where ds='${bizdate}'
  ) s2
  on s1.order_sn=s2.order_sn
  left join
  (
    select
      cct_uid,
      cck_uid,
      cck_vip_status
    from ${hive.databases.ori}.cc_ods_fs_tui_relation
  ) s3
  on s1.user_id=s3.cct_uid
) t5
ON t1.date=t5.date
LEFT JOIN
(
  select
    '${bizdate}'                 as date,
    sum(if(is_in_app=1,1,0))     as inner_detail_pv,
    sum(if(is_in_app=0,1,0))     as out_detail_pv
  from ${hive.databases.ori}.cc_ods_log_cctui_product_coupon_detail_hourly
  where ds='${bizdate}' and detail_type='item'
) t6
ON t1.date=t6.date
LEFT JOIN
(
  select
    '${bizdate}'                 as date,
    count(distinct s1.ad_id)     as fx_prd_cnt,
    count(distinct s1.user_id)   as fx_cck_cnt,
    count(1)                     as fx_cnt
  from
  (
    select
      user_id,
      ad_id
    from ${hive.databases.ori}.cc_ods_log_cctapp_click_hourly
    where ds='${bizdate}' and  ((module='vip' and (zone='material_group-massproduct' or zone='material_group-share')) or (module='detail' and zone='spread') or (module='detail_app' and zone='spread'))
  ) s1
  inner join
  (
    select
      distinct h2.cct_uid as cct_uid
    from
    (
      select
        cck_uid
      from ${hive.databases.ori}.cc_ods_dwxk_fs_wk_cck_user
      where ds='${bizdate}' and platform=14
    ) h1
    inner join
    (
      select
        guider_uid,
        cct_uid
      from ${hive.databases.ori}.cc_ods_fs_tui_relation
    ) h2
    on h1.cck_uid=h2.guider_uid
  ) s2
  on s1.user_id=s2.cct_uid
) t7
ON t1.date=t7.date
LEFT JOIN
(
  select
    '${bizdate}'                   as date,
    count(distinct s1.cck_uid)     as inner_vip_pay_cck_cnt
  from
  (
    select
      h1.cck_uid        as cck_uid,
      h1.uid            as user_id,
      h1.order_sn       as order_sn
    from
    (
      select
        distinct cck_uid as cck_uid,
        uid,
        third_tradeno    as order_sn
      from ${hive.databases.ori}.cc_ods_dwxk_wk_sales_deal_ctime
      where ds='${bizdate}'
    ) h1
    inner join
    (
      select
        cck_uid
      from ${hive.databases.ori}.cc_ods_dwxk_fs_wk_cck_user
      where ds='${bizdate}' and platform=14
    ) h2
    on h1.cck_uid=h2.cck_uid
  ) s1
  inner join
  (
    select
      distinct order_sn as order_sn,
      source
    from ${hive.databases.ori}.cc_ods_log_gwapp_order_track_hourly
    where ds>='${bizdate-2}' and ds<='${bizdate}' and source='cctui'
  ) s3
  on s1.order_sn=s3.order_sn
  inner join
  (
    select
      cct_uid,
      cck_uid,
      cck_vip_status
    from ${hive.databases.ori}.cc_ods_fs_tui_relation
    where cck_vip_status=1
  ) s2
  on s1.user_id=s2.cct_uid and s1.cck_uid=s2.cck_uid
) t8
ON t1.date=t8.date
LEFT JOIN
(
  select
    '${bizdate}'                   as date,
    count(distinct s1.cck_uid)        as out_vip_pay_cck_cnt
  from
  (
    select
      h1.cck_uid        as cck_uid,
      h1.uid            as user_id,
      h1.order_sn       as order_sn
    from
    (
      select
        distinct cck_uid as cck_uid,
        uid,
        third_tradeno    as order_sn
      from ${hive.databases.ori}.cc_ods_dwxk_wk_sales_deal_ctime
      where ds='${bizdate}'
    ) h1
    inner join
    (
      select
        cck_uid
      from ${hive.databases.ori}.cc_ods_dwxk_fs_wk_cck_user
      where ds='${bizdate}' and platform=14
    ) h2
    on h1.cck_uid=h2.cck_uid
  ) s1
  inner join
  (
    select
      distinct order_sn as order_sn,
      source
    from ${hive.databases.ori}.cc_ods_log_gwapp_order_track_hourly
    where ds>='${bizdate-2}' and ds<='${bizdate}' and source!='cctui'
  ) s3
  on s1.order_sn=s3.order_sn
) t9
ON t1.date=t9.date
LEFT JOIN
(
  select
    '${bizdate}'                   as date,
    count(distinct s1.user_id)     as inner_p_vip_pay_cck_cnt
  from
  (
    select
      h1.cck_uid        as cck_uid,
      h1.uid            as user_id,
      h1.order_sn       as order_sn
    from
    (
      select
        distinct cck_uid as cck_uid,
        uid,
        third_tradeno    as order_sn
      from ${hive.databases.ori}.cc_ods_dwxk_wk_sales_deal_ctime
      where ds='${bizdate}'
    ) h1
    inner join
    (
      select
        cck_uid
      from ${hive.databases.ori}.cc_ods_dwxk_fs_wk_cck_user
      where ds='${bizdate}' and platform=14
    ) h2
    on h1.cck_uid=h2.cck_uid
  ) s1
  inner join
  (
    select
      distinct order_sn as order_sn,
      source
    from ${hive.databases.ori}.cc_ods_log_gwapp_order_track_hourly
    where ds>='${bizdate-2}' and ds<='${bizdate}' and source='cctui'
  ) s3
  on s1.order_sn=s3.order_sn
  inner join
  (
    select
      cct_uid,
      cck_uid,
      cck_vip_status
    from ${hive.databases.ori}.cc_ods_fs_tui_relation
    where cck_vip_status=0
  ) s2
  on s1.user_id=s2.cct_uid
) t10
ON t1.date=t10.date
LEFT JOIN
(
  select
    '${bizdate}'       as date,
    sum(s2.used_money) as used_money,
    sum(if(s1.is_cct=1,s2.used_money,0)) as cct_used_money
  from
  (
    select
      distinct 
      h1.order_sn as order_sn,
      if(h3.product_id is not null,1,0) as is_cct
    from
    (
      select
        cck_uid,
        third_tradeno as order_sn,
        product_id
      from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
      where ds='${bizdate}'
    ) h1
    inner join
    (
      select
        cck_uid
      from origin_common.cc_ods_dwxk_fs_wk_cck_user
      where ds='${bizdate}' and platform=14
    ) h2
    on h1.cck_uid=h2.cck_uid
    left join
    (
      select
        pm_pid as product_id
      from origin_common.cc_ods_op_products_map
      where ds='${bizdate}'
    ) h3
    on h1.product_id=h3.product_id
  ) s1
  inner join
  (
    select
      h2.order_sn as order_sn,
      h2.used_money as used_money
    from
    (
      select
        id
      from origin_common.cc_ods_fs_coupon_temp
      where platform='ccj_cct' and range!='shop' and shop_id=0
    ) h1
    inner join
    (
      select
        template_id,
        order_sn,
        used_money
      from origin_common.cc_order_coupon_paytime
      where ds='${bizdate}'
    ) h2
    on h1.id=h2.template_id
  ) s2
  on s1.order_sn=s2.order_sn
) t11
ON t1.date=t11.date
left join
(
    --µ±ÈÕ·ÖÏí´ÎÊý + ·ÖÏíÈËÊý
    select
        '${bizdate}' as date,
        count(distinct a.user_id) as cnt_user,
        count(a.user_id) as cnt_num
    from
    (
        select
            user_id
        from origin_common.cc_ods_log_cctapp_click_hourly
        where ds = '${bizdate}' and module = 'index_new_share' and zone in ('goods_friend', 'goods_circlefriend')
        union all
        select
            user_id
        from origin_common.cc_ods_log_gwapp_click_hourly
        where ds = '${bizdate}' and module = 'index_share_moments' and zone != 'default'
        union all
        select
            user_id
        from origin_common.cc_ods_log_gwapp_click_hourly
        where ds = '${bizdate}' and module = 'afp' and zone in ('footersharecctafp','headsharecctafp')
        union all
        select
            user_id
        from origin_common.cc_ods_log_cctapp_click_hourly
        where ds = '${bizdate}' and module='detail_material' and zone in ('line','small_routine','pQrCode','promotion')
    )a
    join
    (
        select
            cck_uid,cct_uid
        from origin_common.cc_ods_dwxk_fs_wk_cck_user where ds='${bizdate}' and platform=14
    )b
    on a.user_id = b.cct_uid
)t12
ON t1.date=t12.date
