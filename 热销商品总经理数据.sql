INSERT OVERWRITE TABLE report.cc_rpt_cctui_gm_bkmsqg_report
PARTITION (ds = '${bizdate}')
SELECT
  '${bizdate}'                                as date,
  t1.cck_uid                                as cck_uid,
  t1.real_name                              as real_name,
  t1.phone                                  as phone,
  t1.all_count                              as all_count,
  COALESCE(t1.team_leader_cnt,0)            as team_leader_cnt,
  COALESCE(t2.team_gmv,0)                   as team_bk_gmv,
  0                                         as team_ms_gmv,
  COALESCE(t3.team_gmv,0)                   as team_qg_gmv,
  COALESCE(t5.team_out_ipv,0)               as team_bk_out_ipv,
  0                                         as team_ms_out_ipv,
  COALESCE(t7.team_out_ipv,0)               as team_qg_out_ipv,
  COALESCE(t8.team_fx_cnt,0)                as team_bk_fx_cnt,
  COALESCE(t8.team_fx_user_cnt,0)           as team_bk_fx_user_cnt,
  0                                         as team_ms_fx_cnt,
  0                                         as team_ms_fx_user_cnt,
  COALESCE(t10.team_fx_cnt,0)               as team_qg_fx_cnt,
  COALESCE(t10.team_fx_user_cnt,0)          as team_qg_fx_user_cnt,
  COALESCE(t11.team_self_gmv,0)             as team_bk_self_gmv,
  COALESCE(t12.team_self_gmv,0)             as team_ms_self_gmv,
  COALESCE(t13.team_self_gmv,0)             as team_qg_self_gmv,
  COALESCE(t14.team_gm_self_gmv,0)          as team_bk_gm_self_gmv,
  0                                         as team_ms_gm_self_gmv,
  COALESCE(t16.team_gm_self_gmv,0)          as team_qg_gm_self_gmv,
  COALESCE(t17.self_fx_cnt,0)               as self_bk_fx_cnt,
  0                                         as self_ms_fx_cnt,
  COALESCE(t19.self_fx_cnt,0)               as self_qg_fx_cnt,
  COALESCE(t2.team_gmv2,0)                  as team_bk_gmv2,
  COALESCE(t5.team_out_ipv2,0)              as team_bk_out_ipv2,
  COALESCE(t8.team_fx_cnt2,0)               as team_bk_fx_cnt2,
  COALESCE(t8.team_fx_user_cnt2,0)          as team_bk_fx_user_cnt2,
  COALESCE(t11.team_self_gmv2,0)            as team_bk_self_gmv2,
  COALESCE(t14.team_gm_self_gmv2,0)         as team_bk_gm_self_gmv2,
  COALESCE(t17.self_fx_cnt2,0)              as self_bk_fx_cnt2,
  --爆款分享afp统计 by wangfan
  COALESCE(t20.team_fx_cnt,0)               as team_bk_fx_afp_cnt,
  COALESCE(t20.team_fx_user_cnt,0)          as team_bk_fx_user_afp_cnt,
  COALESCE(t20.team_fx_cnt2,0)              as team_bk_fx_afp_cnt2,
  COALESCE(t20.team_fx_user_cnt2,0)         as team_bk_fx_user_afp_cnt2,
  --爆款分享场次分享统计  by wangfan
  0                                         as team_bk_fx_cc_cnt,
  0                                         as team_bk_fx_user_cc_cnt,
  0                                         as team_bk_fx_cc_cnt2,
  0                                         as team_bk_fx_user_cc_cnt2,
  --爆款分享微信裂变统计  by wangfan
  COALESCE(t22.team_fx_cnt,0)               as team_bk_fx_wx_cnt,
  COALESCE(t22.team_fx_user_cnt,0)          as team_bk_fx_user_wx_cnt,
  COALESCE(t22.team_fx_cnt2,0)              as team_bk_fx_wx_cnt2,
  COALESCE(t22.team_fx_user_cnt2,0)         as team_bk_fx_user_wx_cnt2,
  --抢购afp，场次分享，微信裂变统计 by wangfan
  COALESCE(t23.team_fx_cnt,0)               as team_qg_fx_afp_cnt,
  COALESCE(t23.team_fx_user_cnt,0)          as team_qg_fx_user_afp_cnt,
  COALESCE(t24.team_fx_cnt,0)               as team_qg_fx_wx_cnt,
  COALESCE(t24.team_fx_user_cnt,0)          as team_qg_fx_user_wx_cnt,
  COALESCE(t25.team_fx_cnt,0)               as team_qg_fx_cc_cnt,
  COALESCE(t25.team_fx_user_cnt,0)          as team_qg_fx_user_cc_cnt

FROM
(
  select
    s1.cck_uid                  as cck_uid,
    s3.real_name                as real_name,
    s3.phone                    as phone,
    s1.all_count                as all_count,
    s2.team_leader_cnt          as team_leader_cnt
  from
  (
    select
      cck_uid,
      all_count
    from origin_common.cc_ods_fs_wk_cct_layer_info
    where platform=14 and type=2 and is_del=0
  ) s1
  left join
  (
    select
      gm_uid,
      count(cck_uid) as team_leader_cnt
    from origin_common.cc_ods_fs_wk_cct_layer_info
    where platform=14 and type=1 and is_del=0
    group by gm_uid
  ) s2
  on s1.cck_uid=s2.gm_uid
  left join
  (
    select
      cck_uid,
      real_name,
      phone
    from origin_common.cc_ods_dwxk_fs_wk_business_info
    where ds='${bizdate}'
  ) s3
  on s1.cck_uid=s3.cck_uid
) t1
LEFT JOIN --爆款销售额
(
  select
    g1.gm_uid    as cck_uid,
    cast(sum(if(g2.bomb_type=1,g2.item_price,0))/100 as decimal(20,2)) as team_gmv,
    cast(sum(if(g2.bomb_type=2,g2.item_price,0))/100 as decimal(20,2)) as team_gmv2
  from
  (
    select
      gm_uid,
      cck_uid
    from origin_common.cc_ods_fs_wk_cct_layer_info
    where platform=14 and gm_uid>0
    union all
    select
      cck_uid as gm_uid,
      cck_uid
    from origin_common.cc_ods_fs_wk_cct_layer_info
    where platform=14 and type=2
  ) g1
  inner join
  (
    select
      s2.cck_uid          as cck_uid,
      s2.item_price       as item_price,
      s1.bomb_type        as bomb_type
    from
    (
      select
        distinct s2.product_id as product_id,
        s1.bomb_type           as bomb_type
      from
      (
        select
          s.ad_material_id as ad_material_id,
          s.bomb_type      as bomb_type
        from
        (
        select
          ad_material_id,
          1 as bomb_type,
          sort
        from origin_common.cc_ods_fs_cck_xb_policies_hourly
        where zone='productList' and ad_key='cct-task-bomb-product' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
        order by sort desc
        limit 1
        ) s
        union all
        select
          h.ad_material_id as ad_material_id,
          h.bomb_type      as bomb_type
        from
        (
        select
          ad_material_id,
          2 as bomb_type,
          sort
        from origin_common.cc_ods_fs_cck_xb_policies_hourly
        where zone='productList' and ad_key='cct-task-bomb-product-two' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
        order by sort desc
        limit 1
        ) h
      )s1
      inner join
      (
        select
          distinct ad_material_id as ad_material_id,
          product_id
        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
      ) s2
      on s1.ad_material_id=s2.ad_material_id
    ) s1
    inner join
    (
      select
        h1.cck_uid    as cck_uid,
        h1.product_id as product_id,
        h1.item_price as item_price
      from
      (
        select
          cck_uid,
          product_id,
          item_price
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
    ) s2
    on s1.product_id=s2.product_id
  ) g2
  on g1.cck_uid=g2.cck_uid
  group by g1.gm_uid
) t2
ON t1.cck_uid=t2.cck_uid
LEFT JOIN --抢购销售额
(
  select
    g1.gm_uid    as cck_uid,
    cast(sum(g2.item_price)/100 as decimal(20,2)) as team_gmv
  from
  (
    select
      gm_uid,
      cck_uid
    from origin_common.cc_ods_fs_wk_cct_layer_info
    where platform=14 and gm_uid>0
    union all
    select
      cck_uid as gm_uid,
      cck_uid
    from origin_common.cc_ods_fs_wk_cct_layer_info
    where platform=14 and type=2
  ) g1
  left join
  (
    select
      s2.cck_uid          as cck_uid,
      s2.item_price       as item_price
    from
    (
      select
        distinct s2.product_id as product_id
      from
      (
        select
          ad_material_id
        from origin_common.cc_ods_fs_cck_xb_policies_hourly
        where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
      ) s1
      inner join
      (
        select
          distinct ad_material_id as ad_material_id,
          product_id
        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
        where active_type!=1
      ) s2
      on s1.ad_material_id=s2.ad_material_id
      left join
      (
        select
          distinct k2.product_id as product_id
        from
        (
          select
            ad_material_id
          from origin_common.cc_ods_fs_cck_xb_policies_hourly
          where zone='productList' and ad_key in ('cct-task-bomb-product','cct-task-bomb-product-two') and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
        ) k1
        inner join
        (
          select
            ad_material_id,
            product_id
          from origin_common.cc_ods_fs_cck_ad_material_products_hourly
        ) k2
        on k1.ad_material_id=k2.ad_material_id
       ) s3
       on s2.product_id=s3.product_id
       where s3.product_id is null
    ) s1
    inner join
    (
      select
        h1.cck_uid    as cck_uid,
        h1.product_id as product_id,
        h1.item_price as item_price
      from
      (
        select
          cck_uid,
          product_id,
          item_price
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
    ) s2
    on s1.product_id=s2.product_id
  ) g2
  on g1.cck_uid=g2.cck_uid
  group by g1.gm_uid
) t3
ON t1.cck_uid=t3.cck_uid

-- LEFT JOIN -- 秒杀销售额
-- (
--   select
--     g1.gm_uid    as cck_uid,
--     cast(sum(g2.item_price)/100 as decimal(20,2)) as team_gmv
--   from
--   (
--     select
--       gm_uid,
--       cck_uid
--     from origin_common.cc_ods_fs_wk_cct_layer_info
--     where platform=14 and gm_uid>0
--     union all
--     select
--       cck_uid as gm_uid,
--       cck_uid
--     from origin_common.cc_ods_fs_wk_cct_layer_info
--     where platform=14 and type=2
--   ) g1
--   left join
--   (
--     select
--       s2.cck_uid          as cck_uid,
--       s2.item_price       as item_price
--     from
--     (
--       select
--         distinct s2.product_id as product_id
--       from
--       (
--         select
--           ad_material_id
--         from origin_common.cc_ods_fs_cck_xb_policies_hourly
--         where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'1537977600' and end_time>='1537891200'
--       ) s1
--       inner join
--       (
--         select
--           distinct ad_material_id as ad_material_id,
--           product_id
--         from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--         where active_type=1
--       ) s2
--       on s1.ad_material_id=s2.ad_material_id
--     ) s1
--     inner join
--     (
--       select
--         h1.cck_uid    as cck_uid,
--         h1.product_id as product_id,
--         h1.item_price as item_price
--       from
--       (
--         select
--           cck_uid,
--           product_id,
--           item_price
--         from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
--         where ds='${bizdate}'
--       ) h1
--       inner join
--       (
--         select
--           cck_uid
--         from origin_common.cc_ods_dwxk_fs_wk_cck_user
--         where ds='${bizdate}' and platform=14
--       ) h2
--       on h1.cck_uid=h2.cck_uid
--     ) s2
--     on s1.product_id=s2.product_id
--   ) g2
--   on g1.cck_uid=g2.cck_uid
--   group by g1.gm_uid
-- ) t4
-- ON t1.cck_uid=t4.cck_uid
LEFT JOIN -- 爆款站外ipv
(
  select
    g1.gm_uid                 as cck_uid,
    sum(if(g2.bomb_type=1,1,0))      as team_out_ipv,
    sum(if(g2.bomb_type=2,1,0))      as team_out_ipv2
  from
  (
    select
      gm_uid,
      cck_uid
    from origin_common.cc_ods_fs_wk_cct_layer_info
    where platform=14 and gm_uid>0
    union all
    select
      cck_uid as gm_uid,
      cck_uid
    from origin_common.cc_ods_fs_wk_cct_layer_info
    where platform=14 and type=2
  ) g1
  left join
  (
    select
      s2.cck_uid          as cck_uid,
      s2.product_id       as product_id,
      s1.bomb_type        as bomb_type
    from
    (
      select
        distinct s2.product_id as product_id,
        s1.bomb_type           as bomb_type
      from
      (
        select
          s.ad_material_id as ad_material_id,
          s.bomb_type      as bomb_type
        from
        (
        select
          ad_material_id,
          1 as bomb_type,
          sort
        from origin_common.cc_ods_fs_cck_xb_policies_hourly
        where zone='productList' and ad_key='cct-task-bomb-product' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
        order by sort desc
        limit 1
        ) s
        union all
        select
          h.ad_material_id as ad_material_id,
          h.bomb_type      as bomb_type
        from
        (
        select
          ad_material_id,
          2 as bomb_type,
          sort
        from origin_common.cc_ods_fs_cck_xb_policies_hourly
        where zone='productList' and ad_key='cct-task-bomb-product-two' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
        order by sort desc
        limit 1
        ) h
      )s1
      inner join
      (
        select
          distinct ad_material_id as ad_material_id,
          product_id
        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
      ) s2
      on s1.ad_material_id=s2.ad_material_id
    ) s1
    inner join
    (
      select
        h1.cck_uid    as cck_uid,
        h1.product_id as product_id
      from
      (
        select
          cck_uid,
          product_id
        from origin_common.cc_ods_log_cctui_product_coupon_detail_hourly
        where ds='${bizdate}' and is_in_app=0 and detail_type='item'
      ) h1
      inner join
      (
        select
          cck_uid
        from origin_common.cc_ods_dwxk_fs_wk_cck_user
        where ds='${bizdate}' and platform=14
      ) h2
      on h1.cck_uid=h2.cck_uid
    ) s2
    on s1.product_id=s2.product_id
  ) g2
  on g1.cck_uid=g2.cck_uid
  group by g1.gm_uid
) t5
ON t1.cck_uid=t5.cck_uid
-- LEFT JOIN -- 秒杀站外ipv
-- (
--   select
--     g1.gm_uid                 as cck_uid,
--     count(g2.product_id)      as team_out_ipv
--   from
--   (
--     select
--       gm_uid,
--       cck_uid
--     from origin_common.cc_ods_fs_wk_cct_layer_info
--     where platform=14 and gm_uid>0
--     union all
--     select
--       cck_uid as gm_uid,
--       cck_uid
--     from origin_common.cc_ods_fs_wk_cct_layer_info
--     where platform=14 and type=2
--   ) g1
--   left join
--   (
--     select
--       s2.cck_uid          as cck_uid,
--       s2.product_id       as product_id
--     from
--     (
--       select
--         distinct s2.product_id as product_id
--       from
--       (
--         select
--           ad_material_id
--         from origin_common.cc_ods_fs_cck_xb_policies_hourly
--         where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'1537977600' and end_time>='1537891200'
--       ) s1
--       inner join
--       (
--         select
--           distinct ad_material_id as ad_material_id,
--           product_id
--         from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--         where active_type=1
--       ) s2
--       on s1.ad_material_id=s2.ad_material_id
--     ) s1
--     inner join
--     (
--       select
--         h1.cck_uid    as cck_uid,
--         h1.product_id as product_id
--       from
--       (
--         select
--           cck_uid,
--           product_id
--         from origin_common.cc_ods_log_cctui_product_coupon_detail_hourly
--         where ds='${bizdate}' and is_in_app=0 and detail_type='item'
--       ) h1
--       inner join
--       (
--         select
--           cck_uid
--         from origin_common.cc_ods_dwxk_fs_wk_cck_user
--         where ds='${bizdate}' and platform=14
--       ) h2
--       on h1.cck_uid=h2.cck_uid
--     ) s2
--     on s1.product_id=s2.product_id
--   ) g2
--   on g1.cck_uid=g2.cck_uid
--   group by g1.gm_uid
-- ) t6
-- ON t1.cck_uid=t6.cck_uid
LEFT JOIN -- 抢购站外ipv
(
  select
    g1.gm_uid                 as cck_uid,
    count(g2.product_id)      as team_out_ipv
  from
  (
    select
      gm_uid,
      cck_uid
    from origin_common.cc_ods_fs_wk_cct_layer_info
    where platform=14 and gm_uid>0
    union all
    select
      cck_uid as gm_uid,
      cck_uid
    from origin_common.cc_ods_fs_wk_cct_layer_info
    where platform=14 and type=2
  ) g1
  left join
  (
    select
      s2.cck_uid          as cck_uid,
      s2.product_id       as product_id
    from
    (
      select
        distinct s2.product_id as product_id
      from
      (
        select
          ad_material_id
        from origin_common.cc_ods_fs_cck_xb_policies_hourly
        where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
      ) s1
      inner join
      (
        select
          distinct ad_material_id as ad_material_id,
          product_id
        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
        where active_type!=1
      ) s2
      on s1.ad_material_id=s2.ad_material_id
      left join
      (
        select
          distinct k2.product_id as product_id
        from
        (
          select
            id
          from origin_common.cc_ods_fs_cck_xb_policies_hourly
          where zone='productList' and ad_key in ('cct-task-bomb-product','cct-task-bomb-product-two') and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
        ) k1
        inner join
        (
          select
            ad_material_id,
            product_id
          from origin_common.cc_ods_fs_cck_ad_material_products_hourly
        ) k2
        on k1.id=k2.ad_material_id
       ) s3
       on s2.product_id=s3.product_id
       where s3.product_id is null
    ) s1
    inner join
    (
      select
        h1.cck_uid    as cck_uid,
        h1.product_id as product_id
      from
      (
        select
          cck_uid,
          product_id
        from origin_common.cc_ods_log_cctui_product_coupon_detail_hourly
        where ds='${bizdate}' and is_in_app=0 and detail_type='item'
      ) h1
      inner join
      (
        select
          cck_uid
        from origin_common.cc_ods_dwxk_fs_wk_cck_user
        where ds='${bizdate}' and platform=14
      ) h2
      on h1.cck_uid=h2.cck_uid
    ) s2
    on s1.product_id=s2.product_id
  ) g2
  on g1.cck_uid=g2.cck_uid
  group by g1.gm_uid
) t7
ON t1.cck_uid=t7.cck_uid
LEFT JOIN -- 爆款分享
(
  select
    g1.gm_uid                                          as cck_uid,
    sum(if(g2.bomb_type=1,1,0))                        as team_fx_cnt,
    count(distinct if(g2.bomb_type=1,g2.user_id,null)) as team_fx_user_cnt,
    sum(if(g2.bomb_type=2,1,0))                        as team_fx_cnt2,
    count(distinct if(g2.bomb_type=2,g2.user_id,null)) as team_fx_user_cnt2
  from
  (
    select
      k1.gm_uid     as gm_uid,
      k2.cct_uid    as cct_uid
    from
    (
      select
        gm_uid,
        cck_uid
      from origin_common.cc_ods_fs_wk_cct_layer_info
      where platform=14 and gm_uid>0
      union all
      select
        cck_uid as gm_uid,
        cck_uid
      from origin_common.cc_ods_fs_wk_cct_layer_info
      where platform=14 and type=2
    ) k1
    inner join
    (
      select
        cck_uid,
        cct_uid
      from origin_common.cc_ods_fs_tui_relation
    ) k2
    on k1.cck_uid=k2.cck_uid
  ) g1
  left join
  (
  select
    m3.product_id as product_id,
    m1.user_id    as user_id,
    m4.bomb_type  as bomb_type
  from
  (
    select
      user_id,ad_id
    from origin_common.cc_ods_log_cctapp_click_hourly where ds='${bizdate}' and module='detail_material' and zone in ('line','small_routine','pQrCode','promotion')
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
  on m2.item_id = m3.item_id
  inner join
  (
    select
      distinct s2.product_id as product_id,
      s1.bomb_type           as bomb_type
    from
    (
        select
          s.ad_material_id as ad_material_id,
          s.bomb_type      as bomb_type
        from
        (
        select
          ad_material_id,
          1 as bomb_type,
          sort
        from origin_common.cc_ods_fs_cck_xb_policies_hourly
        where zone='productList' and ad_key='cct-task-bomb-product' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
        order by sort desc
        limit 1
        ) s
        union all
        select
          h.ad_material_id as ad_material_id,
          h.bomb_type      as bomb_type
        from
        (
        select
          ad_material_id,
          2 as bomb_type,
          sort
        from origin_common.cc_ods_fs_cck_xb_policies_hourly
        where zone='productList' and ad_key='cct-task-bomb-product-two' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
        order by sort desc
        limit 1
        ) h
    )s1
    inner join
    (
       select
         distinct ad_material_id as ad_material_id,
         product_id
       from origin_common.cc_ods_fs_cck_ad_material_products_hourly
    ) s2
    on s1.ad_material_id=s2.ad_material_id
  ) m4
  on m3.product_id = m4.product_id
  ) g2
  on g1.cct_uid=g2.user_id
  group by g1.gm_uid
) t8
ON t1.cck_uid=t8.cck_uid
-- LEFT JOIN -- 秒杀分享
-- (
--   select
--     g1.gm_uid                  as cck_uid,
--     count(g2.user_id)          as team_fx_cnt,
--     count(distinct g2.user_id) as team_fx_user_cnt
--   from
--   (
--     select
--       k1.gm_uid        as gm_uid,
--       k2.cct_uid       as cct_uid
--     from
--     (
--       select
--         gm_uid,
--         cck_uid
--       from origin_common.cc_ods_fs_wk_cct_layer_info
--       where platform=14 and gm_uid>0
--       union all
--       select
--         cck_uid as gm_uid,
--         cck_uid
--       from origin_common.cc_ods_fs_wk_cct_layer_info
--       where platform=14 and type=2
--     ) k1
--     inner join
--     (
--       select
--         cck_uid,
--         cct_uid
--       from origin_common.cc_ods_fs_tui_relation
--     ) k2
--     on k1.cck_uid=k2.cck_uid
--   ) g1
--   left join
--   (
--   select
--     m3.product_id as product_id,
--     m1.user_id    as user_id
--   from
--   (
--     select
--       ad_material_id as ad_id,
--       user_id
--     from origin_common.cc_ods_log_cctapp_click_hourly
--     where ds = '${bizdate}' and ad_type in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
--     union all
--     select
--       ad_id,
--       user_id
--     from origin_common.cc_ods_log_cctapp_click_hourly
--     where ds = '${bizdate}' and ad_type not in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
--     union all
--     select
--       s2.ad_id,
--       s1.user_id
--     from
--     (
--       select
--         ad_material_id,
--         user_id
--       from origin_common.cc_ods_log_cctapp_click_hourly
--       where ds = '${bizdate}' and module='vip' and ad_type in ('single_product','9_cell') and zone in ('material_group-share','material_moments-share')
--     ) s1
--     inner join
--     (
--       select
--          distinct ad_material_id as ad_material_id,
--          ad_id
--       from data.cc_dm_gwapp_new_ad_material_relation_hourly
--       where ds = '${bizdate}'
--     ) s2
--     on s1.ad_material_id = s2.ad_material_id
--   ) as m1
--   inner join
--   (
--     select
--       ad_id,
--       item_id
--     from origin_common.cc_ods_fs_dwxk_ad_items_daily
--   ) m2
--   on m1.ad_id = m2.ad_id
--   inner join
--   (
--     select
--       item_id,
--       app_item_id as product_id
--     from origin_common.cc_ods_dwxk_fs_wk_items
--   ) m3
--   on m2.item_id = m3.item_id
--   inner join
--   (
--     select
--       distinct s2.product_id as product_id
--     from
--     (
--         select
--           ad_material_id
--         from origin_common.cc_ods_fs_cck_xb_policies_hourly
--         where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'1537977600' and end_time>='1537891200'
--       ) s1
--       inner join
--       (
--         select
--           distinct ad_material_id as ad_material_id,
--           product_id
--         from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--         where active_type=1
--       ) s2
--       on s1.ad_material_id=s2.ad_material_id
--   ) m4
--   on m3.product_id = m4.product_id
--   union all
--   select
--     h1.product_id as product_id,
--     h2.cct_uid     as user_id
--   from
--   (
--     select
--       s1.user_id as user_id,
--       s2.product_id as product_id
--     from
--     (
--       select
--         user_id,
--         ad_material_id
--       from origin_common.cc_ods_log_gwapp_click_hourly
--       where ds='${bizdate}' and module in ('afp','index_share_moments') and (zone in ('footersharecctafp','headsharecctafp') or zone like 'cctproductHotAreaShare%' or zone like 'cctBannerShare%' or zone like 'list_share_%')
--     ) s1
--     inner join
--     (
--       select
--         distinct ad_material_id as ad_material_id,
--         product_id
--       from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--     ) s2
--     on s1.ad_material_id=s2.ad_material_id
--     union all
--     select
--       m1.user_id as user_id,
--       m3.product_id as product_id
--     from
--     (
--       select
--         user_id,
--         ad_id
--       from origin_common.cc_ods_log_gwapp_click_hourly
--       where ds='${bizdate}' and module = 'afp' and zone like 'cctproductshare%'
--     ) m1
--     inner join
--     (
--       select
--         ad_id,
--         item_id
--       from origin_common.cc_ods_fs_dwxk_ad_items_daily
--     ) m2
--     on m1.ad_id = m2.ad_id
--     inner join
--     (
--       select
--         item_id,
--         app_item_id as product_id
--       from origin_common.cc_ods_dwxk_fs_wk_items
--     ) m3
--     on m2.item_id = m3.item_id
--   ) h1
--   inner join
--   (
--     select
--       distinct cck_uid as cck_uid,
--       cct_uid
--     from origin_common.cc_ods_dwxk_fs_wk_cck_user_hourly
--     where ds='${bizdate}' and platform=14
--   ) h2
--   on h1.user_id=h2.cck_uid
--   inner join
--   (
--       select
--         distinct s2.product_id as product_id
--       from
--       (
--         select
--           ad_material_id
--         from origin_common.cc_ods_fs_cck_xb_policies_hourly
--         where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'1537977600' and end_time>='1537891200'
--       ) s1
--       inner join
--       (
--         select
--           distinct ad_material_id as ad_material_id,
--           product_id
--         from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--         where active_type=1
--       ) s2
--       on s1.ad_material_id=s2.ad_material_id
--   ) h3
--   on h1.product_id=h3.product_id
--   ) g2
--   on g1.cct_uid=g2.user_id
--   group by g1.gm_uid
-- ) t9
-- ON t1.cck_uid=t9.cck_uid
LEFT JOIN -- 抢购分享
(
  select
    g1.gm_uid              as cck_uid,
    count(g2.user_id)      as team_fx_cnt,
    count(distinct g2.user_id) as team_fx_user_cnt
  from
  (
    select
      k1.gm_uid     as gm_uid,
      k2.cct_uid    as cct_uid
    from
    (
      select
        gm_uid,
        cck_uid
      from origin_common.cc_ods_fs_wk_cct_layer_info
      where platform=14 and gm_uid>0
      union all
      select
        cck_uid as gm_uid,
        cck_uid
      from origin_common.cc_ods_fs_wk_cct_layer_info
      where platform=14 and type=2
    ) k1
    inner join
    (
      select
        cck_uid,
        cct_uid
      from origin_common.cc_ods_fs_tui_relation
    ) k2
    on k1.cck_uid=k2.cck_uid
  ) g1
  left join
  (
  select
    m3.product_id as product_id,
    m1.user_id    as user_id
  from
  (
    select
      user_id,ad_id
    from origin_common.cc_ods_log_cctapp_click_hourly where ds='${bizdate}' and module='detail_material' and zone in ('line','small_routine','pQrCode','promotion')
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
  on m2.item_id = m3.item_id
  inner join
  (
      select
        distinct s2.product_id as product_id
      from
      (
        select
          ad_material_id
        from origin_common.cc_ods_fs_cck_xb_policies_hourly
        where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
      ) s1
      inner join
      (
        select
          distinct ad_material_id as ad_material_id,
          product_id
        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
        where active_type!=1
      ) s2
      on s1.ad_material_id=s2.ad_material_id
      left join
      (
        select
          distinct k2.product_id as product_id
        from
        (
          select
            ad_material_id
          from origin_common.cc_ods_fs_cck_xb_policies_hourly
          where zone='productList' and ad_key in ('cct-task-bomb-product','cct-task-bomb-product-two') and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
        ) k1
        inner join
        (
          select
            distinct ad_material_id as ad_material_id,
            product_id
          from origin_common.cc_ods_fs_cck_ad_material_products_hourly
        ) k2
        on k1.ad_material_id=k2.ad_material_id
       ) s3
       on s2.product_id=s3.product_id
       where s3.product_id is null
  ) m4
  on m3.product_id = m4.product_id
  ) g2
  on g1.cct_uid=g2.user_id
  group by g1.gm_uid
) t10
ON t1.cck_uid=t10.cck_uid
left join -- 爆款团队自购
(
  select
    g1.gm_uid    as cck_uid,
    cast(sum(if(g2.bomb_type=1,g2.item_price,0))/100 as decimal(20,2)) as team_self_gmv,
    cast(sum(if(g2.bomb_type=2,g2.item_price,0))/100 as decimal(20,2)) as team_self_gmv2
  from
  (
    select
      gm_uid,
      cck_uid
    from origin_common.cc_ods_fs_wk_cct_layer_info
    where platform=14 and gm_uid>0
    union all
    select
      cck_uid as gm_uid,
      cck_uid
    from origin_common.cc_ods_fs_wk_cct_layer_info
    where platform=14 and type=2
  ) g1
  left join
  (
    select
      s2.cck_uid          as cck_uid,
      s2.item_price       as item_price,
      s1.bomb_type        as bomb_type
    from
    (
      select
        distinct s2.product_id as product_id,
        s1.bomb_type           as bomb_type
      from
      (
        select
          s.ad_material_id as ad_material_id,
          s.bomb_type      as bomb_type
        from
        (
        select
          ad_material_id,
          1 as bomb_type,
          sort
        from origin_common.cc_ods_fs_cck_xb_policies_hourly
        where zone='productList' and ad_key='cct-task-bomb-product' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
        order by sort desc
        limit 1
        ) s
        union all
        select
          h.ad_material_id as ad_material_id,
          h.bomb_type      as bomb_type
        from
        (
        select
          ad_material_id,
          2 as bomb_type,
          sort
        from origin_common.cc_ods_fs_cck_xb_policies_hourly
        where zone='productList' and ad_key='cct-task-bomb-product-two' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
        order by sort desc
        limit 1
        ) h
      )s1
      inner join
      (
        select
          distinct ad_material_id as ad_material_id,
          product_id
        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
      ) s2
      on s1.ad_material_id=s2.ad_material_id
    ) s1
    inner join
    (
      select
        h1.cck_uid    as cck_uid,
        h1.product_id as product_id,
        h1.item_price as item_price
      from
      (
        select
          cck_uid,
          uid,
          product_id,
          item_price
        from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where ds='${bizdate}'
      ) h1
      inner join
      (
        select
          distinct cck_uid as cck_uid,
          cct_uid
        from origin_common.cc_ods_dwxk_fs_wk_cck_user_hourly
        where ds='${bizdate}' and platform=14
      ) h2
      on h1.cck_uid=h2.cck_uid and h1.uid=h2.cct_uid
    ) s2
    on s1.product_id=s2.product_id
  ) g2
  on g1.cck_uid=g2.cck_uid
  group by g1.gm_uid
) t11
on t1.cck_uid=t11.cck_uid
left join
(
  select
    g1.gm_uid    as cck_uid,
    cast(sum(g2.item_price)/100 as decimal(20,2)) as team_self_gmv
  from
  (
    select
      gm_uid,
      cck_uid
    from origin_common.cc_ods_fs_wk_cct_layer_info
    where platform=14 and gm_uid>0
    union all
    select
      cck_uid as gm_uid,
      cck_uid
    from origin_common.cc_ods_fs_wk_cct_layer_info
    where platform=14 and type=2
  ) g1
  left join
  (
    select
      s2.cck_uid          as cck_uid,
      s2.item_price       as item_price
    from
    (
      select
        distinct s2.product_id as product_id
      from
      (
        select
          ad_material_id
        from origin_common.cc_ods_fs_cck_xb_policies_hourly
        where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
      ) s1
      inner join
      (
        select
          distinct ad_material_id as ad_material_id,
          product_id
        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
        where active_type=1
      ) s2
      on s1.ad_material_id=s2.ad_material_id
    ) s1
    inner join
    (
      select
        h1.cck_uid    as cck_uid,
        h1.product_id as product_id,
        h1.item_price as item_price
      from
      (
        select
          cck_uid,
          uid,
          product_id,
          item_price
        from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where ds='${bizdate}'
      ) h1
      inner join
      (
        select
          distinct cck_uid as cck_uid,
          cct_uid
        from origin_common.cc_ods_dwxk_fs_wk_cck_user_hourly
        where ds='${bizdate}' and platform=14
      ) h2
      on h1.cck_uid=h2.cck_uid and h1.uid=h2.cct_uid
    ) s2
    on s1.product_id=s2.product_id
  ) g2
  on g1.cck_uid=g2.cck_uid
  group by g1.gm_uid
) t12
on t1.cck_uid=t12.cck_uid
left join --抢购团队自购
(
  select
    g1.gm_uid    as cck_uid,
    cast(sum(g2.item_price)/100 as decimal(20,2)) as team_self_gmv
  from
  (
    select
      gm_uid,
      cck_uid
    from origin_common.cc_ods_fs_wk_cct_layer_info
    where platform=14 and gm_uid>0
    union all
    select
      cck_uid as gm_uid,
      cck_uid
    from origin_common.cc_ods_fs_wk_cct_layer_info
    where platform=14 and type=2
  ) g1
  left join
  (
    select
      s2.cck_uid          as cck_uid,
      s2.item_price       as item_price
    from
    (
      select
        distinct s2.product_id as product_id
      from
      (
        select
          ad_material_id
        from origin_common.cc_ods_fs_cck_xb_policies_hourly
        where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
      ) s1
      inner join
      (
        select
          distinct ad_material_id as ad_material_id,
          product_id
        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
        where active_type!=1
      ) s2
      on s1.ad_material_id=s2.ad_material_id
      left join
      (
        select
          distinct k2.product_id as product_id
        from
        (
          select
            ad_material_id
          from origin_common.cc_ods_fs_cck_xb_policies_hourly
          where zone='productList' and ad_key in ('cct-task-bomb-product','cct-task-bomb-product-two') and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
        ) k1
        inner join
        (
          select
            ad_material_id,
            product_id
          from origin_common.cc_ods_fs_cck_ad_material_products_hourly
        ) k2
        on k1.ad_material_id=k2.ad_material_id
       ) s3
       on s2.product_id=s3.product_id
       where s3.product_id is null
    ) s1
    inner join
    (
      select
        h1.cck_uid    as cck_uid,
        h1.product_id as product_id,
        h1.item_price as item_price
      from
      (
        select
          cck_uid,
          uid,
          product_id,
          item_price
        from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where ds='${bizdate}'
      ) h1
      inner join
      (
        select
          distinct cck_uid as cck_uid,
          cct_uid
        from origin_common.cc_ods_dwxk_fs_wk_cck_user_hourly
        where ds='${bizdate}' and platform=14
      ) h2
      on h1.cck_uid=h2.cck_uid and h1.uid=h2.cct_uid
    ) s2
    on s1.product_id=s2.product_id
  ) g2
  on g1.cck_uid=g2.cck_uid
  group by g1.gm_uid
) t13
on t1.cck_uid=t13.cck_uid
left join --爆款总经理自购
(
    select
    g1.cck_uid    as cck_uid,
    cast(sum(if(g2.bomb_type=1,g2.item_price,0))/100 as decimal(20,2)) as team_gm_self_gmv,
    cast(sum(if(g2.bomb_type=2,g2.item_price,0))/100 as decimal(20,2)) as team_gm_self_gmv2
  from
  (
    select
      cck_uid
    from origin_common.cc_ods_fs_wk_cct_layer_info
    where platform=14 and type=2
  ) g1
  left join
  (
    select
      s2.cck_uid          as cck_uid,
      s2.item_price       as item_price,
      s1.bomb_type        as bomb_type
    from
    (
      select
        distinct s2.product_id as product_id,
        s1.bomb_type           as bomb_type
      from
      (
        select
          s.ad_material_id as ad_material_id,
          s.bomb_type      as bomb_type
        from
        (
        select
          ad_material_id,
          1 as bomb_type,
          sort
        from origin_common.cc_ods_fs_cck_xb_policies_hourly
        where zone='productList' and ad_key='cct-task-bomb-product' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
        order by sort desc
        limit 1
        ) s
        union all
        select
          h.ad_material_id as ad_material_id,
          h.bomb_type      as bomb_type
        from
        (
        select
          ad_material_id,
          2 as bomb_type,
          sort
        from origin_common.cc_ods_fs_cck_xb_policies_hourly
        where zone='productList' and ad_key='cct-task-bomb-product-two' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
        order by sort desc
        limit 1
        ) h
      ) s1
      inner join
      (
        select
          distinct ad_material_id as ad_material_id,
          product_id
        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
      ) s2
      on s1.ad_material_id=s2.ad_material_id
    ) s1
    inner join
    (
      select
        h1.cck_uid    as cck_uid,
        h1.product_id as product_id,
        h1.item_price as item_price
      from
      (
        select
          cck_uid,
          uid,
          product_id,
          item_price
        from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where ds='${bizdate}'
      ) h1
      inner join
      (
        select
          distinct cck_uid as cck_uid,
          cct_uid
        from origin_common.cc_ods_dwxk_fs_wk_cck_user_hourly
        where ds='${bizdate}' and platform=14
      ) h2
      on h1.cck_uid=h2.cck_uid and h1.uid=h2.cct_uid
    ) s2
    on s1.product_id=s2.product_id
  ) g2
  on g1.cck_uid=g2.cck_uid
  group by g1.cck_uid
) t14
on t1.cck_uid=t14.cck_uid
-- left join --秒杀总经理自购
-- (
--   select
--     g1.cck_uid    as cck_uid,
--     cast(sum(g2.item_price)/100 as decimal(20,2)) as team_gm_self_gmv
--   from
--   (
--     select
--       cck_uid
--     from origin_common.cc_ods_fs_wk_cct_layer_info
--     where platform=14 and type=2
--   ) g1
--   left join
--   (
--     select
--       s2.cck_uid          as cck_uid,
--       s2.item_price       as item_price
--     from
--     (
--       select
--         distinct s2.product_id as product_id
--       from
--       (
--         select
--           ad_material_id
--         from origin_common.cc_ods_fs_cck_xb_policies_hourly
--         where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'1537977600' and end_time>='1537891200'
--       ) s1
--       inner join
--       (
--         select
--           distinct ad_material_id as ad_material_id,
--           product_id
--         from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--         where active_type=1
--       ) s2
--       on s1.ad_material_id=s2.ad_material_id
--     ) s1
--     inner join
--     (
--       select
--         h1.cck_uid    as cck_uid,
--         h1.product_id as product_id,
--         h1.item_price as item_price
--       from
--       (
--         select
--           cck_uid,
--           uid,
--           product_id,
--           item_price
--         from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
--         where ds='${bizdate}'
--       ) h1
--       inner join
--       (
--         select
--           distinct cck_uid as cck_uid,
--           cct_uid
--         from origin_common.cc_ods_dwxk_fs_wk_cck_user_hourly
--         where ds='${bizdate}' and platform=14
--       ) h2
--       on h1.cck_uid=h2.cck_uid and h1.uid=h2.cct_uid
--     ) s2
--     on s1.product_id=s2.product_id
--   ) g2
--   on g1.cck_uid=g2.cck_uid
--   group by g1.cck_uid
-- ) t15
-- on t1.cck_uid=t15.cck_uid
left join --抢购总经理自购
(
  select
    g1.cck_uid    as cck_uid,
    cast(sum(g2.item_price)/100 as decimal(20,2)) as team_gm_self_gmv
  from
  (
    select
      cck_uid
    from origin_common.cc_ods_fs_wk_cct_layer_info
    where platform=14 and type=2
  ) g1
  left join
  (
    select
      s2.cck_uid          as cck_uid,
      s2.item_price       as item_price
    from
    (
      select
        distinct s2.product_id as product_id
      from
      (
        select
          ad_material_id
        from origin_common.cc_ods_fs_cck_xb_policies_hourly
        where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
      ) s1
      inner join
      (
        select
          distinct ad_material_id as ad_material_id,
          product_id
        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
        where active_type!=1
      ) s2
      on s1.ad_material_id=s2.ad_material_id
      left join
      (
        select
          distinct k2.product_id as product_id
        from
        (
          select
            ad_material_id
          from origin_common.cc_ods_fs_cck_xb_policies_hourly
          where zone='productList' and ad_key in ('cct-task-bomb-product','cct-task-bomb-product-two') and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
        ) k1
        inner join
        (
          select
            ad_material_id,
            product_id
          from origin_common.cc_ods_fs_cck_ad_material_products_hourly
        ) k2
        on k1.ad_material_id=k2.ad_material_id
       ) s3
       on s2.product_id=s3.product_id
       where s3.product_id is null
    ) s1
    inner join
    (
      select
        h1.cck_uid    as cck_uid,
        h1.product_id as product_id,
        h1.item_price as item_price
      from
      (
        select
          cck_uid,
          uid,
          product_id,
          item_price
        from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where ds='${bizdate}'
      ) h1
      inner join
      (
        select
          distinct cck_uid as cck_uid,
          cct_uid
        from origin_common.cc_ods_dwxk_fs_wk_cck_user_hourly
        where ds='${bizdate}' and platform=14
      ) h2
      on h1.cck_uid=h2.cck_uid and h1.uid=h2.cct_uid
    ) s2
    on s1.product_id=s2.product_id
  ) g2
  on g1.cck_uid=g2.cck_uid
  group by g1.cck_uid
) t16
on t1.cck_uid=t16.cck_uid
LEFT JOIN -- 爆款总经理分享
(
  select
    g1.gm_uid                   as cck_uid,
    sum(if(g2.bomb_type=1,1,0)) as self_fx_cnt,
    sum(if(g2.bomb_type=2,1,0)) as self_fx_cnt2
  from
  (
    select
      k1.cck_uid    as gm_uid,
      k2.cct_uid    as cct_uid
    from
    (
      select
        cck_uid
      from origin_common.cc_ods_fs_wk_cct_layer_info
      where platform=14 and type=2
    ) k1
    inner join
    (
      select
        cck_uid,
        cct_uid
      from origin_common.cc_ods_dwxk_fs_wk_cck_user
      where platform=14 and ds='${bizdate}'
    ) k2
    on k1.cck_uid=k2.cck_uid
  ) g1
  left join
  (
  select
    m3.product_id as product_id,
    m1.user_id    as user_id,
    m4.bomb_type  as bomb_type
  from
  (
    select
      user_id,ad_id
    from origin_common.cc_ods_log_cctapp_click_hourly where ds='${bizdate}' and module='detail_material' and zone in ('line','small_routine','pQrCode','promotion')
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
  on m2.item_id = m3.item_id
  inner join
  (
    select
      distinct s2.product_id as product_id,
      s1.bomb_type           as bomb_type
    from
    (
        select
          s.ad_material_id as ad_material_id,
          s.bomb_type      as bomb_type
        from
        (
        select
          ad_material_id,
          1 as bomb_type,
          sort
        from origin_common.cc_ods_fs_cck_xb_policies_hourly
        where zone='productList' and ad_key='cct-task-bomb-product' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
        order by sort desc
        limit 1
        ) s
        union all
        select
          h.ad_material_id as ad_material_id,
          h.bomb_type      as bomb_type
        from
        (
        select
          ad_material_id,
          2 as bomb_type,
          sort
        from origin_common.cc_ods_fs_cck_xb_policies_hourly
        where zone='productList' and ad_key='cct-task-bomb-product-two' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
        order by sort desc
        limit 1
        ) h
    )s1
    inner join
    (
       select
         distinct ad_material_id as ad_material_id,
         product_id
       from origin_common.cc_ods_fs_cck_ad_material_products_hourly
    ) s2
    on s1.ad_material_id=s2.ad_material_id
  ) m4
  on m3.product_id = m4.product_id
  union all
  select
    h1.product_id as product_id,
    h2.cct_uid     as user_id,
    h3.bomb_type   as bomb_type
  from
  (
    select
      s1.user_id as user_id,
      s2.product_id as product_id
    from
    (
      select
        user_id,
        ad_material_id
      from origin_common.cc_ods_log_gwapp_click_hourly
      where ds='${bizdate}' and module in ('afp','index_share_moments') and (zone in ('footersharecctafp','headsharecctafp') or zone like 'cctproductHotAreaShare%' or zone like 'cctBannerShare%' or zone like 'list_share_%')
      union all
      select
        user_id,
        hash_value as ad_material_id
      from origin_common.cc_ods_log_cctapp_click_hourly
      where ds='${bizdate}' and module = 'share' and zone = 'show'
    ) s1
    inner join
    (
      select
        distinct ad_material_id as ad_material_id,
        product_id
      from origin_common.cc_ods_fs_cck_ad_material_products_hourly
    ) s2
    on s1.ad_material_id=s2.ad_material_id
    union all
    select
      m1.user_id as user_id,
      m3.product_id as product_id
    from
    (
      select
        user_id,
        ad_id
      from origin_common.cc_ods_log_gwapp_click_hourly
      where ds='${bizdate}' and module = 'afp' and zone like 'cctproductshare%'
    ) m1
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
    on m2.item_id = m3.item_id
  ) h1
  inner join
  (
    select
      distinct cck_uid as cck_uid,
      cct_uid
    from origin_common.cc_ods_dwxk_fs_wk_cck_user_hourly
    where ds='${bizdate}' and platform=14
  ) h2
  on h1.user_id=h2.cck_uid
  inner join
  (
    select
      distinct s2.product_id as product_id,
      s1.bomb_type           as bomb_type
    from
    (
        select
          s.ad_material_id as ad_material_id,
          s.bomb_type      as bomb_type
        from
        (
        select
          ad_material_id,
          1 as bomb_type,
          sort
        from origin_common.cc_ods_fs_cck_xb_policies_hourly
        where zone='productList' and ad_key='cct-task-bomb-product' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
        order by sort desc
        limit 1
        ) s
        union all
        select
          h.ad_material_id as ad_material_id,
          h.bomb_type      as bomb_type
        from
        (
        select
          ad_material_id,
          2 as bomb_type,
          sort
        from origin_common.cc_ods_fs_cck_xb_policies_hourly
        where zone='productList' and ad_key='cct-task-bomb-product-two' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
        order by sort desc
        limit 1
        ) h
    )s1
    inner join
    (
       select
         distinct ad_material_id as ad_material_id,
         product_id
       from origin_common.cc_ods_fs_cck_ad_material_products_hourly
    ) s2
    on s1.ad_material_id=s2.ad_material_id
  ) h3
  on h1.product_id=h3.product_id
  ) g2
  on g1.cct_uid=g2.user_id
  group by g1.gm_uid
) t17
ON t1.cck_uid=t17.cck_uid
-- LEFT JOIN -- 秒杀总经理分享
-- (
--   select
--     g1.gm_uid                  as cck_uid,
--     count(g2.user_id)          as self_fx_cnt
--   from
--   (
--     select
--       k1.cck_uid       as gm_uid,
--       k2.cct_uid       as cct_uid
--     from
--     (
--       select
--         cck_uid
--       from origin_common.cc_ods_fs_wk_cct_layer_info
--       where platform=14 and type=2
--     ) k1
--     inner join
--     (
--       select
--         cck_uid,
--         cct_uid
--       from origin_common.cc_ods_fs_tui_relation
--     ) k2
--     on k1.cck_uid=k2.cck_uid
--   ) g1
--   left join
--   (
--   select
--     m3.product_id as product_id,
--     m1.user_id    as user_id
--   from
--   (
--     select
--       ad_material_id as ad_id,
--       user_id
--     from origin_common.cc_ods_log_cctapp_click_hourly
--     where ds = '${bizdate}' and ad_type in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
--     union all
--     select
--       ad_id,
--       user_id
--     from origin_common.cc_ods_log_cctapp_click_hourly
--     where ds = '${bizdate}' and ad_type not in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
--     union all
--     select
--       s2.ad_id,
--       s1.user_id
--     from
--     (
--       select
--         ad_material_id,
--         user_id
--       from origin_common.cc_ods_log_cctapp_click_hourly
--       where ds = '${bizdate}' and module='vip' and ad_type in ('single_product','9_cell') and zone in ('material_group-share','material_moments-share')
--     ) s1
--     inner join
--     (
--       select
--          distinct ad_material_id as ad_material_id,
--          ad_id
--       from data.cc_dm_gwapp_new_ad_material_relation_hourly
--       where ds = '${bizdate}'
--     ) s2
--     on s1.ad_material_id = s2.ad_material_id
--   ) as m1
--   inner join
--   (
--     select
--       ad_id,
--       item_id
--     from origin_common.cc_ods_fs_dwxk_ad_items_daily
--   ) m2
--   on m1.ad_id = m2.ad_id
--   inner join
--   (
--     select
--       item_id,
--       app_item_id as product_id
--     from origin_common.cc_ods_dwxk_fs_wk_items
--   ) m3
--   on m2.item_id = m3.item_id
--   inner join
--   (
--     select
--       distinct s2.product_id as product_id
--     from
--     (
--         select
--           ad_material_id
--         from origin_common.cc_ods_fs_cck_xb_policies_hourly
--         where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'1537977600' and end_time>='1537891200'
--       ) s1
--       inner join
--       (
--         select
--           distinct ad_material_id as ad_material_id,
--           product_id
--         from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--         where active_type=1
--       ) s2
--       on s1.ad_material_id=s2.ad_material_id
--   ) m4
--   on m3.product_id = m4.product_id
--   union all
--   select
--     h1.product_id as product_id,
--     h2.cct_uid     as user_id
--   from
--   (
--     select
--       s1.user_id as user_id,
--       s2.product_id as product_id
--     from
--     (
--       select
--         user_id,
--         ad_material_id
--       from origin_common.cc_ods_log_gwapp_click_hourly
--       where ds='${bizdate}' and module in ('afp','index_share_moments') and (zone in ('footersharecctafp','headsharecctafp') or zone like 'cctproductHotAreaShare%' or zone like 'cctBannerShare%' or zone like 'list_share_%')
--     ) s1
--     inner join
--     (
--       select
--         distinct ad_material_id as ad_material_id,
--         product_id
--       from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--     ) s2
--     on s1.ad_material_id=s2.ad_material_id
--     union all
--     select
--       m1.user_id as user_id,
--       m3.product_id as product_id
--     from
--     (
--       select
--         user_id,
--         ad_id
--       from origin_common.cc_ods_log_gwapp_click_hourly
--       where ds='${bizdate}' and module = 'afp' and zone like 'cctproductshare%'
--     ) m1
--     inner join
--     (
--       select
--         ad_id,
--         item_id
--       from origin_common.cc_ods_fs_dwxk_ad_items_daily
--     ) m2
--     on m1.ad_id = m2.ad_id
--     inner join
--     (
--       select
--         item_id,
--         app_item_id as product_id
--       from origin_common.cc_ods_dwxk_fs_wk_items
--     ) m3
--     on m2.item_id = m3.item_id
--   ) h1
--   inner join
--   (
--     select
--       distinct cck_uid as cck_uid,
--       cct_uid
--     from origin_common.cc_ods_dwxk_fs_wk_cck_user_hourly
--     where ds='${bizdate}' and platform=14
--   ) h2
--   on h1.user_id=h2.cck_uid
--   inner join
--   (
--       select
--         distinct s2.product_id as product_id
--       from
--       (
--         select
--           ad_material_id
--         from origin_common.cc_ods_fs_cck_xb_policies_hourly
--         where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'1537977600' and end_time>='1537891200'
--       ) s1
--       inner join
--       (
--         select
--           distinct ad_material_id as ad_material_id,
--           product_id
--         from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--         where active_type=1
--       ) s2
--       on s1.ad_material_id=s2.ad_material_id
--   ) h3
--   on h1.product_id=h3.product_id
--   ) g2
--   on g1.cct_uid=g2.user_id
--   group by g1.gm_uid
-- ) t18
-- ON t1.cck_uid=t18.cck_uid
LEFT JOIN -- 抢购总经理分享
(
  select
    g1.gm_uid              as cck_uid,
    count(g2.user_id)          as self_fx_cnt
  from
  (
    select
      k1.cck_uid    as gm_uid,
      k2.cct_uid    as cct_uid
    from
    (
      select
        cck_uid
      from origin_common.cc_ods_fs_wk_cct_layer_info
      where platform=14 and type=2
    ) k1
    inner join
    (
      select
        cck_uid,
        cct_uid
      from origin_common.cc_ods_fs_tui_relation
    ) k2
    on k1.cck_uid=k2.cck_uid
  ) g1
  left join
  (
  select
    m3.product_id as product_id,
    m1.user_id    as user_id
  from
  (
    select
      ad_id,user_id
    from origin_common.cc_ods_log_cctapp_click_hourly where ds='${bizdate}' and module='detail_material' and zone in ('line','small_routine','pQrCode','promotion')
    union all
    select
      hash_value as ad_id,
      user_id
    from origin_common.cc_ods_log_cctapp_click_hourly
    where ds='${bizdate}' and module = 'share' and zone = 'show'
  ) m1
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
  on m2.item_id = m3.item_id
  inner join
  (
      select
        distinct s2.product_id as product_id
      from
      (
        select
          ad_material_id
        from origin_common.cc_ods_fs_cck_xb_policies_hourly
        where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
      ) s1
      inner join
      (
        select
          distinct ad_material_id as ad_material_id,
          product_id
        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
        where active_type!=1
      ) s2
      on s1.ad_material_id=s2.ad_material_id
      left join
      (
        select
          distinct k2.product_id as product_id
        from
        (
          select
            ad_material_id
          from origin_common.cc_ods_fs_cck_xb_policies_hourly
          where zone='productList' and ad_key in ('cct-task-bomb-product','cct-task-bomb-product-two') and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
        ) k1
        inner join
        (
          select
            distinct ad_material_id as ad_material_id,
            product_id
          from origin_common.cc_ods_fs_cck_ad_material_products_hourly
        ) k2
        on k1.ad_material_id=k2.ad_material_id
       ) s3
       on s2.product_id=s3.product_id
       where s3.product_id is null
  ) m4
  on m3.product_id = m4.product_id
  union all
  select
    h1.product_id as product_id,
    h2.cct_uid     as user_id
  from
  (
    select
      s1.user_id as user_id,
      s2.product_id as product_id
    from
    (
      select
        user_id,
        ad_material_id
      from origin_common.cc_ods_log_gwapp_click_hourly
      where ds='${bizdate}' and module in ('afp','index_share_moments') and (zone in ('footersharecctafp','headsharecctafp') or zone like 'cctproductHotAreaShare%' or zone like 'cctBannerShare%' or zone like 'list_share_%')
    ) s1
    inner join
    (
      select
        distinct ad_material_id as ad_material_id,
        product_id
      from origin_common.cc_ods_fs_cck_ad_material_products_hourly
    ) s2
    on s1.ad_material_id=s2.ad_material_id
    union all
    select
      m1.user_id as user_id,
      m3.product_id as product_id
    from
    (
      select
        user_id,
        ad_id
      from origin_common.cc_ods_log_gwapp_click_hourly
      where ds='${bizdate}' and module = 'afp' and zone like 'cctproductshare%'
    ) m1
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
    on m2.item_id = m3.item_id
  ) h1
  inner join
  (
    select
      distinct cck_uid as cck_uid,
      cct_uid
    from origin_common.cc_ods_dwxk_fs_wk_cck_user_hourly
    where ds='${bizdate}' and platform=14
  ) h2
  on h1.user_id=h2.cck_uid
  inner join
  (
      select
        distinct s2.product_id as product_id
      from
      (
        select
          ad_material_id
        from origin_common.cc_ods_fs_cck_xb_policies_hourly
        where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
      ) s1
      inner join
      (
        select
          distinct ad_material_id as ad_material_id,
          product_id
        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
        where active_type!=1
      ) s2
      on s1.ad_material_id=s2.ad_material_id
      left join
      (
        select
          distinct k2.product_id as product_id
        from
        (
          select
            ad_material_id
          from origin_common.cc_ods_fs_cck_xb_policies_hourly
          where zone='productList' and ad_key in ('cct-task-bomb-product','cct-task-bomb-product-two') and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
        ) k1
        inner join
        (
          select
            distinct ad_material_id as ad_material_id,
            product_id
          from origin_common.cc_ods_fs_cck_ad_material_products_hourly
        ) k2
        on k1.ad_material_id=k2.ad_material_id
       ) s3
       on s2.product_id=s3.product_id
       where s3.product_id is null
  ) h3
  on h1.product_id=h3.product_id
  ) g2
  on g1.cct_uid=g2.user_id
  group by g1.gm_uid
) t19
ON t1.cck_uid=t19.cck_uid
left join
--这是爆款商品的afp分享次数统计 by 王帆
(
    select
        g1.gm_uid                                          as cck_uid,
        sum(if(g2.bomb_type=1,1,0))                        as team_fx_cnt,
        count(distinct if(g2.bomb_type=1,g2.user_id,null)) as team_fx_user_cnt,
        sum(if(g2.bomb_type=2,1,0))                        as team_fx_cnt2,
        count(distinct if(g2.bomb_type=2,g2.user_id,null)) as team_fx_user_cnt2
      from
      (
        select
          k1.gm_uid     as gm_uid,
          k2.cct_uid    as cct_uid
        from
        (
          select
            gm_uid,
            cck_uid
          from origin_common.cc_ods_fs_wk_cct_layer_info
          where platform=14 and gm_uid>0
          union all
          select
            cck_uid as gm_uid,
            cck_uid
          from origin_common.cc_ods_fs_wk_cct_layer_info
          where platform=14 and type=2
        ) k1
        inner join
        (
          select
            cck_uid,
            cct_uid
          from origin_common.cc_ods_dwxk_fs_wk_cck_user
          where platform=14 and ds='${bizdate}'
        ) k2
        on k1.cck_uid=k2.cck_uid
      ) g1
      left join
      (
      select
        h1.product_id as product_id,
        h2.cct_uid     as user_id,
        h3.bomb_type   as bomb_type
      from
      (
        select
          s1.user_id as user_id,
          s2.product_id as product_id
        from
        (
          select
            user_id,
            ad_material_id
          from origin_common.cc_ods_log_gwapp_click_hourly
          where ds='${bizdate}' and module in ('afp','index_share_moments') and (zone in ('footersharecctafp','headsharecctafp') or zone like 'cctproductHotAreaShare%' or zone like 'cctBannerShare%' or zone like 'list_share_%')
        ) s1
        inner join
        (
          select
            distinct ad_material_id as ad_material_id,
            product_id
          from origin_common.cc_ods_fs_cck_ad_material_products_hourly
        ) s2
        on s1.ad_material_id=s2.ad_material_id
        union all
        select
          m1.user_id as user_id,
          m3.product_id as product_id
        from
        (
          select
            user_id,
            ad_id
          from origin_common.cc_ods_log_gwapp_click_hourly
          where ds='${bizdate}' and module = 'afp' and zone like 'cctproductshare%'
        ) m1
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
        on m2.item_id = m3.item_id
      ) h1
      inner join
      (
        select
          distinct cck_uid as cck_uid,
          cct_uid
        from origin_common.cc_ods_dwxk_fs_wk_cck_user_hourly
        where ds='${bizdate}' and platform=14
      ) h2
      on h1.user_id=h2.cck_uid
      inner join
      (
        select
          distinct s2.product_id as product_id,
          s1.bomb_type           as bomb_type
        from
        (
            select
              s.ad_material_id as ad_material_id,
              s.bomb_type      as bomb_type
            from
            (
            select
              ad_material_id,
              1 as bomb_type,
              sort
            from origin_common.cc_ods_fs_cck_xb_policies_hourly
            where zone='productList' and ad_key='cct-task-bomb-product' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
            order by sort desc
            limit 1
            ) s
            union all
            select
              h.ad_material_id as ad_material_id,
              h.bomb_type      as bomb_type
            from
            (
            select
              ad_material_id,
              2 as bomb_type,
              sort
            from origin_common.cc_ods_fs_cck_xb_policies_hourly
            where zone='productList' and ad_key='cct-task-bomb-product-two' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
            order by sort desc
            limit 1
            ) h
        )s1
        inner join
        (
           select
             distinct ad_material_id,
             product_id
           from origin_common.cc_ods_fs_cck_ad_material_products_hourly
        ) s2
        on s1.ad_material_id=s2.ad_material_id
      ) h3
      on h1.product_id=h3.product_id
      ) g2
      on g1.cct_uid=g2.user_id
      group by g1.gm_uid
)t20
on t1.cck_uid = t20.cck_uid
-- left join
-- ---这是爆款商品的场次分享统计 by 王帆
-- (
-- select
--     g1.gm_uid                                          as cck_uid,
--     sum(if(g2.bomb_type=1,1,0))                        as team_fx_cnt,
--     count(distinct if(g2.bomb_type=1,g2.user_id,null)) as team_fx_user_cnt,
--     sum(if(g2.bomb_type=2,1,0))                        as team_fx_cnt2,
--     count(distinct if(g2.bomb_type=2,g2.user_id,null)) as team_fx_user_cnt2
--   from
--   (
--     select
--       k1.gm_uid     as gm_uid,
--       k2.cct_uid    as cct_uid
--     from
--     (
--       select
--         gm_uid,
--         cck_uid
--       from origin_common.cc_ods_fs_wk_cct_layer_info
--       where platform=14 and gm_uid>0
--       union all
--       select
--         cck_uid as gm_uid,
--         cck_uid
--       from origin_common.cc_ods_fs_wk_cct_layer_info
--       where platform=14 and type=2
--     ) k1
--     inner join
--     (
--       select
--         cck_uid,
--         cct_uid
--       from origin_common.cc_ods_dwxk_fs_wk_cck_user
--       where platform = 14 and ds='${bizdate}'
--     ) k2
--     on k1.cck_uid=k2.cck_uid
--   ) g1
--   left join
--   --
--   (
--   select
--     m3.product_id as product_id,
--     m1.user_id    as user_id,
--     m4.bomb_type  as bomb_type
--   from
--   (
--     select
--       a.user_id as user_id,tmp.sub as ad_id
--     from
--     (
--       select
--           user_id,hash_value
--       from origin_common.cc_ods_log_cctapp_click_hourly where module='index_new_share' and zone in ('goods_friend','goods_circlefriend') and ds='${bizdate}'
--     )a lateral view explode(split(a.hash_value,':_:')) tmp as sub
--   ) as m1
--   inner join
--   (
--     select
--       ad_id,
--       item_id
--     from origin_common.cc_ods_fs_dwxk_ad_items_daily
--   ) m2
--   on m1.ad_id = m2.ad_id
--   inner join
--   (
--     select
--       item_id,
--       app_item_id as product_id
--     from origin_common.cc_ods_dwxk_fs_wk_items
--   ) m3
--   on m2.item_id = m3.item_id
--   inner join
--   (
--     select
--       distinct s2.product_id as product_id,
--       s1.bomb_type           as bomb_type
--     from
--     (
--         select
--           s.ad_material_id as ad_material_id,
--           s.bomb_type      as bomb_type
--         from
--         (
--         select
--           ad_material_id,
--           1 as bomb_type,
--           sort
--         from origin_common.cc_ods_fs_cck_xb_policies_hourly
--         where zone='productList' and ad_key='cct-task-bomb-product' and status!='DELETE' and begin_time<'1537977600' and end_time>='1537891200'
--         order by sort desc
--         limit 1
--         ) s
--         union all
--         select
--           h.ad_material_id as ad_material_id,
--           h.bomb_type      as bomb_type
--         from
--         (
--         select
--           ad_material_id,
--           2 as bomb_type,
--           sort
--         from origin_common.cc_ods_fs_cck_xb_policies_hourly
--         where zone='productList' and ad_key='cct-task-bomb-product-two' and status!='DELETE' and begin_time<'1537977600' and end_time>='1537891200'
--         order by sort desc
--         limit 1
--         ) h
--     )s1
--     inner join
--     (
--        select
--          distinct ad_material_id as ad_material_id,
--          product_id
--        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--     ) s2
--     on s1.ad_material_id=s2.ad_material_id
--   ) m4
--   on m3.product_id = m4.product_id
--   ) g2
--   on g1.cct_uid=g2.user_id
--   group by g1.gm_uid
-- )t21
-- on t1.cck_uid = t21.cck_uid

left join
---这是爆款商品的微信裂变统计  by 王帆
(
      select
        g1.gm_uid                                          as cck_uid,
        sum(if(g2.bomb_type=1,1,0))                        as team_fx_cnt,
        count(distinct if(g2.bomb_type=1,g2.user_id,null)) as team_fx_user_cnt,
        sum(if(g2.bomb_type=2,1,0))                        as team_fx_cnt2,
        count(distinct if(g2.bomb_type=2,g2.user_id,null)) as team_fx_user_cnt2
      from
      (
        select
          k1.gm_uid     as gm_uid,
          k2.cct_uid    as cct_uid
        from
        (
          select
            gm_uid,
            cck_uid
          from origin_common.cc_ods_fs_wk_cct_layer_info
          where platform=14 and gm_uid>0
          union all
          select
            cck_uid as gm_uid,
            cck_uid
          from origin_common.cc_ods_fs_wk_cct_layer_info
          where platform=14 and type=2
        ) k1
        inner join
        (
          select
            cck_uid,
            cct_uid
          from origin_common.cc_ods_dwxk_fs_wk_cck_user
          where ds='${bizdate}' and platform = 14
        ) k2
        on k1.cck_uid=k2.cck_uid
      ) g1
      left join
      --
      (
      select
        m3.product_id as product_id,
        m1.user_id    as user_id,
        m4.bomb_type  as bomb_type
      from
      (
        select
          user_id,
          hash_value as ad_id
        from origin_common.cc_ods_log_cctapp_click_hourly
        where ds='${bizdate}' and module = 'share' and zone = 'show'
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
      on m2.item_id = m3.item_id
      inner join
      (
        select
          distinct s2.product_id as product_id,
          s1.bomb_type           as bomb_type
        from
        (
            select
              s.ad_material_id as ad_material_id,
              s.bomb_type      as bomb_type
            from
            (
            select
              ad_material_id,
              1 as bomb_type,
              sort
            from origin_common.cc_ods_fs_cck_xb_policies_hourly
            where zone='productList' and ad_key='cct-task-bomb-product' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
            order by sort desc
            limit 1
            ) s
            union all
            select
              h.ad_material_id as ad_material_id,
              h.bomb_type      as bomb_type
            from
            (
            select
              ad_material_id,
              2 as bomb_type,
              sort
            from origin_common.cc_ods_fs_cck_xb_policies_hourly
            where zone='productList' and ad_key='cct-task-bomb-product-two' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
            order by sort desc
            limit 1
            ) h
        )s1
        inner join
        (
           select
             distinct ad_material_id as ad_material_id,
             product_id
           from origin_common.cc_ods_fs_cck_ad_material_products_hourly
        ) s2
        on s1.ad_material_id=s2.ad_material_id
      ) m4
      on m3.product_id = m4.product_id
      ) g2
      on g1.cct_uid=g2.user_id
      group by g1.gm_uid
)t22
on t1.cck_uid = t22.cck_uid

left join
---抢购afp分享统计  by wangfan
(
  select
    g1.gm_uid              as cck_uid,
    count(g2.user_id)      as team_fx_cnt,
    count(distinct g2.user_id) as team_fx_user_cnt
  from
  --这部分是团队的cck_uid
  (
    select
      k1.gm_uid     as gm_uid,
      k2.cct_uid    as cct_uid
    from
    (
      select
        gm_uid,
        cck_uid
      from origin_common.cc_ods_fs_wk_cct_layer_info
      where platform=14 and gm_uid>0
      union all
      select
        cck_uid as gm_uid,
        cck_uid
      from origin_common.cc_ods_fs_wk_cct_layer_info
      where platform=14 and type=2
    ) k1
    inner join
    (
      select
        cck_uid,
        cct_uid
      from origin_common.cc_ods_fs_tui_relation
    ) k2
    on k1.cck_uid=k2.cck_uid
  ) g1
  left join
  (
  --这部分是afp的点击商品和用户id
  select
    h1.product_id as product_id,
    h2.cct_uid     as user_id
  from
  (
    select
      s1.user_id as user_id,
      s2.product_id as product_id
    from
    (
      select
        user_id,
        ad_material_id
      from origin_common.cc_ods_log_gwapp_click_hourly
      where ds='${bizdate}' and module in ('afp','index_share_moments') and (zone in ('footersharecctafp','headsharecctafp') or zone like 'cctproductHotAreaShare%' or zone like 'cctBannerShare%' or zone like 'list_share_%')
    ) s1
    inner join
    (
      select
        distinct ad_material_id as ad_material_id,
        product_id
      from origin_common.cc_ods_fs_cck_ad_material_products_hourly
    ) s2
    on s1.ad_material_id=s2.ad_material_id
    union all
    select
      m1.user_id as user_id,
      m3.product_id as product_id
    from
    (
      select
        user_id,
        ad_id
      from origin_common.cc_ods_log_gwapp_click_hourly
      where ds='${bizdate}' and module = 'afp' and zone like 'cctproductshare%'
    ) m1
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
    on m2.item_id = m3.item_id
  ) h1
  inner join
  (
    select
      distinct cck_uid as cck_uid,
      cct_uid
    from origin_common.cc_ods_dwxk_fs_wk_cck_user_hourly
    where ds='${bizdate}' and platform=14
  ) h2
  on h1.user_id=h2.cck_uid
  --这部分还是抢购商品的id
  inner join
  (
      select
        distinct s2.product_id as product_id
      from
      (
        select
          ad_material_id
        from origin_common.cc_ods_fs_cck_xb_policies_hourly
        where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
      ) s1
      inner join
      (
        select
          distinct ad_material_id as ad_material_id,
          product_id
        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
        where active_type!=1
      ) s2
      on s1.ad_material_id=s2.ad_material_id
      left join
      (
        select
          distinct k2.product_id as product_id
        from
        (
          select
            ad_material_id
          from origin_common.cc_ods_fs_cck_xb_policies_hourly
          where zone='productList' and ad_key in ('cct-task-bomb-product','cct-task-bomb-product-two') and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
        ) k1
        inner join
        (
          select
            distinct ad_material_id as ad_material_id,
            product_id
          from origin_common.cc_ods_fs_cck_ad_material_products_hourly
        ) k2
        on k1.ad_material_id=k2.ad_material_id
       ) s3
       on s2.product_id=s3.product_id
       where s3.product_id is null
  ) h3
  on h1.product_id=h3.product_id
  ) g2
  on g1.cct_uid=g2.user_id
  group by g1.gm_uid
)t23
on t1.cck_uid = t23.cck_uid

--这部分是抢购商品的微信裂变统计  by  wangfan
left join
(
      select
        g1.gm_uid                                          as cck_uid,
        count(g2.user_id)                                  as team_fx_cnt,
        count(distinct g2.user_id)                         as team_fx_user_cnt
      from
      (
        select
          k1.gm_uid     as gm_uid,
          k2.cct_uid    as cct_uid
        from
        (
          select
            gm_uid,
            cck_uid
          from origin_common.cc_ods_fs_wk_cct_layer_info
          where platform=14 and gm_uid>0
          union all
          select
            cck_uid as gm_uid,
            cck_uid
          from origin_common.cc_ods_fs_wk_cct_layer_info
          where platform=14 and type=2
        ) k1
        inner join
        (
          select
            cck_uid,
            cct_uid
          from origin_common.cc_ods_dwxk_fs_wk_cck_user
          where ds='${bizdate}' and platform = 14
        ) k2
        on k1.cck_uid=k2.cck_uid
      ) g1

      left join
      --
      (
      select
        m3.product_id as product_id,
        m1.user_id    as user_id
      from
      (
        select
          user_id,
          hash_value as ad_id
        from origin_common.cc_ods_log_cctapp_click_hourly
        where ds='${bizdate}' and module = 'share' and zone = 'show'
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
      on m2.item_id = m3.item_id
      inner join
      --这里面提供秒杀商品的id
      (
        select
            distinct s2.product_id as product_id
        from
        (
          select
            ad_material_id
          from origin_common.cc_ods_fs_cck_xb_policies_hourly
          where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
        ) s1
        inner join
        (
          select
            distinct ad_material_id as ad_material_id,
            product_id
          from origin_common.cc_ods_fs_cck_ad_material_products_hourly
          where active_type!=1
        ) s2
        on s1.ad_material_id=s2.ad_material_id
        left join
        (
          select
            distinct k2.product_id as product_id
          from
          (
            select
              ad_material_id
            from origin_common.cc_ods_fs_cck_xb_policies_hourly
            where zone='productList' and ad_key in ('cct-task-bomb-product','cct-task-bomb-product-two') and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
          ) k1
          inner join
          (
            select
              distinct ad_material_id as ad_material_id,
              product_id
            from origin_common.cc_ods_fs_cck_ad_material_products_hourly
          ) k2
          on k1.ad_material_id=k2.ad_material_id
         ) s3
         on s2.product_id=s3.product_id
         where s3.product_id is null
      ) m4
      on m3.product_id = m4.product_id
      ) g2
      on g1.cct_uid=g2.user_id
      group by g1.gm_uid
)t24
on t1.cck_uid = t24.cck_uid
left join

----这是抢购商品的场次分享统计 by  wangfan
(
       select
        g1.gm_uid                                          as cck_uid,
        count(g2.user_id)                                  as team_fx_cnt,
        count(distinct g2.user_id)                         as team_fx_user_cnt
  from
  (
    select
      k1.gm_uid     as gm_uid,
      k2.cct_uid    as cct_uid
    from
    (
      select
        gm_uid,
        cck_uid
      from origin_common.cc_ods_fs_wk_cct_layer_info
      where platform=14 and gm_uid>0
      union all
      select
        cck_uid as gm_uid,
        cck_uid
      from origin_common.cc_ods_fs_wk_cct_layer_info
      where platform=14 and type=2
    ) k1
    inner join
    (
      select
        cck_uid,
        cct_uid
      from origin_common.cc_ods_dwxk_fs_wk_cck_user
      where platform = 14 and ds='${bizdate}'
    ) k2
    on k1.cck_uid=k2.cck_uid
  ) g1
  left join
  --
  (
  select
    m3.product_id as product_id,
    m1.user_id    as user_id
  from
  (
    select
      a.user_id as user_id,tmp.sub as ad_id
    from
    (
      select
          user_id,hash_value
      from origin_common.cc_ods_log_cctapp_click_hourly where module='index_new_share' and zone in ('goods_friend','goods_circlefriend') and ds='${bizdate}'
    )a lateral view explode(split(a.hash_value,':_:')) tmp as sub
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
  on m2.item_id = m3.item_id
  inner join
  ---这里面的是抢购商品的id
  (
      select
            distinct s2.product_id as product_id
        from
        (
          select
            ad_material_id
          from origin_common.cc_ods_fs_cck_xb_policies_hourly
          where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
        ) s1
        inner join
        (
          select
            distinct ad_material_id as ad_material_id,
            product_id
          from origin_common.cc_ods_fs_cck_ad_material_products_hourly
          where active_type!=1
        ) s2
        on s1.ad_material_id=s2.ad_material_id
        left join
        (
          select
            distinct k2.product_id as product_id
          from
          (
            select
              ad_material_id
            from origin_common.cc_ods_fs_cck_xb_policies_hourly
            where zone='productList' and ad_key in ('cct-task-bomb-product','cct-task-bomb-product-two') and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
          ) k1
          inner join
          (
            select
              distinct ad_material_id as ad_material_id,
              product_id
            from origin_common.cc_ods_fs_cck_ad_material_products_hourly
          ) k2
          on k1.ad_material_id=k2.ad_material_id
         ) s3
         on s2.product_id=s3.product_id
         where s3.product_id is null
  ) m4
  on m3.product_id = m4.product_id
  ) g2
  on g1.cct_uid=g2.user_id
  group by g1.gm_uid
)t25
on t1.cck_uid = t25.cck_uid
--INSERT OVERWRITE TABLE report.cc_rpt_cctui_gm_bkmsqg_report
--PARTITION (ds = '${bizdate}')
--SELECT
--  '${bizdate}'                                as date,
--  t1.cck_uid                                as cck_uid,
--  t1.real_name                              as real_name,
--  t1.phone                                  as phone,
--  t1.all_count                              as all_count,
--  COALESCE(t1.team_leader_cnt,0)            as team_leader_cnt,
--  COALESCE(t2.team_gmv,0)                   as team_bk_gmv,
--  0                                         as team_ms_gmv,
--  COALESCE(t3.team_gmv,0)                   as team_qg_gmv,
--  COALESCE(t5.team_out_ipv,0)               as team_bk_out_ipv,
--  0                                         as team_ms_out_ipv,
--  COALESCE(t7.team_out_ipv,0)               as team_qg_out_ipv,
--  COALESCE(t8.team_fx_cnt,0)                as team_bk_fx_cnt,
--  COALESCE(t8.team_fx_user_cnt,0)           as team_bk_fx_user_cnt,
--  0                                         as team_ms_fx_cnt,
--  0                                         as team_ms_fx_user_cnt,
--  COALESCE(t10.team_fx_cnt,0)               as team_qg_fx_cnt,
--  COALESCE(t10.team_fx_user_cnt,0)          as team_qg_fx_user_cnt,
--  COALESCE(t11.team_self_gmv,0)             as team_bk_self_gmv,
--  COALESCE(t12.team_self_gmv,0)             as team_ms_self_gmv,
--  COALESCE(t13.team_self_gmv,0)             as team_qg_self_gmv,
--  COALESCE(t14.team_gm_self_gmv,0)          as team_bk_gm_self_gmv,
--  0                                         as team_ms_gm_self_gmv,
--  COALESCE(t16.team_gm_self_gmv,0)          as team_qg_gm_self_gmv,
--  COALESCE(t17.self_fx_cnt,0)               as self_bk_fx_cnt,
--  0                                         as self_ms_fx_cnt,
--  COALESCE(t19.self_fx_cnt,0)               as self_qg_fx_cnt,
--  COALESCE(t2.team_gmv2,0)                  as team_bk_gmv2,
--  COALESCE(t5.team_out_ipv2,0)              as team_bk_out_ipv2,
--  COALESCE(t8.team_fx_cnt2,0)               as team_bk_fx_cnt2,
--  COALESCE(t8.team_fx_user_cnt2,0)          as team_bk_fx_user_cnt2,
--  COALESCE(t11.team_self_gmv2,0)            as team_bk_self_gmv2,
--  COALESCE(t14.team_gm_self_gmv2,0)         as team_bk_gm_self_gmv2,
--  COALESCE(t17.self_fx_cnt2,0)              as self_bk_fx_cnt2,
--  --爆款分享afp统计 by wangfan
--  COALESCE(t20.team_fx_cnt,0)               as team_bk_fx_afp_cnt,
--  COALESCE(t20.team_fx_user_cnt,0)          as team_bk_fx_user_afp_cnt,
--  COALESCE(t20.team_fx_cnt2,0)              as team_bk_fx_afp_cnt2,
--  COALESCE(t20.team_fx_user_cnt2,0)         as team_bk_fx_user_afp_cnt2,
--  --爆款分享场次分享统计  by wangfan
--  0                                         as team_bk_fx_cc_cnt,
--  0                                         as team_bk_fx_user_cc_cnt,
--  0                                         as team_bk_fx_cc_cnt2,
--  0                                         as team_bk_fx_user_cc_cnt2,
--  --爆款分享微信裂变统计  by wangfan
--  COALESCE(t22.team_fx_cnt,0)               as team_bk_fx_wx_cnt,
--  COALESCE(t22.team_fx_user_cnt,0)          as team_bk_fx_user_wx_cnt,
--  COALESCE(t22.team_fx_cnt2,0)              as team_bk_fx_wx_cnt2,
--  COALESCE(t22.team_fx_user_cnt2,0)         as team_bk_fx_user_wx_cnt2,
--  --抢购afp，场次分享，微信裂变统计 by wangfan
--  COALESCE(t23.team_fx_cnt,0)               as team_qg_fx_afp_cnt,
--  COALESCE(t23.team_fx_user_cnt,0)          as team_qg_fx_user_afp_cnt,
--  COALESCE(t24.team_fx_cnt,0)               as team_qg_fx_wx_cnt,
--  COALESCE(t24.team_fx_user_cnt,0)          as team_qg_fx_user_wx_cnt,
--  COALESCE(t25.team_fx_cnt,0)               as team_qg_fx_cc_cnt,
--  COALESCE(t25.team_fx_user_cnt,0)          as team_qg_fx_user_cc_cnt
--
--FROM
--(
--  select
--    s1.cck_uid                  as cck_uid,
--    s3.real_name                as real_name,
--    s3.phone                    as phone,
--    s1.all_count                as all_count,
--    s2.team_leader_cnt          as team_leader_cnt
--  from
--  (
--    select
--      cck_uid,
--      all_count
--    from origin_common.cc_ods_fs_wk_cct_layer_info
--    where platform=14 and type=2 and is_del=0
--  ) s1
--  left join
--  (
--    select
--      gm_uid,
--      count(cck_uid) as team_leader_cnt
--    from origin_common.cc_ods_fs_wk_cct_layer_info
--    where platform=14 and type=1 and is_del=0
--    group by gm_uid
--  ) s2
--  on s1.cck_uid=s2.gm_uid
--  left join
--  (
--    select
--      cck_uid,
--      real_name,
--      phone
--    from origin_common.cc_ods_dwxk_fs_wk_business_info
--    where ds='${bizdate}'
--  ) s3
--  on s1.cck_uid=s3.cck_uid
--) t1
--LEFT JOIN --爆款销售额
--(
--  select
--    g1.gm_uid    as cck_uid,
--    cast(sum(if(g2.bomb_type=1,g2.item_price,0))/100 as decimal(20,2)) as team_gmv,
--    cast(sum(if(g2.bomb_type=2,g2.item_price,0))/100 as decimal(20,2)) as team_gmv2
--  from
--  (
--    select
--      gm_uid,
--      cck_uid
--    from origin_common.cc_ods_fs_wk_cct_layer_info
--    where platform=14 and gm_uid>0
--    union all
--    select
--      cck_uid as gm_uid,
--      cck_uid
--    from origin_common.cc_ods_fs_wk_cct_layer_info
--    where platform=14 and type=2
--  ) g1
--  inner join
--  (
--    select
--      s2.cck_uid          as cck_uid,
--      s2.item_price       as item_price,
--      s1.bomb_type        as bomb_type
--    from
--    (
--      select
--        distinct s2.product_id as product_id,
--        s1.bomb_type           as bomb_type
--      from
--      (
--        select
--          s.ad_material_id as ad_material_id,
--          s.bomb_type      as bomb_type
--        from
--        (
--        select
--          ad_material_id,
--          1 as bomb_type,
--          sort
--        from origin_common.cc_ods_fs_cck_xb_policies_hourly
--        where zone='productList' and ad_key='cct-task-bomb-product' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--        order by sort desc
--        limit 1
--        ) s
--        union all
--        select
--          h.ad_material_id as ad_material_id,
--          h.bomb_type      as bomb_type
--        from
--        (
--        select
--          ad_material_id,
--          2 as bomb_type,
--          sort
--        from origin_common.cc_ods_fs_cck_xb_policies_hourly
--        where zone='productList' and ad_key='cct-task-bomb-product-two' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--        order by sort desc
--        limit 1
--        ) h
--      )s1
--      inner join
--      (
--        select
--          distinct ad_material_id as ad_material_id,
--          product_id
--        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--      ) s2
--      on s1.ad_material_id=s2.ad_material_id
--    ) s1
--    inner join
--    (
--      select
--        h1.cck_uid    as cck_uid,
--        h1.product_id as product_id,
--        h1.item_price as item_price
--      from
--      (
--        select
--          cck_uid,
--          product_id,
--          item_price
--        from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
--        where ds='${bizdate}'
--      ) h1
--      inner join
--      (
--        select
--          cck_uid
--        from origin_common.cc_ods_dwxk_fs_wk_cck_user
--        where ds='${bizdate}' and platform=14
--      ) h2
--      on h1.cck_uid=h2.cck_uid
--    ) s2
--    on s1.product_id=s2.product_id
--  ) g2
--  on g1.cck_uid=g2.cck_uid
--  group by g1.gm_uid
--) t2
--ON t1.cck_uid=t2.cck_uid
--LEFT JOIN --抢购销售额
--(
--  select
--    g1.gm_uid    as cck_uid,
--    cast(sum(g2.item_price)/100 as decimal(20,2)) as team_gmv
--  from
--  (
--    select
--      gm_uid,
--      cck_uid
--    from origin_common.cc_ods_fs_wk_cct_layer_info
--    where platform=14 and gm_uid>0
--    union all
--    select
--      cck_uid as gm_uid,
--      cck_uid
--    from origin_common.cc_ods_fs_wk_cct_layer_info
--    where platform=14 and type=2
--  ) g1
--  left join
--  (
--    select
--      s2.cck_uid          as cck_uid,
--      s2.item_price       as item_price
--    from
--    (
--      select
--        distinct s2.product_id as product_id
--      from
--      (
--        select
--          ad_material_id
--        from origin_common.cc_ods_fs_cck_xb_policies_hourly
--        where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--      ) s1
--      inner join
--      (
--        select
--          distinct ad_material_id as ad_material_id,
--          product_id
--        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--        where active_type!=1
--      ) s2
--      on s1.ad_material_id=s2.ad_material_id
--      left join
--      (
--        select
--          distinct k2.product_id as product_id
--        from
--        (
--          select
--            ad_material_id
--          from origin_common.cc_ods_fs_cck_xb_policies_hourly
--          where zone='productList' and ad_key in ('cct-task-bomb-product','cct-task-bomb-product-two') and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--        ) k1
--        inner join
--        (
--          select
--            ad_material_id,
--            product_id
--          from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--        ) k2
--        on k1.ad_material_id=k2.ad_material_id
--       ) s3
--       on s2.product_id=s3.product_id
--       where s3.product_id is null
--    ) s1
--    inner join
--    (
--      select
--        h1.cck_uid    as cck_uid,
--        h1.product_id as product_id,
--        h1.item_price as item_price
--      from
--      (
--        select
--          cck_uid,
--          product_id,
--          item_price
--        from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
--        where ds='${bizdate}'
--      ) h1
--      inner join
--      (
--        select
--          cck_uid
--        from origin_common.cc_ods_dwxk_fs_wk_cck_user
--        where ds='${bizdate}' and platform=14
--      ) h2
--      on h1.cck_uid=h2.cck_uid
--    ) s2
--    on s1.product_id=s2.product_id
--  ) g2
--  on g1.cck_uid=g2.cck_uid
--  group by g1.gm_uid
--) t3
--ON t1.cck_uid=t3.cck_uid
---- LEFT JOIN -- 秒杀销售额
---- (
----   select
----     g1.gm_uid    as cck_uid,
----     cast(sum(g2.item_price)/100 as decimal(20,2)) as team_gmv
----   from
----   (
----     select
----       gm_uid,
----       cck_uid
----     from origin_common.cc_ods_fs_wk_cct_layer_info
----     where platform=14 and gm_uid>0
----     union all
----     select
----       cck_uid as gm_uid,
----       cck_uid
----     from origin_common.cc_ods_fs_wk_cct_layer_info
----     where platform=14 and type=2
----   ) g1
----   left join
----   (
----     select
----       s2.cck_uid          as cck_uid,
----       s2.item_price       as item_price
----     from
----     (
----       select
----         distinct s2.product_id as product_id
----       from
----       (
----         select
----           ad_material_id
----         from origin_common.cc_ods_fs_cck_xb_policies_hourly
----         where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'1537977600' and end_time>='1537891200'
----       ) s1
----       inner join
----       (
----         select
----           distinct ad_material_id as ad_material_id,
----           product_id
----         from origin_common.cc_ods_fs_cck_ad_material_products_hourly
----         where active_type=1
----       ) s2
----       on s1.ad_material_id=s2.ad_material_id
----     ) s1
----     inner join
----     (
----       select
----         h1.cck_uid    as cck_uid,
----         h1.product_id as product_id,
----         h1.item_price as item_price
----       from
----       (
----         select
----           cck_uid,
----           product_id,
----           item_price
----         from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
----         where ds='${bizdate}'
----       ) h1
----       inner join
----       (
----         select
----           cck_uid
----         from origin_common.cc_ods_dwxk_fs_wk_cck_user
----         where ds='${bizdate}' and platform=14
----       ) h2
----       on h1.cck_uid=h2.cck_uid
----     ) s2
----     on s1.product_id=s2.product_id
----   ) g2
----   on g1.cck_uid=g2.cck_uid
----   group by g1.gm_uid
---- ) t4
---- ON t1.cck_uid=t4.cck_uid
--LEFT JOIN -- 爆款站外ipv
--(
--  select
--    g1.gm_uid                 as cck_uid,
--    sum(if(g2.bomb_type=1,1,0))      as team_out_ipv,
--    sum(if(g2.bomb_type=2,1,0))      as team_out_ipv2
--  from
--  (
--    select
--      gm_uid,
--      cck_uid
--    from origin_common.cc_ods_fs_wk_cct_layer_info
--    where platform=14 and gm_uid>0
--    union all
--    select
--      cck_uid as gm_uid,
--      cck_uid
--    from origin_common.cc_ods_fs_wk_cct_layer_info
--    where platform=14 and type=2
--  ) g1
--  left join
--  (
--    select
--      s2.cck_uid          as cck_uid,
--      s2.product_id       as product_id,
--      s1.bomb_type        as bomb_type
--    from
--    (
--      select
--        distinct s2.product_id as product_id,
--        s1.bomb_type           as bomb_type
--      from
--      (
--        select
--          s.ad_material_id as ad_material_id,
--          s.bomb_type      as bomb_type
--        from
--        (
--        select
--          ad_material_id,
--          1 as bomb_type,
--          sort
--        from origin_common.cc_ods_fs_cck_xb_policies_hourly
--        where zone='productList' and ad_key='cct-task-bomb-product' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--        order by sort desc
--        limit 1
--        ) s
--        union all
--        select
--          h.ad_material_id as ad_material_id,
--          h.bomb_type      as bomb_type
--        from
--        (
--        select
--          ad_material_id,
--          2 as bomb_type,
--          sort
--        from origin_common.cc_ods_fs_cck_xb_policies_hourly
--        where zone='productList' and ad_key='cct-task-bomb-product-two' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--        order by sort desc
--        limit 1
--        ) h
--      )s1
--      inner join
--      (
--        select
--          distinct ad_material_id as ad_material_id,
--          product_id
--        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--      ) s2
--      on s1.ad_material_id=s2.ad_material_id
--    ) s1
--    inner join
--    (
--      select
--        h1.cck_uid    as cck_uid,
--        h1.product_id as product_id
--      from
--      (
--        select
--          cck_uid,
--          product_id
--        from origin_common.cc_ods_log_cctui_product_coupon_detail_hourly
--        where ds='${bizdate}' and is_in_app=0 and detail_type='item'
--      ) h1
--      inner join
--      (
--        select
--          cck_uid
--        from origin_common.cc_ods_dwxk_fs_wk_cck_user
--        where ds='${bizdate}' and platform=14
--      ) h2
--      on h1.cck_uid=h2.cck_uid
--    ) s2
--    on s1.product_id=s2.product_id
--  ) g2
--  on g1.cck_uid=g2.cck_uid
--  group by g1.gm_uid
--) t5
--ON t1.cck_uid=t5.cck_uid
---- LEFT JOIN -- 秒杀站外ipv
---- (
----   select
----     g1.gm_uid                 as cck_uid,
----     count(g2.product_id)      as team_out_ipv
----   from
----   (
----     select
----       gm_uid,
----       cck_uid
----     from origin_common.cc_ods_fs_wk_cct_layer_info
----     where platform=14 and gm_uid>0
----     union all
----     select
----       cck_uid as gm_uid,
----       cck_uid
----     from origin_common.cc_ods_fs_wk_cct_layer_info
----     where platform=14 and type=2
----   ) g1
----   left join
----   (
----     select
----       s2.cck_uid          as cck_uid,
----       s2.product_id       as product_id
----     from
----     (
----       select
----         distinct s2.product_id as product_id
----       from
----       (
----         select
----           ad_material_id
----         from origin_common.cc_ods_fs_cck_xb_policies_hourly
----         where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'1537977600' and end_time>='1537891200'
----       ) s1
----       inner join
----       (
----         select
----           distinct ad_material_id as ad_material_id,
----           product_id
----         from origin_common.cc_ods_fs_cck_ad_material_products_hourly
----         where active_type=1
----       ) s2
----       on s1.ad_material_id=s2.ad_material_id
----     ) s1
----     inner join
----     (
----       select
----         h1.cck_uid    as cck_uid,
----         h1.product_id as product_id
----       from
----       (
----         select
----           cck_uid,
----           product_id
----         from origin_common.cc_ods_log_cctui_product_coupon_detail_hourly
----         where ds='${bizdate}' and is_in_app=0 and detail_type='item'
----       ) h1
----       inner join
----       (
----         select
----           cck_uid
----         from origin_common.cc_ods_dwxk_fs_wk_cck_user
----         where ds='${bizdate}' and platform=14
----       ) h2
----       on h1.cck_uid=h2.cck_uid
----     ) s2
----     on s1.product_id=s2.product_id
----   ) g2
----   on g1.cck_uid=g2.cck_uid
----   group by g1.gm_uid
---- ) t6
---- ON t1.cck_uid=t6.cck_uid
--LEFT JOIN -- 抢购站外ipv
--(
--  select
--    g1.gm_uid                 as cck_uid,
--    count(g2.product_id)      as team_out_ipv
--  from
--  (
--    select
--      gm_uid,
--      cck_uid
--    from origin_common.cc_ods_fs_wk_cct_layer_info
--    where platform=14 and gm_uid>0
--    union all
--    select
--      cck_uid as gm_uid,
--      cck_uid
--    from origin_common.cc_ods_fs_wk_cct_layer_info
--    where platform=14 and type=2
--  ) g1
--  left join
--  (
--    select
--      s2.cck_uid          as cck_uid,
--      s2.product_id       as product_id
--    from
--    (
--      select
--        distinct s2.product_id as product_id
--      from
--      (
--        select
--          ad_material_id
--        from origin_common.cc_ods_fs_cck_xb_policies_hourly
--        where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--      ) s1
--      inner join
--      (
--        select
--          distinct ad_material_id as ad_material_id,
--          product_id
--        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--        where active_type!=1
--      ) s2
--      on s1.ad_material_id=s2.ad_material_id
--      left join
--      (
--        select
--          distinct k2.product_id as product_id
--        from
--        (
--          select
--            id
--          from origin_common.cc_ods_fs_cck_xb_policies_hourly
--          where zone='productList' and ad_key in ('cct-task-bomb-product','cct-task-bomb-product-two') and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--        ) k1
--        inner join
--        (
--          select
--            ad_material_id,
--            product_id
--          from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--        ) k2
--        on k1.id=k2.ad_material_id
--       ) s3
--       on s2.product_id=s3.product_id
--       where s3.product_id is null
--    ) s1
--    inner join
--    (
--      select
--        h1.cck_uid    as cck_uid,
--        h1.product_id as product_id
--      from
--      (
--        select
--          cck_uid,
--          product_id
--        from origin_common.cc_ods_log_cctui_product_coupon_detail_hourly
--        where ds='${bizdate}' and is_in_app=0 and detail_type='item'
--      ) h1
--      inner join
--      (
--        select
--          cck_uid
--        from origin_common.cc_ods_dwxk_fs_wk_cck_user
--        where ds='${bizdate}' and platform=14
--      ) h2
--      on h1.cck_uid=h2.cck_uid
--    ) s2
--    on s1.product_id=s2.product_id
--  ) g2
--  on g1.cck_uid=g2.cck_uid
--  group by g1.gm_uid
--) t7
--ON t1.cck_uid=t7.cck_uid
--LEFT JOIN -- 爆款分享
--(
--  select
--    g1.gm_uid                                          as cck_uid,
--    sum(if(g2.bomb_type=1,1,0))                        as team_fx_cnt,
--    count(distinct if(g2.bomb_type=1,g2.user_id,null)) as team_fx_user_cnt,
--    sum(if(g2.bomb_type=2,1,0))                        as team_fx_cnt2,
--    count(distinct if(g2.bomb_type=2,g2.user_id,null)) as team_fx_user_cnt2
--  from
--  (
--    select
--      k1.gm_uid     as gm_uid,
--      k2.cct_uid    as cct_uid
--    from
--    (
--      select
--        gm_uid,
--        cck_uid
--      from origin_common.cc_ods_fs_wk_cct_layer_info
--      where platform=14 and gm_uid>0
--      union all
--      select
--        cck_uid as gm_uid,
--        cck_uid
--      from origin_common.cc_ods_fs_wk_cct_layer_info
--      where platform=14 and type=2
--    ) k1
--    inner join
--    (
--      select
--        cck_uid,
--        cct_uid
--      from origin_common.cc_ods_fs_tui_relation
--    ) k2
--    on k1.cck_uid=k2.cck_uid
--  ) g1
--  left join
--  (
--  select
--    m3.product_id as product_id,
--    m1.user_id    as user_id,
--    m4.bomb_type  as bomb_type
--  from
--  (
--    select
--      user_id,ad_id
--    from origin_common.cc_ods_log_cctapp_click_hourly where ds='${bizdate}' and module='detail_material' and zone in ('line','small_routine','pQrCode','promotion')
--  ) as m1
--  inner join
--  (
--    select
--      ad_id,
--      item_id
--    from origin_common.cc_ods_fs_dwxk_ad_items_daily
--  ) m2
--  on m1.ad_id = m2.ad_id
--  inner join
--  (
--    select
--      item_id,
--      app_item_id as product_id
--    from origin_common.cc_ods_dwxk_fs_wk_items
--  ) m3
--  on m2.item_id = m3.item_id
--  inner join
--  (
--    select
--      distinct s2.product_id as product_id,
--      s1.bomb_type           as bomb_type
--    from
--    (
--        select
--          s.ad_material_id as ad_material_id,
--          s.bomb_type      as bomb_type
--        from
--        (
--        select
--          ad_material_id,
--          1 as bomb_type,
--          sort
--        from origin_common.cc_ods_fs_cck_xb_policies_hourly
--        where zone='productList' and ad_key='cct-task-bomb-product' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--        order by sort desc
--        limit 1
--        ) s
--        union all
--        select
--          h.ad_material_id as ad_material_id,
--          h.bomb_type      as bomb_type
--        from
--        (
--        select
--          ad_material_id,
--          2 as bomb_type,
--          sort
--        from origin_common.cc_ods_fs_cck_xb_policies_hourly
--        where zone='productList' and ad_key='cct-task-bomb-product-two' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--        order by sort desc
--        limit 1
--        ) h
--    )s1
--    inner join
--    (
--       select
--         distinct ad_material_id as ad_material_id,
--         product_id
--       from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--    ) s2
--    on s1.ad_material_id=s2.ad_material_id
--  ) m4
--  on m3.product_id = m4.product_id
--  ) g2
--  on g1.cct_uid=g2.user_id
--  group by g1.gm_uid
--) t8
--ON t1.cck_uid=t8.cck_uid
---- LEFT JOIN -- 秒杀分享
---- (
----   select
----     g1.gm_uid                  as cck_uid,
----     count(g2.user_id)          as team_fx_cnt,
----     count(distinct g2.user_id) as team_fx_user_cnt
----   from
----   (
----     select
----       k1.gm_uid        as gm_uid,
----       k2.cct_uid       as cct_uid
----     from
----     (
----       select
----         gm_uid,
----         cck_uid
----       from origin_common.cc_ods_fs_wk_cct_layer_info
----       where platform=14 and gm_uid>0
----       union all
----       select
----         cck_uid as gm_uid,
----         cck_uid
----       from origin_common.cc_ods_fs_wk_cct_layer_info
----       where platform=14 and type=2
----     ) k1
----     inner join
----     (
----       select
----         cck_uid,
----         cct_uid
----       from origin_common.cc_ods_fs_tui_relation
----     ) k2
----     on k1.cck_uid=k2.cck_uid
----   ) g1
----   left join
----   (
----   select
----     m3.product_id as product_id,
----     m1.user_id    as user_id
----   from
----   (
----     select
----       ad_material_id as ad_id,
----       user_id
----     from origin_common.cc_ods_log_cctapp_click_hourly
----     where ds = '${bizdate}' and ad_type in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
----     union all
----     select
----       ad_id,
----       user_id
----     from origin_common.cc_ods_log_cctapp_click_hourly
----     where ds = '${bizdate}' and ad_type not in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
----     union all
----     select
----       s2.ad_id,
----       s1.user_id
----     from
----     (
----       select
----         ad_material_id,
----         user_id
----       from origin_common.cc_ods_log_cctapp_click_hourly
----       where ds = '${bizdate}' and module='vip' and ad_type in ('single_product','9_cell') and zone in ('material_group-share','material_moments-share')
----     ) s1
----     inner join
----     (
----       select
----          distinct ad_material_id as ad_material_id,
----          ad_id
----       from data.cc_dm_gwapp_new_ad_material_relation_hourly
----       where ds = '${bizdate}'
----     ) s2
----     on s1.ad_material_id = s2.ad_material_id
----   ) as m1
----   inner join
----   (
----     select
----       ad_id,
----       item_id
----     from origin_common.cc_ods_fs_dwxk_ad_items_daily
----   ) m2
----   on m1.ad_id = m2.ad_id
----   inner join
----   (
----     select
----       item_id,
----       app_item_id as product_id
----     from origin_common.cc_ods_dwxk_fs_wk_items
----   ) m3
----   on m2.item_id = m3.item_id
----   inner join
----   (
----     select
----       distinct s2.product_id as product_id
----     from
----     (
----         select
----           ad_material_id
----         from origin_common.cc_ods_fs_cck_xb_policies_hourly
----         where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'1537977600' and end_time>='1537891200'
----       ) s1
----       inner join
----       (
----         select
----           distinct ad_material_id as ad_material_id,
----           product_id
----         from origin_common.cc_ods_fs_cck_ad_material_products_hourly
----         where active_type=1
----       ) s2
----       on s1.ad_material_id=s2.ad_material_id
----   ) m4
----   on m3.product_id = m4.product_id
----   union all
----   select
----     h1.product_id as product_id,
----     h2.cct_uid     as user_id
----   from
----   (
----     select
----       s1.user_id as user_id,
----       s2.product_id as product_id
----     from
----     (
----       select
----         user_id,
----         ad_material_id
----       from origin_common.cc_ods_log_gwapp_click_hourly
----       where ds='${bizdate}' and module in ('afp','index_share_moments') and (zone in ('footersharecctafp','headsharecctafp') or zone like 'cctproductHotAreaShare%' or zone like 'cctBannerShare%' or zone like 'list_share_%')
----     ) s1
----     inner join
----     (
----       select
----         distinct ad_material_id as ad_material_id,
----         product_id
----       from origin_common.cc_ods_fs_cck_ad_material_products_hourly
----     ) s2
----     on s1.ad_material_id=s2.ad_material_id
----     union all
----     select
----       m1.user_id as user_id,
----       m3.product_id as product_id
----     from
----     (
----       select
----         user_id,
----         ad_id
----       from origin_common.cc_ods_log_gwapp_click_hourly
----       where ds='${bizdate}' and module = 'afp' and zone like 'cctproductshare%'
----     ) m1
----     inner join
----     (
----       select
----         ad_id,
----         item_id
----       from origin_common.cc_ods_fs_dwxk_ad_items_daily
----     ) m2
----     on m1.ad_id = m2.ad_id
----     inner join
----     (
----       select
----         item_id,
----         app_item_id as product_id
----       from origin_common.cc_ods_dwxk_fs_wk_items
----     ) m3
----     on m2.item_id = m3.item_id
----   ) h1
----   inner join
----   (
----     select
----       distinct cck_uid as cck_uid,
----       cct_uid
----     from origin_common.cc_ods_dwxk_fs_wk_cck_user_hourly
----     where ds='${bizdate}' and platform=14
----   ) h2
----   on h1.user_id=h2.cck_uid
----   inner join
----   (
----       select
----         distinct s2.product_id as product_id
----       from
----       (
----         select
----           ad_material_id
----         from origin_common.cc_ods_fs_cck_xb_policies_hourly
----         where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'1537977600' and end_time>='1537891200'
----       ) s1
----       inner join
----       (
----         select
----           distinct ad_material_id as ad_material_id,
----           product_id
----         from origin_common.cc_ods_fs_cck_ad_material_products_hourly
----         where active_type=1
----       ) s2
----       on s1.ad_material_id=s2.ad_material_id
----   ) h3
----   on h1.product_id=h3.product_id
----   ) g2
----   on g1.cct_uid=g2.user_id
----   group by g1.gm_uid
---- ) t9
---- ON t1.cck_uid=t9.cck_uid
--LEFT JOIN -- 抢购分享
--(
--  select
--    g1.gm_uid              as cck_uid,
--    count(g2.user_id)      as team_fx_cnt,
--    count(distinct g2.user_id) as team_fx_user_cnt
--  from
--  (
--    select
--      k1.gm_uid     as gm_uid,
--      k2.cct_uid    as cct_uid
--    from
--    (
--      select
--        gm_uid,
--        cck_uid
--      from origin_common.cc_ods_fs_wk_cct_layer_info
--      where platform=14 and gm_uid>0
--      union all
--      select
--        cck_uid as gm_uid,
--        cck_uid
--      from origin_common.cc_ods_fs_wk_cct_layer_info
--      where platform=14 and type=2
--    ) k1
--    inner join
--    (
--      select
--        cck_uid,
--        cct_uid
--      from origin_common.cc_ods_fs_tui_relation
--    ) k2
--    on k1.cck_uid=k2.cck_uid
--  ) g1
--  left join
--  (
--  select
--    m3.product_id as product_id,
--    m1.user_id    as user_id
--  from
--  (
--    select
--      user_id,ad_id
--    from origin_common.cc_ods_log_cctapp_click_hourly where ds='${bizdate}' and module='detail_material' and zone in ('line','small_routine','pQrCode','promotion')
--  ) as m1
--  inner join
--  (
--    select
--      ad_id,
--      item_id
--    from origin_common.cc_ods_fs_dwxk_ad_items_daily
--  ) m2
--  on m1.ad_id = m2.ad_id
--  inner join
--  (
--    select
--      item_id,
--      app_item_id as product_id
--    from origin_common.cc_ods_dwxk_fs_wk_items
--  ) m3
--  on m2.item_id = m3.item_id
--  inner join
--  (
--      select
--        distinct s2.product_id as product_id
--      from
--      (
--        select
--          ad_material_id
--        from origin_common.cc_ods_fs_cck_xb_policies_hourly
--        where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--      ) s1
--      inner join
--      (
--        select
--          distinct ad_material_id as ad_material_id,
--          product_id
--        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--        where active_type!=1
--      ) s2
--      on s1.ad_material_id=s2.ad_material_id
--      left join
--      (
--        select
--          distinct k2.product_id as product_id
--        from
--        (
--          select
--            ad_material_id
--          from origin_common.cc_ods_fs_cck_xb_policies_hourly
--          where zone='productList' and ad_key in ('cct-task-bomb-product','cct-task-bomb-product-two') and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--        ) k1
--        inner join
--        (
--          select
--            distinct ad_material_id as ad_material_id,
--            product_id
--          from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--        ) k2
--        on k1.ad_material_id=k2.ad_material_id
--       ) s3
--       on s2.product_id=s3.product_id
--       where s3.product_id is null
--  ) m4
--  on m3.product_id = m4.product_id
--  ) g2
--  on g1.cct_uid=g2.user_id
--  group by g1.gm_uid
--) t10
--ON t1.cck_uid=t10.cck_uid
--left join -- 爆款团队自购
--(
--  select
--    g1.gm_uid    as cck_uid,
--    cast(sum(if(g2.bomb_type=1,g2.item_price,0))/100 as decimal(20,2)) as team_self_gmv,
--    cast(sum(if(g2.bomb_type=2,g2.item_price,0))/100 as decimal(20,2)) as team_self_gmv2
--  from
--  (
--    select
--      gm_uid,
--      cck_uid
--    from origin_common.cc_ods_fs_wk_cct_layer_info
--    where platform=14 and gm_uid>0
--    union all
--    select
--      cck_uid as gm_uid,
--      cck_uid
--    from origin_common.cc_ods_fs_wk_cct_layer_info
--    where platform=14 and type=2
--  ) g1
--  left join
--  (
--    select
--      s2.cck_uid          as cck_uid,
--      s2.item_price       as item_price,
--      s1.bomb_type        as bomb_type
--    from
--    (
--      select
--        distinct s2.product_id as product_id,
--        s1.bomb_type           as bomb_type
--      from
--      (
--        select
--          s.ad_material_id as ad_material_id,
--          s.bomb_type      as bomb_type
--        from
--        (
--        select
--          ad_material_id,
--          1 as bomb_type,
--          sort
--        from origin_common.cc_ods_fs_cck_xb_policies_hourly
--        where zone='productList' and ad_key='cct-task-bomb-product' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--        order by sort desc
--        limit 1
--        ) s
--        union all
--        select
--          h.ad_material_id as ad_material_id,
--          h.bomb_type      as bomb_type
--        from
--        (
--        select
--          ad_material_id,
--          2 as bomb_type,
--          sort
--        from origin_common.cc_ods_fs_cck_xb_policies_hourly
--        where zone='productList' and ad_key='cct-task-bomb-product-two' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--        order by sort desc
--        limit 1
--        ) h
--      )s1
--      inner join
--      (
--        select
--          distinct ad_material_id as ad_material_id,
--          product_id
--        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--      ) s2
--      on s1.ad_material_id=s2.ad_material_id
--    ) s1
--    inner join
--    (
--      select
--        h1.cck_uid    as cck_uid,
--        h1.product_id as product_id,
--        h1.item_price as item_price
--      from
--      (
--        select
--          cck_uid,
--          uid,
--          product_id,
--          item_price
--        from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
--        where ds='${bizdate}'
--      ) h1
--      inner join
--      (
--        select
--          distinct cck_uid as cck_uid,
--          cct_uid
--        from origin_common.cc_ods_dwxk_fs_wk_cck_user_hourly
--        where ds='${bizdate}' and platform=14
--      ) h2
--      on h1.cck_uid=h2.cck_uid and h1.uid=h2.cct_uid
--    ) s2
--    on s1.product_id=s2.product_id
--  ) g2
--  on g1.cck_uid=g2.cck_uid
--  group by g1.gm_uid
--) t11
--on t1.cck_uid=t11.cck_uid
--left join
--(
--  select
--    g1.gm_uid    as cck_uid,
--    cast(sum(g2.item_price)/100 as decimal(20,2)) as team_self_gmv
--  from
--  (
--    select
--      gm_uid,
--      cck_uid
--    from origin_common.cc_ods_fs_wk_cct_layer_info
--    where platform=14 and gm_uid>0
--    union all
--    select
--      cck_uid as gm_uid,
--      cck_uid
--    from origin_common.cc_ods_fs_wk_cct_layer_info
--    where platform=14 and type=2
--  ) g1
--  left join
--  (
--    select
--      s2.cck_uid          as cck_uid,
--      s2.item_price       as item_price
--    from
--    (
--      select
--        distinct s2.product_id as product_id
--      from
--      (
--        select
--          ad_material_id
--        from origin_common.cc_ods_fs_cck_xb_policies_hourly
--        where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--      ) s1
--      inner join
--      (
--        select
--          distinct ad_material_id as ad_material_id,
--          product_id
--        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--        where active_type=1
--      ) s2
--      on s1.ad_material_id=s2.ad_material_id
--    ) s1
--    inner join
--    (
--      select
--        h1.cck_uid    as cck_uid,
--        h1.product_id as product_id,
--        h1.item_price as item_price
--      from
--      (
--        select
--          cck_uid,
--          uid,
--          product_id,
--          item_price
--        from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
--        where ds='${bizdate}'
--      ) h1
--      inner join
--      (
--        select
--          distinct cck_uid as cck_uid,
--          cct_uid
--        from origin_common.cc_ods_dwxk_fs_wk_cck_user_hourly
--        where ds='${bizdate}' and platform=14
--      ) h2
--      on h1.cck_uid=h2.cck_uid and h1.uid=h2.cct_uid
--    ) s2
--    on s1.product_id=s2.product_id
--  ) g2
--  on g1.cck_uid=g2.cck_uid
--  group by g1.gm_uid
--) t12
--on t1.cck_uid=t12.cck_uid
--left join --抢购团队自购
--(
--  select
--    g1.gm_uid    as cck_uid,
--    cast(sum(g2.item_price)/100 as decimal(20,2)) as team_self_gmv
--  from
--  (
--    select
--      gm_uid,
--      cck_uid
--    from origin_common.cc_ods_fs_wk_cct_layer_info
--    where platform=14 and gm_uid>0
--    union all
--    select
--      cck_uid as gm_uid,
--      cck_uid
--    from origin_common.cc_ods_fs_wk_cct_layer_info
--    where platform=14 and type=2
--  ) g1
--  left join
--  (
--    select
--      s2.cck_uid          as cck_uid,
--      s2.item_price       as item_price
--    from
--    (
--      select
--        distinct s2.product_id as product_id
--      from
--      (
--        select
--          ad_material_id
--        from origin_common.cc_ods_fs_cck_xb_policies_hourly
--        where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--      ) s1
--      inner join
--      (
--        select
--          distinct ad_material_id as ad_material_id,
--          product_id
--        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--        where active_type!=1
--      ) s2
--      on s1.ad_material_id=s2.ad_material_id
--      left join
--      (
--        select
--          distinct k2.product_id as product_id
--        from
--        (
--          select
--            ad_material_id
--          from origin_common.cc_ods_fs_cck_xb_policies_hourly
--          where zone='productList' and ad_key in ('cct-task-bomb-product','cct-task-bomb-product-two') and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--        ) k1
--        inner join
--        (
--          select
--            ad_material_id,
--            product_id
--          from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--        ) k2
--        on k1.ad_material_id=k2.ad_material_id
--       ) s3
--       on s2.product_id=s3.product_id
--       where s3.product_id is null
--    ) s1
--    inner join
--    (
--      select
--        h1.cck_uid    as cck_uid,
--        h1.product_id as product_id,
--        h1.item_price as item_price
--      from
--      (
--        select
--          cck_uid,
--          uid,
--          product_id,
--          item_price
--        from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
--        where ds='${bizdate}'
--      ) h1
--      inner join
--      (
--        select
--          distinct cck_uid as cck_uid,
--          cct_uid
--        from origin_common.cc_ods_dwxk_fs_wk_cck_user_hourly
--        where ds='${bizdate}' and platform=14
--      ) h2
--      on h1.cck_uid=h2.cck_uid and h1.uid=h2.cct_uid
--    ) s2
--    on s1.product_id=s2.product_id
--  ) g2
--  on g1.cck_uid=g2.cck_uid
--  group by g1.gm_uid
--) t13
--on t1.cck_uid=t13.cck_uid
--left join --爆款总经理自购
--(
--    select
--    g1.cck_uid    as cck_uid,
--    cast(sum(if(g2.bomb_type=1,g2.item_price,0))/100 as decimal(20,2)) as team_gm_self_gmv,
--    cast(sum(if(g2.bomb_type=2,g2.item_price,0))/100 as decimal(20,2)) as team_gm_self_gmv2
--  from
--  (
--    select
--      cck_uid
--    from origin_common.cc_ods_fs_wk_cct_layer_info
--    where platform=14 and type=2
--  ) g1
--  left join
--  (
--    select
--      s2.cck_uid          as cck_uid,
--      s2.item_price       as item_price,
--      s1.bomb_type        as bomb_type
--    from
--    (
--      select
--        distinct s2.product_id as product_id,
--        s1.bomb_type           as bomb_type
--      from
--      (
--        select
--          s.ad_material_id as ad_material_id,
--          s.bomb_type      as bomb_type
--        from
--        (
--        select
--          ad_material_id,
--          1 as bomb_type,
--          sort
--        from origin_common.cc_ods_fs_cck_xb_policies_hourly
--        where zone='productList' and ad_key='cct-task-bomb-product' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--        order by sort desc
--        limit 1
--        ) s
--        union all
--        select
--          h.ad_material_id as ad_material_id,
--          h.bomb_type      as bomb_type
--        from
--        (
--        select
--          ad_material_id,
--          2 as bomb_type,
--          sort
--        from origin_common.cc_ods_fs_cck_xb_policies_hourly
--        where zone='productList' and ad_key='cct-task-bomb-product-two' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--        order by sort desc
--        limit 1
--        ) h
--      ) s1
--      inner join
--      (
--        select
--          distinct ad_material_id as ad_material_id,
--          product_id
--        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--      ) s2
--      on s1.ad_material_id=s2.ad_material_id
--    ) s1
--    inner join
--    (
--      select
--        h1.cck_uid    as cck_uid,
--        h1.product_id as product_id,
--        h1.item_price as item_price
--      from
--      (
--        select
--          cck_uid,
--          uid,
--          product_id,
--          item_price
--        from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
--        where ds='${bizdate}'
--      ) h1
--      inner join
--      (
--        select
--          distinct cck_uid as cck_uid,
--          cct_uid
--        from origin_common.cc_ods_dwxk_fs_wk_cck_user_hourly
--        where ds='${bizdate}' and platform=14
--      ) h2
--      on h1.cck_uid=h2.cck_uid and h1.uid=h2.cct_uid
--    ) s2
--    on s1.product_id=s2.product_id
--  ) g2
--  on g1.cck_uid=g2.cck_uid
--  group by g1.cck_uid
--) t14
--on t1.cck_uid=t14.cck_uid
---- left join --秒杀总经理自购
---- (
----   select
----     g1.cck_uid    as cck_uid,
----     cast(sum(g2.item_price)/100 as decimal(20,2)) as team_gm_self_gmv
----   from
----   (
----     select
----       cck_uid
----     from origin_common.cc_ods_fs_wk_cct_layer_info
----     where platform=14 and type=2
----   ) g1
----   left join
----   (
----     select
----       s2.cck_uid          as cck_uid,
----       s2.item_price       as item_price
----     from
----     (
----       select
----         distinct s2.product_id as product_id
----       from
----       (
----         select
----           ad_material_id
----         from origin_common.cc_ods_fs_cck_xb_policies_hourly
----         where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'1537977600' and end_time>='1537891200'
----       ) s1
----       inner join
----       (
----         select
----           distinct ad_material_id as ad_material_id,
----           product_id
----         from origin_common.cc_ods_fs_cck_ad_material_products_hourly
----         where active_type=1
----       ) s2
----       on s1.ad_material_id=s2.ad_material_id
----     ) s1
----     inner join
----     (
----       select
----         h1.cck_uid    as cck_uid,
----         h1.product_id as product_id,
----         h1.item_price as item_price
----       from
----       (
----         select
----           cck_uid,
----           uid,
----           product_id,
----           item_price
----         from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
----         where ds='${bizdate}'
----       ) h1
----       inner join
----       (
----         select
----           distinct cck_uid as cck_uid,
----           cct_uid
----         from origin_common.cc_ods_dwxk_fs_wk_cck_user_hourly
----         where ds='${bizdate}' and platform=14
----       ) h2
----       on h1.cck_uid=h2.cck_uid and h1.uid=h2.cct_uid
----     ) s2
----     on s1.product_id=s2.product_id
----   ) g2
----   on g1.cck_uid=g2.cck_uid
----   group by g1.cck_uid
---- ) t15
---- on t1.cck_uid=t15.cck_uid
--left join --抢购总经理自购
--(
--  select
--    g1.cck_uid    as cck_uid,
--    cast(sum(g2.item_price)/100 as decimal(20,2)) as team_gm_self_gmv
--  from
--  (
--    select
--      cck_uid
--    from origin_common.cc_ods_fs_wk_cct_layer_info
--    where platform=14 and type=2
--  ) g1
--  left join
--  (
--    select
--      s2.cck_uid          as cck_uid,
--      s2.item_price       as item_price
--    from
--    (
--      select
--        distinct s2.product_id as product_id
--      from
--      (
--        select
--          ad_material_id
--        from origin_common.cc_ods_fs_cck_xb_policies_hourly
--        where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--      ) s1
--      inner join
--      (
--        select
--          distinct ad_material_id as ad_material_id,
--          product_id
--        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--        where active_type!=1
--      ) s2
--      on s1.ad_material_id=s2.ad_material_id
--      left join
--      (
--        select
--          distinct k2.product_id as product_id
--        from
--        (
--          select
--            ad_material_id
--          from origin_common.cc_ods_fs_cck_xb_policies_hourly
--          where zone='productList' and ad_key in ('cct-task-bomb-product','cct-task-bomb-product-two') and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--        ) k1
--        inner join
--        (
--          select
--            ad_material_id,
--            product_id
--          from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--        ) k2
--        on k1.ad_material_id=k2.ad_material_id
--       ) s3
--       on s2.product_id=s3.product_id
--       where s3.product_id is null
--    ) s1
--    inner join
--    (
--      select
--        h1.cck_uid    as cck_uid,
--        h1.product_id as product_id,
--        h1.item_price as item_price
--      from
--      (
--        select
--          cck_uid,
--          uid,
--          product_id,
--          item_price
--        from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
--        where ds='${bizdate}'
--      ) h1
--      inner join
--      (
--        select
--          distinct cck_uid as cck_uid,
--          cct_uid
--        from origin_common.cc_ods_dwxk_fs_wk_cck_user_hourly
--        where ds='${bizdate}' and platform=14
--      ) h2
--      on h1.cck_uid=h2.cck_uid and h1.uid=h2.cct_uid
--    ) s2
--    on s1.product_id=s2.product_id
--  ) g2
--  on g1.cck_uid=g2.cck_uid
--  group by g1.cck_uid
--) t16
--on t1.cck_uid=t16.cck_uid
--LEFT JOIN -- 爆款总经理分享
--(
--  select
--    g1.gm_uid                   as cck_uid,
--    sum(if(g2.bomb_type=1,1,0)) as self_fx_cnt,
--    sum(if(g2.bomb_type=2,1,0)) as self_fx_cnt2
--  from
--  (
--    select
--      k1.cck_uid    as gm_uid,
--      k2.cct_uid    as cct_uid
--    from
--    (
--      select
--        cck_uid
--      from origin_common.cc_ods_fs_wk_cct_layer_info
--      where platform=14 and type=2
--    ) k1
--    inner join
--    (
--      select
--        cck_uid,
--        cct_uid
--      from origin_common.cc_ods_dwxk_fs_wk_cck_user
--      where platform=14 and ds='${bizdate}'
--    ) k2
--    on k1.cck_uid=k2.cck_uid
--  ) g1
--  left join
--  (
--  select
--    m3.product_id as product_id,
--    m1.user_id    as user_id,
--    m4.bomb_type  as bomb_type
--  from
--  (
--    select
--      user_id,ad_id
--    from origin_common.cc_ods_log_cctapp_click_hourly where ds='${bizdate}' and module='detail_material' and zone in ('line','small_routine','pQrCode','promotion')
--  ) as m1
--  inner join
--  (
--    select
--      ad_id,
--      item_id
--    from origin_common.cc_ods_fs_dwxk_ad_items_daily
--  ) m2
--  on m1.ad_id = m2.ad_id
--  inner join
--  (
--    select
--      item_id,
--      app_item_id as product_id
--    from origin_common.cc_ods_dwxk_fs_wk_items
--  ) m3
--  on m2.item_id = m3.item_id
--  inner join
--  (
--    select
--      distinct s2.product_id as product_id,
--      s1.bomb_type           as bomb_type
--    from
--    (
--        select
--          s.ad_material_id as ad_material_id,
--          s.bomb_type      as bomb_type
--        from
--        (
--        select
--          ad_material_id,
--          1 as bomb_type,
--          sort
--        from origin_common.cc_ods_fs_cck_xb_policies_hourly
--        where zone='productList' and ad_key='cct-task-bomb-product' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--        order by sort desc
--        limit 1
--        ) s
--        union all
--        select
--          h.ad_material_id as ad_material_id,
--          h.bomb_type      as bomb_type
--        from
--        (
--        select
--          ad_material_id,
--          2 as bomb_type,
--          sort
--        from origin_common.cc_ods_fs_cck_xb_policies_hourly
--        where zone='productList' and ad_key='cct-task-bomb-product-two' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--        order by sort desc
--        limit 1
--        ) h
--    )s1
--    inner join
--    (
--       select
--         distinct ad_material_id as ad_material_id,
--         product_id
--       from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--    ) s2
--    on s1.ad_material_id=s2.ad_material_id
--  ) m4
--  on m3.product_id = m4.product_id
--  union all
--  select
--    h1.product_id as product_id,
--    h2.cct_uid     as user_id,
--    h3.bomb_type   as bomb_type
--  from
--  (
--    select
--      s1.user_id as user_id,
--      s2.product_id as product_id
--    from
--    (
--      select
--        user_id,
--        ad_material_id
--      from origin_common.cc_ods_log_gwapp_click_hourly
--      where ds='${bizdate}' and module in ('afp','index_share_moments') and (zone in ('footersharecctafp','headsharecctafp') or zone like 'cctproductHotAreaShare%' or zone like 'cctBannerShare%' or zone like 'list_share_%')
--      union all
--      select
--        user_id,
--        hash_value as ad_material_id
--      from origin_common.cc_ods_log_cctapp_click_hourly
--      where ds='${bizdate}' and module = 'share' and zone = 'show'
--    ) s1
--    inner join
--    (
--      select
--        distinct ad_material_id as ad_material_id,
--        product_id
--      from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--    ) s2
--    on s1.ad_material_id=s2.ad_material_id
--    union all
--    select
--      m1.user_id as user_id,
--      m3.product_id as product_id
--    from
--    (
--      select
--        user_id,
--        ad_id
--      from origin_common.cc_ods_log_gwapp_click_hourly
--      where ds='${bizdate}' and module = 'afp' and zone like 'cctproductshare%'
--    ) m1
--    inner join
--    (
--      select
--        ad_id,
--        item_id
--      from origin_common.cc_ods_fs_dwxk_ad_items_daily
--    ) m2
--    on m1.ad_id = m2.ad_id
--    inner join
--    (
--      select
--        item_id,
--        app_item_id as product_id
--      from origin_common.cc_ods_dwxk_fs_wk_items
--    ) m3
--    on m2.item_id = m3.item_id
--  ) h1
--  inner join
--  (
--    select
--      distinct cck_uid as cck_uid,
--      cct_uid
--    from origin_common.cc_ods_dwxk_fs_wk_cck_user_hourly
--    where ds='${bizdate}' and platform=14
--  ) h2
--  on h1.user_id=h2.cck_uid
--  inner join
--  (
--    select
--      distinct s2.product_id as product_id,
--      s1.bomb_type           as bomb_type
--    from
--    (
--        select
--          s.ad_material_id as ad_material_id,
--          s.bomb_type      as bomb_type
--        from
--        (
--        select
--          ad_material_id,
--          1 as bomb_type,
--          sort
--        from origin_common.cc_ods_fs_cck_xb_policies_hourly
--        where zone='productList' and ad_key='cct-task-bomb-product' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--        order by sort desc
--        limit 1
--        ) s
--        union all
--        select
--          h.ad_material_id as ad_material_id,
--          h.bomb_type      as bomb_type
--        from
--        (
--        select
--          ad_material_id,
--          2 as bomb_type,
--          sort
--        from origin_common.cc_ods_fs_cck_xb_policies_hourly
--        where zone='productList' and ad_key='cct-task-bomb-product-two' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--        order by sort desc
--        limit 1
--        ) h
--    )s1
--    inner join
--    (
--       select
--         distinct ad_material_id as ad_material_id,
--         product_id
--       from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--    ) s2
--    on s1.ad_material_id=s2.ad_material_id
--  ) h3
--  on h1.product_id=h3.product_id
--  ) g2
--  on g1.cct_uid=g2.user_id
--  group by g1.gm_uid
--) t17
--ON t1.cck_uid=t17.cck_uid
---- LEFT JOIN -- 秒杀总经理分享
---- (
----   select
----     g1.gm_uid                  as cck_uid,
----     count(g2.user_id)          as self_fx_cnt
----   from
----   (
----     select
----       k1.cck_uid       as gm_uid,
----       k2.cct_uid       as cct_uid
----     from
----     (
----       select
----         cck_uid
----       from origin_common.cc_ods_fs_wk_cct_layer_info
----       where platform=14 and type=2
----     ) k1
----     inner join
----     (
----       select
----         cck_uid,
----         cct_uid
----       from origin_common.cc_ods_fs_tui_relation
----     ) k2
----     on k1.cck_uid=k2.cck_uid
----   ) g1
----   left join
----   (
----   select
----     m3.product_id as product_id,
----     m1.user_id    as user_id
----   from
----   (
----     select
----       ad_material_id as ad_id,
----       user_id
----     from origin_common.cc_ods_log_cctapp_click_hourly
----     where ds = '${bizdate}' and ad_type in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
----     union all
----     select
----       ad_id,
----       user_id
----     from origin_common.cc_ods_log_cctapp_click_hourly
----     where ds = '${bizdate}' and ad_type not in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
----     union all
----     select
----       s2.ad_id,
----       s1.user_id
----     from
----     (
----       select
----         ad_material_id,
----         user_id
----       from origin_common.cc_ods_log_cctapp_click_hourly
----       where ds = '${bizdate}' and module='vip' and ad_type in ('single_product','9_cell') and zone in ('material_group-share','material_moments-share')
----     ) s1
----     inner join
----     (
----       select
----          distinct ad_material_id as ad_material_id,
----          ad_id
----       from data.cc_dm_gwapp_new_ad_material_relation_hourly
----       where ds = '${bizdate}'
----     ) s2
----     on s1.ad_material_id = s2.ad_material_id
----   ) as m1
----   inner join
----   (
----     select
----       ad_id,
----       item_id
----     from origin_common.cc_ods_fs_dwxk_ad_items_daily
----   ) m2
----   on m1.ad_id = m2.ad_id
----   inner join
----   (
----     select
----       item_id,
----       app_item_id as product_id
----     from origin_common.cc_ods_dwxk_fs_wk_items
----   ) m3
----   on m2.item_id = m3.item_id
----   inner join
----   (
----     select
----       distinct s2.product_id as product_id
----     from
----     (
----         select
----           ad_material_id
----         from origin_common.cc_ods_fs_cck_xb_policies_hourly
----         where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'1537977600' and end_time>='1537891200'
----       ) s1
----       inner join
----       (
----         select
----           distinct ad_material_id as ad_material_id,
----           product_id
----         from origin_common.cc_ods_fs_cck_ad_material_products_hourly
----         where active_type=1
----       ) s2
----       on s1.ad_material_id=s2.ad_material_id
----   ) m4
----   on m3.product_id = m4.product_id
----   union all
----   select
----     h1.product_id as product_id,
----     h2.cct_uid     as user_id
----   from
----   (
----     select
----       s1.user_id as user_id,
----       s2.product_id as product_id
----     from
----     (
----       select
----         user_id,
----         ad_material_id
----       from origin_common.cc_ods_log_gwapp_click_hourly
----       where ds='${bizdate}' and module in ('afp','index_share_moments') and (zone in ('footersharecctafp','headsharecctafp') or zone like 'cctproductHotAreaShare%' or zone like 'cctBannerShare%' or zone like 'list_share_%')
----     ) s1
----     inner join
----     (
----       select
----         distinct ad_material_id as ad_material_id,
----         product_id
----       from origin_common.cc_ods_fs_cck_ad_material_products_hourly
----     ) s2
----     on s1.ad_material_id=s2.ad_material_id
----     union all
----     select
----       m1.user_id as user_id,
----       m3.product_id as product_id
----     from
----     (
----       select
----         user_id,
----         ad_id
----       from origin_common.cc_ods_log_gwapp_click_hourly
----       where ds='${bizdate}' and module = 'afp' and zone like 'cctproductshare%'
----     ) m1
----     inner join
----     (
----       select
----         ad_id,
----         item_id
----       from origin_common.cc_ods_fs_dwxk_ad_items_daily
----     ) m2
----     on m1.ad_id = m2.ad_id
----     inner join
----     (
----       select
----         item_id,
----         app_item_id as product_id
----       from origin_common.cc_ods_dwxk_fs_wk_items
----     ) m3
----     on m2.item_id = m3.item_id
----   ) h1
----   inner join
----   (
----     select
----       distinct cck_uid as cck_uid,
----       cct_uid
----     from origin_common.cc_ods_dwxk_fs_wk_cck_user_hourly
----     where ds='${bizdate}' and platform=14
----   ) h2
----   on h1.user_id=h2.cck_uid
----   inner join
----   (
----       select
----         distinct s2.product_id as product_id
----       from
----       (
----         select
----           ad_material_id
----         from origin_common.cc_ods_fs_cck_xb_policies_hourly
----         where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'1537977600' and end_time>='1537891200'
----       ) s1
----       inner join
----       (
----         select
----           distinct ad_material_id as ad_material_id,
----           product_id
----         from origin_common.cc_ods_fs_cck_ad_material_products_hourly
----         where active_type=1
----       ) s2
----       on s1.ad_material_id=s2.ad_material_id
----   ) h3
----   on h1.product_id=h3.product_id
----   ) g2
----   on g1.cct_uid=g2.user_id
----   group by g1.gm_uid
---- ) t18
---- ON t1.cck_uid=t18.cck_uid
--LEFT JOIN -- 抢购总经理分享
--(
--  select
--    g1.gm_uid              as cck_uid,
--    count(g2.user_id)          as self_fx_cnt
--  from
--  (
--    select
--      k1.cck_uid    as gm_uid,
--      k2.cct_uid    as cct_uid
--    from
--    (
--      select
--        cck_uid
--      from origin_common.cc_ods_fs_wk_cct_layer_info
--      where platform=14 and type=2
--    ) k1
--    inner join
--    (
--      select
--        cck_uid,
--        cct_uid
--      from origin_common.cc_ods_fs_tui_relation
--    ) k2
--    on k1.cck_uid=k2.cck_uid
--  ) g1
--  left join
--  (
--  select
--    m3.product_id as product_id,
--    m1.user_id    as user_id
--  from
--  (
--    select
--      ad_id,user_id
--    from origin_common.cc_ods_log_cctapp_click_hourly where ds='${bizdate}' and module='detail_material' and zone in ('line','small_routine','pQrCode','promotion')
--    union all
--    select
--      hash_value as ad_id,
--      user_id
--    from origin_common.cc_ods_log_cctapp_click_hourly
--    where ds='${bizdate}' and module = 'share' and zone = 'show'
--  ) m1
--  inner join
--  (
--    select
--      ad_id,
--      item_id
--    from origin_common.cc_ods_fs_dwxk_ad_items_daily
--  ) m2
--  on m1.ad_id = m2.ad_id
--  inner join
--  (
--    select
--      item_id,
--      app_item_id as product_id
--    from origin_common.cc_ods_dwxk_fs_wk_items
--  ) m3
--  on m2.item_id = m3.item_id
--  inner join
--  (
--      select
--        distinct s2.product_id as product_id
--      from
--      (
--        select
--          ad_material_id
--        from origin_common.cc_ods_fs_cck_xb_policies_hourly
--        where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--      ) s1
--      inner join
--      (
--        select
--          distinct ad_material_id as ad_material_id,
--          product_id
--        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--        where active_type!=1
--      ) s2
--      on s1.ad_material_id=s2.ad_material_id
--      left join
--      (
--        select
--          distinct k2.product_id as product_id
--        from
--        (
--          select
--            ad_material_id
--          from origin_common.cc_ods_fs_cck_xb_policies_hourly
--          where zone='productList' and ad_key in ('cct-task-bomb-product','cct-task-bomb-product-two') and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--        ) k1
--        inner join
--        (
--          select
--            distinct ad_material_id as ad_material_id,
--            product_id
--          from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--        ) k2
--        on k1.ad_material_id=k2.ad_material_id
--       ) s3
--       on s2.product_id=s3.product_id
--       where s3.product_id is null
--  ) m4
--  on m3.product_id = m4.product_id
--  union all
--  select
--    h1.product_id as product_id,
--    h2.cct_uid     as user_id
--  from
--  (
--    select
--      s1.user_id as user_id,
--      s2.product_id as product_id
--    from
--    (
--      select
--        user_id,
--        ad_material_id
--      from origin_common.cc_ods_log_gwapp_click_hourly
--      where ds='${bizdate}' and module in ('afp','index_share_moments') and (zone in ('footersharecctafp','headsharecctafp') or zone like 'cctproductHotAreaShare%' or zone like 'cctBannerShare%' or zone like 'list_share_%')
--    ) s1
--    inner join
--    (
--      select
--        distinct ad_material_id as ad_material_id,
--        product_id
--      from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--    ) s2
--    on s1.ad_material_id=s2.ad_material_id
--    union all
--    select
--      m1.user_id as user_id,
--      m3.product_id as product_id
--    from
--    (
--      select
--        user_id,
--        ad_id
--      from origin_common.cc_ods_log_gwapp_click_hourly
--      where ds='${bizdate}' and module = 'afp' and zone like 'cctproductshare%'
--    ) m1
--    inner join
--    (
--      select
--        ad_id,
--        item_id
--      from origin_common.cc_ods_fs_dwxk_ad_items_daily
--    ) m2
--    on m1.ad_id = m2.ad_id
--    inner join
--    (
--      select
--        item_id,
--        app_item_id as product_id
--      from origin_common.cc_ods_dwxk_fs_wk_items
--    ) m3
--    on m2.item_id = m3.item_id
--  ) h1
--  inner join
--  (
--    select
--      distinct cck_uid as cck_uid,
--      cct_uid
--    from origin_common.cc_ods_dwxk_fs_wk_cck_user_hourly
--    where ds='${bizdate}' and platform=14
--  ) h2
--  on h1.user_id=h2.cck_uid
--  inner join
--  (
--      select
--        distinct s2.product_id as product_id
--      from
--      (
--        select
--          ad_material_id
--        from origin_common.cc_ods_fs_cck_xb_policies_hourly
--        where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--      ) s1
--      inner join
--      (
--        select
--          distinct ad_material_id as ad_material_id,
--          product_id
--        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--        where active_type!=1
--      ) s2
--      on s1.ad_material_id=s2.ad_material_id
--      left join
--      (
--        select
--          distinct k2.product_id as product_id
--        from
--        (
--          select
--            ad_material_id
--          from origin_common.cc_ods_fs_cck_xb_policies_hourly
--          where zone='productList' and ad_key in ('cct-task-bomb-product','cct-task-bomb-product-two') and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--        ) k1
--        inner join
--        (
--          select
--            distinct ad_material_id as ad_material_id,
--            product_id
--          from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--        ) k2
--        on k1.ad_material_id=k2.ad_material_id
--       ) s3
--       on s2.product_id=s3.product_id
--       where s3.product_id is null
--  ) h3
--  on h1.product_id=h3.product_id
--  ) g2
--  on g1.cct_uid=g2.user_id
--  group by g1.gm_uid
--) t19
--ON t1.cck_uid=t19.cck_uid
--left join
----这是爆款商品的afp分享次数统计 by 王帆
--(
--    select
--        g1.gm_uid                                          as cck_uid,
--        sum(if(g2.bomb_type=1,1,0))                        as team_fx_cnt,
--        count(distinct if(g2.bomb_type=1,g2.user_id,null)) as team_fx_user_cnt,
--        sum(if(g2.bomb_type=2,1,0))                        as team_fx_cnt2,
--        count(distinct if(g2.bomb_type=2,g2.user_id,null)) as team_fx_user_cnt2
--      from
--      (
--        select
--          k1.gm_uid     as gm_uid,
--          k2.cct_uid    as cct_uid
--        from
--        (
--          select
--            gm_uid,
--            cck_uid
--          from origin_common.cc_ods_fs_wk_cct_layer_info
--          where platform=14 and gm_uid>0
--          union all
--          select
--            cck_uid as gm_uid,
--            cck_uid
--          from origin_common.cc_ods_fs_wk_cct_layer_info
--          where platform=14 and type=2
--        ) k1
--        inner join
--        (
--          select
--            cck_uid,
--            cct_uid
--          from origin_common.cc_ods_dwxk_fs_wk_cck_user
--          where platform=14 and ds='${bizdate}'
--        ) k2
--        on k1.cck_uid=k2.cck_uid
--      ) g1
--      left join
--      (
--      select
--        h1.product_id as product_id,
--        h2.cct_uid     as user_id,
--        h3.bomb_type   as bomb_type
--      from
--      (
--        select
--          s1.user_id as user_id,
--          s2.product_id as product_id
--        from
--        (
--          select
--            user_id,
--            ad_material_id
--          from origin_common.cc_ods_log_gwapp_click_hourly
--          where ds='${bizdate}' and module in ('afp','index_share_moments') and (zone in ('footersharecctafp','headsharecctafp') or zone like 'cctproductHotAreaShare%' or zone like 'cctBannerShare%' or zone like 'list_share_%')
--        ) s1
--        inner join
--        (
--          select
--            distinct ad_material_id as ad_material_id,
--            product_id
--          from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--        ) s2
--        on s1.ad_material_id=s2.ad_material_id
--        union all
--        select
--          m1.user_id as user_id,
--          m3.product_id as product_id
--        from
--        (
--          select
--            user_id,
--            ad_id
--          from origin_common.cc_ods_log_gwapp_click_hourly
--          where ds='${bizdate}' and module = 'afp' and zone like 'cctproductshare%'
--        ) m1
--        inner join
--        (
--          select
--            ad_id,
--            item_id
--          from origin_common.cc_ods_fs_dwxk_ad_items_daily
--        ) m2
--        on m1.ad_id = m2.ad_id
--        inner join
--        (
--          select
--            item_id,
--            app_item_id as product_id
--          from origin_common.cc_ods_dwxk_fs_wk_items
--        ) m3
--        on m2.item_id = m3.item_id
--      ) h1
--      inner join
--      (
--        select
--          distinct cck_uid as cck_uid,
--          cct_uid
--        from origin_common.cc_ods_dwxk_fs_wk_cck_user_hourly
--        where ds='${bizdate}' and platform=14
--      ) h2
--      on h1.user_id=h2.cck_uid
--      inner join
--      (
--        select
--          distinct s2.product_id as product_id,
--          s1.bomb_type           as bomb_type
--        from
--        (
--            select
--              s.ad_material_id as ad_material_id,
--              s.bomb_type      as bomb_type
--            from
--            (
--            select
--              ad_material_id,
--              1 as bomb_type,
--              sort
--            from origin_common.cc_ods_fs_cck_xb_policies_hourly
--            where zone='productList' and ad_key='cct-task-bomb-product' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--            order by sort desc
--            limit 1
--            ) s
--            union all
--            select
--              h.ad_material_id as ad_material_id,
--              h.bomb_type      as bomb_type
--            from
--            (
--            select
--              ad_material_id,
--              2 as bomb_type,
--              sort
--            from origin_common.cc_ods_fs_cck_xb_policies_hourly
--            where zone='productList' and ad_key='cct-task-bomb-product-two' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--            order by sort desc
--            limit 1
--            ) h
--        )s1
--        inner join
--        (
--           select
--             distinct ad_material_id,
--             product_id
--           from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--        ) s2
--        on s1.ad_material_id=s2.ad_material_id
--      ) h3
--      on h1.product_id=h3.product_id
--      ) g2
--      on g1.cct_uid=g2.user_id
--      group by g1.gm_uid
--)t20
--on t1.cck_uid = t20.cck_uid
---- left join
---- ---这是爆款商品的场次分享统计 by 王帆
---- (
---- select
----     g1.gm_uid                                          as cck_uid,
----     sum(if(g2.bomb_type=1,1,0))                        as team_fx_cnt,
----     count(distinct if(g2.bomb_type=1,g2.user_id,null)) as team_fx_user_cnt,
----     sum(if(g2.bomb_type=2,1,0))                        as team_fx_cnt2,
----     count(distinct if(g2.bomb_type=2,g2.user_id,null)) as team_fx_user_cnt2
----   from
----   (
----     select
----       k1.gm_uid     as gm_uid,
----       k2.cct_uid    as cct_uid
----     from
----     (
----       select
----         gm_uid,
----         cck_uid
----       from origin_common.cc_ods_fs_wk_cct_layer_info
----       where platform=14 and gm_uid>0
----       union all
----       select
----         cck_uid as gm_uid,
----         cck_uid
----       from origin_common.cc_ods_fs_wk_cct_layer_info
----       where platform=14 and type=2
----     ) k1
----     inner join
----     (
----       select
----         cck_uid,
----         cct_uid
----       from origin_common.cc_ods_dwxk_fs_wk_cck_user
----       where platform = 14 and ds='${bizdate}'
----     ) k2
----     on k1.cck_uid=k2.cck_uid
----   ) g1
----   left join
----   --
----   (
----   select
----     m3.product_id as product_id,
----     m1.user_id    as user_id,
----     m4.bomb_type  as bomb_type
----   from
----   (
----     select
----       a.user_id as user_id,tmp.sub as ad_id
----     from
----     (
----       select
----           user_id,hash_value
----       from origin_common.cc_ods_log_cctapp_click_hourly where module="index_new_share" and zone in ("goods_friend","goods_circlefriend") and ds='${bizdate}'
----     )a lateral view explode(split(a.hash_value,':_:')) tmp as sub
----   ) as m1
----   inner join
----   (
----     select
----       ad_id,
----       item_id
----     from origin_common.cc_ods_fs_dwxk_ad_items_daily
----   ) m2
----   on m1.ad_id = m2.ad_id
----   inner join
----   (
----     select
----       item_id,
----       app_item_id as product_id
----     from origin_common.cc_ods_dwxk_fs_wk_items
----   ) m3
----   on m2.item_id = m3.item_id
----   inner join
----   (
----     select
----       distinct s2.product_id as product_id,
----       s1.bomb_type           as bomb_type
----     from
----     (
----         select
----           s.ad_material_id as ad_material_id,
----           s.bomb_type      as bomb_type
----         from
----         (
----         select
----           ad_material_id,
----           1 as bomb_type,
----           sort
----         from origin_common.cc_ods_fs_cck_xb_policies_hourly
----         where zone='productList' and ad_key='cct-task-bomb-product' and status!='DELETE' and begin_time<'1537977600' and end_time>='1537891200'
----         order by sort desc
----         limit 1
----         ) s
----         union all
----         select
----           h.ad_material_id as ad_material_id,
----           h.bomb_type      as bomb_type
----         from
----         (
----         select
----           ad_material_id,
----           2 as bomb_type,
----           sort
----         from origin_common.cc_ods_fs_cck_xb_policies_hourly
----         where zone='productList' and ad_key='cct-task-bomb-product-two' and status!='DELETE' and begin_time<'1537977600' and end_time>='1537891200'
----         order by sort desc
----         limit 1
----         ) h
----     )s1
----     inner join
----     (
----        select
----          distinct ad_material_id as ad_material_id,
----          product_id
----        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
----     ) s2
----     on s1.ad_material_id=s2.ad_material_id
----   ) m4
----   on m3.product_id = m4.product_id
----   ) g2
----   on g1.cct_uid=g2.user_id
----   group by g1.gm_uid
---- )t21
---- on t1.cck_uid = t21.cck_uid
--
--left join
-----这是爆款商品的微信裂变统计  by 王帆
--(
--      select
--        g1.gm_uid                                          as cck_uid,
--        sum(if(g2.bomb_type=1,1,0))                        as team_fx_cnt,
--        count(distinct if(g2.bomb_type=1,g2.user_id,null)) as team_fx_user_cnt,
--        sum(if(g2.bomb_type=2,1,0))                        as team_fx_cnt2,
--        count(distinct if(g2.bomb_type=2,g2.user_id,null)) as team_fx_user_cnt2
--      from
--      (
--        select
--          k1.gm_uid     as gm_uid,
--          k2.cct_uid    as cct_uid
--        from
--        (
--          select
--            gm_uid,
--            cck_uid
--          from origin_common.cc_ods_fs_wk_cct_layer_info
--          where platform=14 and gm_uid>0
--          union all
--          select
--            cck_uid as gm_uid,
--            cck_uid
--          from origin_common.cc_ods_fs_wk_cct_layer_info
--          where platform=14 and type=2
--        ) k1
--        inner join
--        (
--          select
--            cck_uid,
--            cct_uid
--          from origin_common.cc_ods_dwxk_fs_wk_cck_user
--          where ds='${bizdate}' and platform = 14
--        ) k2
--        on k1.cck_uid=k2.cck_uid
--      ) g1
--      left join
--      --
--      (
--      select
--        m3.product_id as product_id,
--        m1.user_id    as user_id,
--        m4.bomb_type  as bomb_type
--      from
--      (
--        select
--          user_id,
--          hash_value as ad_id
--        from origin_common.cc_ods_log_cctapp_click_hourly
--        where ds='${bizdate}' and module = 'share' and zone = 'show'
--      ) as m1
--      inner join
--      (
--        select
--          ad_id,
--          item_id
--        from origin_common.cc_ods_fs_dwxk_ad_items_daily
--      ) m2
--      on m1.ad_id = m2.ad_id
--      inner join
--      (
--        select
--          item_id,
--          app_item_id as product_id
--        from origin_common.cc_ods_dwxk_fs_wk_items
--      ) m3
--      on m2.item_id = m3.item_id
--      inner join
--      (
--        select
--          distinct s2.product_id as product_id,
--          s1.bomb_type           as bomb_type
--        from
--        (
--            select
--              s.ad_material_id as ad_material_id,
--              s.bomb_type      as bomb_type
--            from
--            (
--            select
--              ad_material_id,
--              1 as bomb_type,
--              sort
--            from origin_common.cc_ods_fs_cck_xb_policies_hourly
--            where zone='productList' and ad_key='cct-task-bomb-product' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--            order by sort desc
--            limit 1
--            ) s
--            union all
--            select
--              h.ad_material_id as ad_material_id,
--              h.bomb_type      as bomb_type
--            from
--            (
--            select
--              ad_material_id,
--              2 as bomb_type,
--              sort
--            from origin_common.cc_ods_fs_cck_xb_policies_hourly
--            where zone='productList' and ad_key='cct-task-bomb-product-two' and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--            order by sort desc
--            limit 1
--            ) h
--        )s1
--        inner join
--        (
--           select
--             distinct ad_material_id as ad_material_id,
--             product_id
--           from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--        ) s2
--        on s1.ad_material_id=s2.ad_material_id
--      ) m4
--      on m3.product_id = m4.product_id
--      ) g2
--      on g1.cct_uid=g2.user_id
--      group by g1.gm_uid
--)t22
--on t1.cck_uid = t22.cck_uid
--
--left join
-----抢购afp分享统计  by wangfan
--(
--  select
--    g1.gm_uid              as cck_uid,
--    count(g2.user_id)      as team_fx_cnt,
--    count(distinct g2.user_id) as team_fx_user_cnt
--  from
--  --这部分是团队的cck_uid
--  (
--    select
--      k1.gm_uid     as gm_uid,
--      k2.cct_uid    as cct_uid
--    from
--    (
--      select
--        gm_uid,
--        cck_uid
--      from origin_common.cc_ods_fs_wk_cct_layer_info
--      where platform=14 and gm_uid>0
--      union all
--      select
--        cck_uid as gm_uid,
--        cck_uid
--      from origin_common.cc_ods_fs_wk_cct_layer_info
--      where platform=14 and type=2
--    ) k1
--    inner join
--    (
--      select
--        cck_uid,
--        cct_uid
--      from origin_common.cc_ods_fs_tui_relation
--    ) k2
--    on k1.cck_uid=k2.cck_uid
--  ) g1
--  left join
--  (
--  --这部分是afp的点击商品和用户id
--  select
--    h1.product_id as product_id,
--    h2.cct_uid     as user_id
--  from
--  (
--    select
--      s1.user_id as user_id,
--      s2.product_id as product_id
--    from
--    (
--      select
--        user_id,
--        ad_material_id
--      from origin_common.cc_ods_log_gwapp_click_hourly
--      where ds='${bizdate}' and module in ('afp','index_share_moments') and (zone in ('footersharecctafp','headsharecctafp') or zone like 'cctproductHotAreaShare%' or zone like 'cctBannerShare%' or zone like 'list_share_%')
--    ) s1
--    inner join
--    (
--      select
--        distinct ad_material_id as ad_material_id,
--        product_id
--      from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--    ) s2
--    on s1.ad_material_id=s2.ad_material_id
--    union all
--    select
--      m1.user_id as user_id,
--      m3.product_id as product_id
--    from
--    (
--      select
--        user_id,
--        ad_id
--      from origin_common.cc_ods_log_gwapp_click_hourly
--      where ds='${bizdate}' and module = 'afp' and zone like 'cctproductshare%'
--    ) m1
--    inner join
--    (
--      select
--        ad_id,
--        item_id
--      from origin_common.cc_ods_fs_dwxk_ad_items_daily
--    ) m2
--    on m1.ad_id = m2.ad_id
--    inner join
--    (
--      select
--        item_id,
--        app_item_id as product_id
--      from origin_common.cc_ods_dwxk_fs_wk_items
--    ) m3
--    on m2.item_id = m3.item_id
--  ) h1
--  inner join
--  (
--    select
--      distinct cck_uid as cck_uid,
--      cct_uid
--    from origin_common.cc_ods_dwxk_fs_wk_cck_user_hourly
--    where ds='${bizdate}' and platform=14
--  ) h2
--  on h1.user_id=h2.cck_uid
--  --这部分还是抢购商品的id
--  inner join
--  (
--      select
--        distinct s2.product_id as product_id
--      from
--      (
--        select
--          ad_material_id
--        from origin_common.cc_ods_fs_cck_xb_policies_hourly
--        where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--      ) s1
--      inner join
--      (
--        select
--          distinct ad_material_id as ad_material_id,
--          product_id
--        from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--        where active_type!=1
--      ) s2
--      on s1.ad_material_id=s2.ad_material_id
--      left join
--      (
--        select
--          distinct k2.product_id as product_id
--        from
--        (
--          select
--            ad_material_id
--          from origin_common.cc_ods_fs_cck_xb_policies_hourly
--          where zone='productList' and ad_key in ('cct-task-bomb-product','cct-task-bomb-product-two') and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--        ) k1
--        inner join
--        (
--          select
--            distinct ad_material_id as ad_material_id,
--            product_id
--          from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--        ) k2
--        on k1.ad_material_id=k2.ad_material_id
--       ) s3
--       on s2.product_id=s3.product_id
--       where s3.product_id is null
--  ) h3
--  on h1.product_id=h3.product_id
--  ) g2
--  on g1.cct_uid=g2.user_id
--  group by g1.gm_uid
--)t23
--on t1.cck_uid = t23.cck_uid
--
----这部分是抢购商品的微信裂变统计  by  wangfan
--left join
--(
--      select
--        g1.gm_uid                                          as cck_uid,
--        count(g2.user_id)                                  as team_fx_cnt,
--        count(distinct g2.user_id)                         as team_fx_user_cnt
--      from
--      (
--        select
--          k1.gm_uid     as gm_uid,
--          k2.cct_uid    as cct_uid
--        from
--        (
--          select
--            gm_uid,
--            cck_uid
--          from origin_common.cc_ods_fs_wk_cct_layer_info
--          where platform=14 and gm_uid>0
--          union all
--          select
--            cck_uid as gm_uid,
--            cck_uid
--          from origin_common.cc_ods_fs_wk_cct_layer_info
--          where platform=14 and type=2
--        ) k1
--        inner join
--        (
--          select
--            cck_uid,
--            cct_uid
--          from origin_common.cc_ods_dwxk_fs_wk_cck_user
--          where ds='${bizdate}' and platform = 14
--        ) k2
--        on k1.cck_uid=k2.cck_uid
--      ) g1
--
--      left join
--      --
--      (
--      select
--        m3.product_id as product_id,
--        m1.user_id    as user_id
--      from
--      (
--        select
--          user_id,
--          hash_value as ad_id
--        from origin_common.cc_ods_log_cctapp_click_hourly
--        where ds='${bizdate}' and module = 'share' and zone = 'show'
--      ) as m1
--      inner join
--      (
--        select
--          ad_id,
--          item_id
--        from origin_common.cc_ods_fs_dwxk_ad_items_daily
--      ) m2
--      on m1.ad_id = m2.ad_id
--      inner join
--      (
--        select
--          item_id,
--          app_item_id as product_id
--        from origin_common.cc_ods_dwxk_fs_wk_items
--      ) m3
--      on m2.item_id = m3.item_id
--      inner join
--      --这里面提供秒杀商品的id
--      (
--        select
--            distinct s2.product_id as product_id
--        from
--        (
--          select
--            ad_material_id
--          from origin_common.cc_ods_fs_cck_xb_policies_hourly
--          where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--        ) s1
--        inner join
--        (
--          select
--            distinct ad_material_id as ad_material_id,
--            product_id
--          from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--          where active_type!=1
--        ) s2
--        on s1.ad_material_id=s2.ad_material_id
--        left join
--        (
--          select
--            distinct k2.product_id as product_id
--          from
--          (
--            select
--              ad_material_id
--            from origin_common.cc_ods_fs_cck_xb_policies_hourly
--            where zone='productList' and ad_key in ('cct-task-bomb-product','cct-task-bomb-product-two') and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--          ) k1
--          inner join
--          (
--            select
--              distinct ad_material_id as ad_material_id,
--              product_id
--            from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--          ) k2
--          on k1.ad_material_id=k2.ad_material_id
--         ) s3
--         on s2.product_id=s3.product_id
--         where s3.product_id is null
--      ) m4
--      on m3.product_id = m4.product_id
--      ) g2
--      on g1.cct_uid=g2.user_id
--      group by g1.gm_uid
--)t24
--on t1.cck_uid = t24.cck_uid
--left join
--
------这是抢购商品的场次分享统计 by  wangfan
--(
--       select
--        g1.gm_uid                                          as cck_uid,
--        count(g2.user_id)                                  as team_fx_cnt,
--        count(distinct g2.user_id)                         as team_fx_user_cnt
--  from
--  (
--    select
--      k1.gm_uid     as gm_uid,
--      k2.cct_uid    as cct_uid
--    from
--    (
--      select
--        gm_uid,
--        cck_uid
--      from origin_common.cc_ods_fs_wk_cct_layer_info
--      where platform=14 and gm_uid>0
--      union all
--      select
--        cck_uid as gm_uid,
--        cck_uid
--      from origin_common.cc_ods_fs_wk_cct_layer_info
--      where platform=14 and type=2
--    ) k1
--    inner join
--    (
--      select
--        cck_uid,
--        cct_uid
--      from origin_common.cc_ods_dwxk_fs_wk_cck_user
--      where platform = 14 and ds='${bizdate}'
--    ) k2
--    on k1.cck_uid=k2.cck_uid
--  ) g1
--  left join
--  --
--  (
--  select
--    m3.product_id as product_id,
--    m1.user_id    as user_id
--  from
--  (
--    select
--      a.user_id as user_id,tmp.sub as ad_id
--    from
--    (
--      select
--          user_id,hash_value
--      from origin_common.cc_ods_log_cctapp_click_hourly where module='index_new_share' and zone in ('goods_friend','goods_circlefriend') and ds='${bizdate}'
--    )a lateral view explode(split(a.hash_value,':_:')) tmp as sub
--  ) as m1
--  inner join
--  (
--    select
--      ad_id,
--      item_id
--    from origin_common.cc_ods_fs_dwxk_ad_items_daily
--  ) m2
--  on m1.ad_id = m2.ad_id
--  inner join
--  (
--    select
--      item_id,
--      app_item_id as product_id
--    from origin_common.cc_ods_dwxk_fs_wk_items
--  ) m3
--  on m2.item_id = m3.item_id
--  inner join
--  ---这里面的是抢购商品的id
--  (
--      select
--            distinct s2.product_id as product_id
--        from
--        (
--          select
--            ad_material_id
--          from origin_common.cc_ods_fs_cck_xb_policies_hourly
--          where zone='productList' and ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')  and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--        ) s1
--        inner join
--        (
--          select
--            distinct ad_material_id as ad_material_id,
--            product_id
--          from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--          where active_type!=1
--        ) s2
--        on s1.ad_material_id=s2.ad_material_id
--        left join
--        (
--          select
--            distinct k2.product_id as product_id
--          from
--          (
--            select
--              ad_material_id
--            from origin_common.cc_ods_fs_cck_xb_policies_hourly
--            where zone='productList' and ad_key in ('cct-task-bomb-product','cct-task-bomb-product-two') and status!='DELETE' and begin_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
--          ) k1
--          inner join
--          (
--            select
--              distinct ad_material_id as ad_material_id,
--              product_id
--            from origin_common.cc_ods_fs_cck_ad_material_products_hourly
--          ) k2
--          on k1.ad_material_id=k2.ad_material_id
--         ) s3
--         on s2.product_id=s3.product_id
--         where s3.product_id is null
--  ) m4
--  on m3.product_id = m4.product_id
--  ) g2
--  on g1.cct_uid=g2.user_id
--  group by g1.gm_uid
--)t25
--on t1.cck_uid = t25.cck_uid