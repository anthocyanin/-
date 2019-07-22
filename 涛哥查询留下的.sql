select
    '20180608'                              as date_sign,
    t1.cck_uid                              as cck_uid,
    coalesce(t2.real_name,0)                as real_name,
    coalesce(t2.phone,0)                    as phone,
    coalesce(t1.gm_uid,0)                   as gm_uid,
    coalesce(t4.real_name,0)                as gm_name,
    coalesce(t4.phone,0)                    as gm_phone,
    coalesce(t1.invite_uid,0)               as invite_uid,
    coalesce(t3.real_name,0)                as invite_name,
    coalesce(t3.phone,0)                    as invite_phone,
    coalesce(t1.team_vip_cnt,0)             as team_vip_cnt,
    coalesce(t1.team_valid_vip_cnt,0)       as team_valid_vip_cnt,
    coalesce(t5.self_commission,0)          as self_commission,
    coalesce(t5.self_order_cnt,0)           as self_order_cnt,
    coalesce(t5.self_sales_gmv,0)           as self_sales_gmv,
    coalesce(t6.team_vip_commission,0)      as team_vip_commission,
    coalesce(t6.team_vip_order_cnt,0)       as team_vip_order_cnt,
    coalesce(t6.team_vip_sales_gmv,0)       as team_vip_sales_gmv,
    coalesce(t7.team_vip_app_commission,0)  as team_vip_app_commission,
    coalesce(t7.team_vip_app_order_cnt,0)   as team_vip_app_order_cnt,
    coalesce(t7.team_vip_app_sales_gmv,0)   as team_vip_app_sales_gmv,
    coalesce(t1.new_team_vip_cnt_daily,0)   as new_team_vip_cnt_daily,
    coalesce(t1.new_team_vip_cnt_daily,0)   as new_team_vip_cnt_daily,
    coalesce(t8.team_vip_app_dau,0)         as team_vip_app_dau,
    coalesce(t6.team_vip_pay_dau,0)         as team_vip_pay_dau,
    coalesce(t9.team_vip_fx_cnt,0)          as team_vip_fx_cnt,
    coalesce(t9.team_vip_fx_cck_cnt,0)      as team_vip_fx_cck_cnt
from
(
    select
        s1.cck_uid as cck_uid,--总监id
        s1.gm_uid as gm_uid,--总经理id
        s1.invite_uid as invite_uid,--总监邀请人id
        s2.team_vip_cnt as team_vip_cnt,
        s2.team_valid_vip_cnt as team_valid_vip_cnt,
        s2.new_team_vip_cnt_daily as new_team_vip_cnt_daily,
        s2.new_team_valid_vip_cnt_daily as new_team_valid_vip_cnt_daily
    from
    (
        select
            cck_uid,--因为type=1.所以此时的楚客实际上是总监。楚客层级表里的楚客id包含了楚客id，总监id，总经理id。这已经是多次遇到得出的经验了。
            invite_uid,
            gm_uid
        from
            origin_common.cc_ods_fs_wk_cct_layer_info
        where
            platform = 14 and type=1
    ) s1
    left join
    (--每个总监团队的楚客数量，有效楚客数量，成为楚客时间为0608的新楚客客数量，成为楚客有效时间为0608的新有效楚客客数量，
        select
            leader_uid,
            count(cck_uid) as team_vip_cnt,
            sum(if(status=1,1,0)) as team_valid_vip_cnt,
            sum(if(from_unixtime(create_time,'yyyymmdd')='20180608',1,0)) as new_team_vip_cnt_daily,
            sum(if(from_unixtime(valid_ctime,'yyyymmdd')='20180608',1,0)) as new_team_valid_vip_cnt_daily
        from origin_common.cc_ods_fs_wk_cct_layer_info_hourly
        where type=0
        group by leader_uid
    ) s2
    on s1.cck_uid=s2.leader_uid
) t1
left join
(--返回总监的信息
    select
        cck_uid,
        phone,
        real_name
    from origin_common.cc_ods_dwxk_fs_wk_business_info
    where ds='20180607'
) t2
on t1.cck_uid=t2.cck_uid
left join
(--返回总监邀请人的信息
    select
        cck_uid,
        phone,
        real_name
    from origin_common.cc_ods_dwxk_fs_wk_business_info
    where ds='20180607'
) t3
on t1.invite_uid=t3.cck_uid
left join
(--返回总经理信息
    select
        cck_uid,
        phone,
        real_name
    from origin_common.cc_ods_dwxk_fs_wk_business_info
    where ds='20180607'
) t4
on t1.gm_uid=t4.cck_uid
left join
(--返回总监自己在0608当天的的销售支付金额，佣金，订单数
    select
        cck_uid,
        cast(sum(item_price)/100 as decimal(20,2)) as self_sales_gmv,
        cast(sum(cck_commission)/100 as decimal(20,2)) as self_commission,
        count(distinct third_tradeno) as self_order_cnt
    from origin_common.cc_ods_dwxk_wk_sales_deal_hourly
    where ds='20180608'
    group by cck_uid
) t5
on t1.cck_uid=t5.cck_uid
left join
(--返回总监整个团队的推广和自买销售情况，且包含总监自己的销售数据
    select
        s2.leader_uid              as cck_uid,
        count(distinct s1.cck_uid) as team_vip_pay_dau,--每个总监团队的动销人数
        sum(s1.team_pay_fee)       as team_vip_sales_gmv,
        sum(s1.team_commission)    as team_vip_commission,
        sum(s1.team_pay_order_cnt) as team_vip_order_cnt
    from
    (
        select
            cck_uid,
            cast(sum(item_price)/100 as decimal(20,2)) as team_pay_fee,
            cast(sum(cck_commission)/100 as decimal(20,2)) as team_commission,
            count(distinct third_tradeno) as team_pay_order_cnt
        from origin_common.cc_ods_dwxk_wk_sales_deal_hourly
        where ds='20180608'
        group by cck_uid
    ) s1
    inner join
    (
        select
            cck_uid,
            leader_uid
        from origin_common.cc_ods_fs_wk_cct_layer_info_hourly
        where platform=14
    ) s2
    on s1.cck_uid=s2.cck_uid
    group by s2.leader_uid
) t6
on t1.cck_uid=t6.cck_uid
left join
(--返回总监整个团队的自买销售情况，且包含总监自己的销售数据
    select
        s2.leader_uid              as cck_uid,
        sum(s1.team_pay_fee)       as team_vip_app_sales_gmv,
        sum(s1.team_commission)    as team_vip_app_commission,
        sum(s1.team_pay_order_cnt) as team_vip_app_order_cnt
    from
    (
        select
            h1.cck_uid as cck_uid,
            cast(sum(h1.item_price)/100 as decimal(20,2)) as team_pay_fee,
            cast(sum(h1.cck_commission)/100 as decimal(20,2)) as team_commission,
            count(distinct h1.third_tradeno) as team_pay_order_cnt
        from
        (
            select
               cck_uid,
               item_price,
               cck_commission,
               third_tradeno
            from origin_common.cc_ods_dwxk_wk_sales_deal_hourly
            where ds='20180608'
        ) h1
        inner join  
        (
            select
                distinct order_sn as order_sn
            from origin_common.cc_ods_log_gwapp_order_track_hourly
            where 
                ds>=from_unixtime(unix_timestamp('20180608')-86400*2,'yyyymmdd')  --ds>=20180604
            and 
                ds<='20180608' 
            and 
                source='cctui'
        ) h2
        on h1.third_tradeno=h2.order_sn
        group by h1.cck_uid
    ) s1
    inner join
    (
        select
            cck_uid,
            leader_uid
        from origin_common.cc_ods_fs_wk_cct_layer_info_hourly
        where platform=14
    ) s2
    on s1.cck_uid=s2.cck_uid
    group by s2.leader_uid
) t7
on t1.cck_uid=t7.cck_uid
left join
(--返回每个总监团队下面0806当天的的活跃vip人数
    select
       s1.leader_uid as cck_uid,
       count(distinct s2.user_id) as team_vip_app_dau
    from
    (
        select
            cck_uid,
            leader_uid
        from origin_common.cc_ods_fs_wk_cct_layer_info_hourly
        where platform=14
    ) s1
    inner join
    (
        select
            distinct user_id as user_id
        from origin_common.cc_ods_log_gwapp_pv_hourly
        where ds = '20180608' and module  = 'https://app-h5.daweixinke.com/chuchutui/index.html'
    ) s2
    on s1.cck_uid=s2.user_id
    group by s1.leader_uid
) t8
on t1.cck_uid=t8.cck_uid
left join
(--返回每个总监团队的VIP在0608当天的推广人数和推广次数
    select
       m3.leader_uid as cck_uid,
       count(m1.user_id) as team_vip_fx_cnt,
       count(distinct m1.user_id) as team_vip_fx_cck_cnt
    from
    (
        select
            user_id
        from origin_common.cc_ods_log_cctapp_click_hourly
        where ds = '20180608' and 
            ((ad_type in ('search','category') and module in ('detail','detail_app') and zone = 'spread') or (module='vip' and ad_type in ('single_product','9_cell') and zone in ('material_group-share','material_moments-share')))
    ) m1
    inner join
    (
        select
           cct_uid,
           cck_uid
        from origin_common.cc_ods_fs_tui_relation_hourly
    ) m2
    on m1.user_id=m2.cct_uid
    inner join
    (
        select
            cck_uid,
            leader_uid
        from origin_common.cc_ods_fs_wk_cct_layer_info_hourly
        where platform=14
    ) m3
    on m2.cck_uid = m3.cck_uid
    group by m3.leader_uid
) t9
on t1.cck_uid=t9.cck_uid


select ds,hour,count(1) as cnt from cc_ods_log_cctapp_click_hourly where ds=20180607 group by ds,hour