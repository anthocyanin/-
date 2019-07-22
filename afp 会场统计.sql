#会场统计代码
alter table
  ${hive.databases.data.name}.${hive.tables.cc_dm_cct_afp_page_source.name}
add 
  if not exists partition (ds = '${bizdate}') location '${bizdate}';
insert overwrite table ${hive.databases.data.name}.${hive.tables.cc_dm_cct_afp_page_source.name} partition (ds = '${bizdate}')

select 
    '${bizdate}' as date,--日期
    t1.page_id,--页面id
    t7.page_url,--
    t7.title,--页面名称
    case when t1.pv is null then 0 else t1.pv end,--页面pv
    case when t1.uv is null then 0 else t1.uv end,--页面uv
    case when t5.pc is null then 0 else t5.pc end,--页面pc
    case when t5.uc is null then 0 else t5.uc end,--页面uc
    case when t4.jump_uc is null then 0 else t4.jump_uc end,--坑位uc
    case when t4.jump_num is null then 0 else t4.jump_num end,--坑位数
    case when t6.ipv is null then 0 else t6.ipv end,--ipv
    case when t2.product_num is null then 0 else t2.product_num end,--商品数
    case when t9.pay_fee is null then 0.0 else t9.pay_fee end,--支付金额
    case when t9.pay_count is null then 0 else t9.pay_count end,--付款单数
    case when t9.cck_commission is null then 0 else t9.cck_commission end,--用户佣金
    case when t9.cck_count is null then 0 else t9.cck_count end,--有销量推广人数
    case when t3.page_share_cnt is null then 0 else t3.page_share_cnt end,--页面分享次数
    case when t3.page_share_user_cnt is null then 0 else t3.page_share_user_cnt end,--页面分享人数
    case when t8.product_share_user_cnt is null then 0 else t8.product_share_user_cnt end,--商品分享人数
    case when t9.pay_fee1 is null then 0.0 else t9.pay_fee1 end,
    case when t9.pay_count1 is null then 0 else t9.pay_count1 end,
    case when t9.cck_commission1 is null then 0 else t9.cck_commission1 end,
    case when t9.cck_count1 is null then 0 else t9.cck_count1 end
from
(--取页面pv，uv
    select 
        url,
        page_id,
        count(*) as pv,
        count(distinct ipaddress) as uv
    from 
        ${hive.databases.ods.name}.${hive.tables.cc_ods_log_cct_wap_afp.name}
    where 
        ds = '${bizdate}'
        and 
        (pid=14 or pid=-1)
    group by  
        url,page_id
) t1
left join
(--取页面商品数
    select 
        t1.page_id,
        count(distinct product_id) as product_num
    from
    (
        select
            *
        from 
            origin_common.cc_ods_fs_afp_cct_product
        where 
            ds='${bizdate}'
    ) t1
    join
    (
        select 
            distinct 
            id,
            LATERAL VIEW explode(split(regexp_replace(regexp_replace(module_sort,'\\\\[',''),'\\\\]',''), ',')) num as module_id--把两个中括号替换掉，在按逗号分割
        from 
            origin_common.cc_ods_fs_afp_page
        where 
            ds='${bizdate}'
            and
            module_sort!=''
    ) t2 
    on t1.page_id=t2.id and t1.module_id=t2.module_id
    group by 
        t1.page_id
) t2 
on t1.page_id=t2.page_id
left join
(
    select 
        split(c2.track,':_:')[1] as page_id,
        count(*) as page_share_cnt,
        count(distinct c1.user_id) as page_share_user_cnt
    from
    (
        select 
            hash_value,
            app_flag,
            user_id
        from 
            ${hive.databases.ods.name}.${hive.tables.cc_ods_log_gwapp_click_hourly.name}
        where 
            ds = '${bizdate}'
            and module = 'afp'
            and (zone = 'cctfloaticonshare'
            or  zone = 'headsharecctafp'
            or  zone = 'footersharecctafp')
            and (app_flag = 'cct' or app_flag = '')
    ) c1
    join
    (
        select 
            hash_value,
            track
        from 
            ${hive.databases.ods.name}.${hive.tables.cc_ods_fs_gwapp_hash_track_hourly.name}
    ) c2 
    on c1.hash_value = c2.hash_value
    group by  
        split(c2.track, ':_:')[1]
) t3 
on t1.page_id=t3.page_id
left join
(
    select 
        split(c4.track,':_:')[1] as page_id,
        count(distinct c3.user_id) as product_share_user_cnt
    from
    (
        select 
            hash_value,
            app_flag,
            user_id
        from 
            ${hive.databases.ods.name}.${hive.tables.cc_ods_log_gwapp_click_hourly.name}
        where 
            ds = '${bizdate}'
            and module = 'afp'
            and ( instr(zone, 'share') != 0 
            or    instr(zone, 'share') != 0 
                )
            and zone <> 'cctfloaticonshare'
            and zone <> 'headsharecctafp'
            and zone <> 'footersharecctafp'
            and (app_flag = 'cct' or app_flag = '')
    ) c3
    join
    (
        select 
            hash_value,
            track
        from 
            ${hive.databases.ods.name}.${hive.tables.cc_ods_fs_gwapp_hash_track_hourly.name}
    ) c4 
    on c3.hash_value = c4.hash_value
    group by 
        split(c4.track, ':_:')[1]
) t8 
on t1.page_id=t8.page_id
left join
(
    select 
        c8.page_id,
        sum(if(c8.cck_uid1=c8.cck_uid2,c8.item_price,0)) as pay_fee,
        sum(if(c8.cck_uid1!=c8.cck_uid2,c8.item_price,0)) as pay_fee1,
        --->sum(if(c8.cck_uid1=c8.cck_uid2,c8.cck_commission,0)) as cck_commission,
        sum(cck_commission) as cck_commission,
        sum(if(c8.cck_uid1!=c8.cck_uid2,c8.cck_commission,0)) as cck_commission1,
        count(distinct if(c8.cck_uid1=c8.cck_uid2,c8.order_sn,0))-max(if(c8.cck_uid1!=c8.cck_uid2,1,0)) as pay_count,
        count(distinct if(c8.cck_uid1!=c8.cck_uid2,c8.order_sn,0))-max(if(c8.cck_uid1=c8.cck_uid2,1,0)) as pay_count1,
        count(distinct if(c8.cck_uid1=c8.cck_uid2,c8.cck_uid1,0))-max(if(c8.cck_uid1!=c8.cck_uid2,1,0)) as cck_count,
        count(distinct if(c8.cck_uid1!=c8.cck_uid2,c8.cck_uid1,0))-max(if(c8.cck_uid1=c8.cck_uid2,1,0)) as cck_count1
    from
    (
        select 
            c7.page_id,
            c5.order_sn as order_sn,
            c5.cck_uid as cck_uid1,
            if(c9.cck_uid is null ,0,c9.cck_uid) as cck_uid2,
            c5.item_price as item_price,
            c5.cck_commission as cck_commission
        from
        (
            select 
                cck_uid,
                third_tradeno as order_sn,
                cast(item_price/100 as decimal(20,2)) as item_price,
                cast(cck_commission/100 as decimal(20,2)) as cck_commission,
                uid
            from 
                origin_common.cc_ods_dwxk_wk_sales_deal_ctime
            where 
                ds = '${bizdate}' 
        ) c5
        join
        (
            select 
                cck_uid,
                platform
            from 
                origin_common.cc_ods_dwxk_fs_wk_cck_user
            where 
                ds = '${bizdate}'
                and platform = 14 
        ) c6 
        on c5.cck_uid = c6.cck_uid
        join
        (
            select 
                order_sn,
                ad_material_id as page_id
            from 
                origin_common.cc_ods_log_gwapp_order_track_hourly
            where 
                ds >= '${bizdate-1}'
                and 
                ds <= '${bizdate}'
                and 
                ad_type = 'special-activity' 
        ) c7 
        on c5.order_sn = c7.order_sn
        left join
        (
            select 
                *
            from 
                origin_common.cc_ods_fs_tui_relation
        ) c9 
        on c5.uid=c9.cct_uid
    ) c8
    group by  
        c8.page_id
) t9 
on t1.page_id=t9.page_id
left join
(
    select 
        a1.page_id,
        count(distinct a1.jump_type, a1.jump_id) as jump_num,
        sum(a1.uc) jump_uc
    from
    (
        select 
            '${bizdate}' as date,
            page_id,
            jump_type,
            jump_id,
            count(distinct user_id) as uc
        from 
            ${hive.databases.data.name}.${hive.tables.cc_mid_cct_afp_click_union.name}
        where 
            ds='${bizdate}'
        group by  
            page_id, jump_type, jump_id
    ) a1
    group by  a1.page_id
) t4 
on t1.page_id=t4.page_id
left join
(
    select 
        page_id,
        count(*) as pc,
        count(distinct user_id) as uc
    from 
        ${hive.databases.data.name}.${hive.tables.cc_mid_cct_afp_click_union.name}
    where 
        ds='${bizdate}'
    group by 
        page_id
) t5 
on t1.page_id=t5.page_id
left join
(
    select 
        a3.page_id,
        count(distinct a4.user_id ) ipv
    from
    (
        select 
            distinct 
            page_id,
            product_id
        from 
            ${hive.databases.ods.name}.${hive.tables.cc_ods_fs_afp_cct_product.name}
        where ds='${bizdate}'
    ) a3
    left join
    (
        select 
        *
        from 
            ${hive.databases.ods.name}.${hive.tables.cc_ods_log_cctui_product_coupon_detail_hourly.name}
        where 
            ds='${bizdate}'
    ) a4 
    on a3.product_id=a4.product_id
    group by a3.page_id
) t6 
on t1.page_id=t6.page_id
left join
(
    select 
        *
    from 
        ${hive.databases.ods.name}.${hive.tables.cc_ods_fs_afp_page.name}
    where 
        ds='${bizdate}'
) t7 
on t1.page_id=t7.id
