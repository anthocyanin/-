select
    n1.item_price        as pay_fee,--支付金额
    n1.order_count       as order_count,--订单数
    n1.sales_num         as sales_num,--销量
    n1.cck_commission    as cck_commission,--直接佣金
    n1.user_count        as user_count,--有成交用户数
    n2.pv                as pv,--站内pv
    n2.ipv_uv            as ipv_uv,--站内uv
    n3.user_count        as self_user_count,--自买用户数
    n3.order_count       as self_order_count,--自买订单数
    n4.fx_user_cnt       as detail_fx_user_cnt,--详情页推广人数
    n5.fx_user_cnt       as fx_user_cnt,--总推广人数
    n5.fx_cnt            as fx_cnt,--总推广次数
    n6.cck_count         as cck_count,--有效推广人数站外打开详情页 
    n6.pv                as pv,--推广pv
    n7.user_count        as fx_od_user_cnt,--有推广订单用户数
    n8.new_user_num      as new_user_num,
    n9.new_self_user_num as new_self_user_num
from
(--订单数，支付金额，购买人数，佣金
    select
        product_id,
        count(third_tradeno)    as order_count, --订单数
        sum(sale_num)           as sales_num,  --销量
        count(distinct cck_uid) as user_count, --有成交用户数
        sum(item_price/100)     as item_price,  --支付金额
        sum(cck_commission/100) as cck_commission --佣金
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where
        ds = '${stat_date}'
    and
        product_id = '${product_id}'
    group by product_id
)  as n1
left join
(
    select
        a1.product_id,
        count(a1.cck_uid) as pv,--站内pv
        count(distinct a1.cck_uid) as cck_count,--应该是站内楚客uv
        count(distinct a1.user_id) as ipv_uv--站内uv
    from
    (
        select
            product_id,
            cck_uid,
            user_id
        from 
            origin_common.cc_ods_log_cctui_product_coupon_detail_hourly 
        where 
            ds = '${stat_date}' 
        and 
            product_id = '${product_id}' 
        and 
            detail_type='item' 
        and 
            is_in_app = 1
    ) as a1 
    group by a1.product_id
) as n2
on n1.product_id = n2.product_id
left join
(
    select
        a0.product_id,
        count(distinct a0.third_tradeno) as order_count,--自买订单数
        count(distinct a0.cck_uid) as user_count--自买用户数
    from
    (
        select
            product_id,
            third_tradeno,
            cck_uid
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where 
            ds = '${stat_date}' 
        and 
            product_id = '${product_id}'
    ) as a0
    inner join
    (
       select 
           distinct 
           order_sn
       from  
           origin_common.cc_ods_log_gwapp_order_track_hourly
       where 
           ds = '${stat_date}' 
       and 
           source='cctui'
    ) as a1
    on a0.third_tradeno = a1.order_sn
    group by a0.product_id
) as n3
on n1.product_id = n3.product_id
left join
(
    select
        m3.product_id,
        count(m1.user_id) as fx_cnt,--详情页推广次数吧
        count(distinct m1.user_id) as fx_user_cnt--详情页 推广=分享 人数
    from
    (
        select
            ad_material_id as ad_id,
            user_id
        from 
            origin_common.cc_ods_log_cctapp_click_hourly
        where 
            ds = '${stat_date}' 
        and 
            ad_type in ('search','category') 
        and 
            module = 'detail_material' 
        and 
            zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC','link_Circle','link_friends','link_copy','small_routine')
        union all
        select
            ad_id,
            user_id
        from 
            origin_common.cc_ods_log_cctapp_click_hourly
        where 
            ds = '${stat_date}' 
        and 
            ad_type not in ('search','category') 
        and 
            module = 'detail_material' 
        and 
            zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC','link_Circle','link_friends','link_copy','small_routine')
    ) as m1
    inner join
    (
        select
            ad_id,
            item_id
        from 
            origin_common.cc_ods_fs_dwxk_ad_items_daily
    ) m2
    on 
        m1.ad_id = m2.ad_id
    inner join
    (
        select
            item_id,
            app_item_id as product_id
        from 
            origin_common.cc_ods_dwxk_fs_wk_items
    ) m3
    on 
        m3.item_id = m2.item_id
    where
        m3.product_id = '${product_id}'
    group by
        m3.product_id
) as n4
on n1.product_id = n4.product_id
left join
(
    select
        m3.product_id,
        count(m1.user_id) as fx_cnt,--总推广次数
        count(distinct m1.user_id) as fx_user_cnt--总推广人数
    from
    (
        select
            ad_material_id as ad_id,
            user_id
        from 
            origin_common.cc_ods_log_cctapp_click_hourly
        where 
            ds = '${stat_date}' 
        and 
            ad_type in ('search','category') 
        and 
            module = 'detail_material' 
        and 
            zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC','link_Circle','link_friends','link_copy','small_routine')
        union all
        select
            ad_id,
            user_id
        from 
            origin_common.cc_ods_log_cctapp_click_hourly
        where 
            ds = '${stat_date}' 
        and 
            ad_type not in ('search','category') 
        and 
            module = 'detail_material' 
        and 
            zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC','link_Circle','link_friends','link_copy','small_routine')
        union all
        select
            s2.ad_id,
            s1.user_id
        from
        (
            select
                ad_material_id,
                user_id
            from
                origin_common.cc_ods_log_cctapp_click_hourly
            where 
                ds = '${stat_date}' 
            and 
                module='vip' 
            and 
                ad_type in ('single_product','9_cell') 
            and 
                zone in ('material_group-share','material_moments-share')
        ) s1
        inner join
        (
            select
                distinct 
                ad_material_id as ad_material_id,
                ad_id
            from 
                data.cc_dm_gwapp_new_ad_material_relation_hourly
            where 
                ds = '${stat_date}'
        ) s2
        on  
            s1.ad_material_id = s2.ad_material_id
    ) as m1
    inner join
    (
        select
            ad_id,
            item_id
        from 
            origin_common.cc_ods_fs_dwxk_ad_items_daily
    ) m2
    on 
        m1.ad_id = m2.ad_id
    inner join
    (
        select
            item_id,
            app_item_id as product_id
        from 
            origin_common.cc_ods_dwxk_fs_wk_items
    ) m3
    on 
        m3.item_id = m2.item_id
    where
        m3.product_id = '${product_id}'
    group by
        m3.product_id
) as n5
on n1.product_id = n5.product_id
left join
(
    select
        a1.product_id,
        count(a1.cck_uid) as pv,
        count(distinct a1.cck_uid) as cck_count,
        count(distinct a1.user_id) as ipv_uv
    from
    (
        select
            product_id,
            cck_uid,
            user_id
        from
            origin_common.cc_ods_log_cctui_product_coupon_detail_hourly 
        where
            ds = '${stat_date}'
        and
            product_id = '${product_id}'
        and
            detail_type='item'
        and
            is_in_app = 0
    ) as a1 
    group by
        a1.product_id
) as n6
on n1.product_id = n6.product_id
left join
(
    select
        a0.product_id,
        count(distinct a0.cck_uid) as user_count
    from
    (
        select
            product_id,
            third_tradeno,
            cck_uid
        from
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where
            ds = '${stat_date}'
        and
            product_id = '${product_id}'
    ) as a0
    left join
    (
        select 
            distinct 
            order_sn
        from
            origin_common.cc_ods_log_gwapp_order_track_hourly
        where
            ds = '${stat_date}'
        and
            source='cctui'
    ) as a1
    on 
        a0.third_tradeno = a1.order_sn
    where
        a1.order_sn is null
    group by 
        a0.product_id
) as n7
on n1.product_id = n7.product_id
left join
(
    select
        t1.product_id,
        count(distinct t1.cck_uid) as new_user_num
    from 
    (
        select
            product_id,
            cck_uid,
            create_time
        from
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where 
            ds = '${stat_date}'
        and 
            product_id = '${product_id}'
    ) t1
    inner join 
    (
        select
            cck_uid,
            min(create_time) as fir_time
        from
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        group by 
            cck_uid
    ) t2
    on t1.cck_uid = t2.cck_uid and t1.create_time = t2.fir_time
    group by 
        t1.product_id
) as n8
on n1.product_id = n8.product_id
left join
(
    select
        o1.product_id,
        count(distinct o1.cck_uid) as new_self_user_num
    from
    (
        select
            p1.product_id,
            p2.cck_uid,
            p2.third_tradeno
        from
        (
            select
                n1.product_id,
                n1.cck_uid
            from
            (
                select
                    product_id, 
                    third_tradeno,
                    cck_uid,
                    create_time
                from
                    origin_common.cc_ods_dwxk_wk_sales_deal_ctime
                where
                    ds = '${stat_date}'
                and
                    product_id = '${product_id}'
            ) as n1
            left join
            (
                select
                    cck_uid,
                    min(create_time) as fir_time
                from
                    origin_common.cc_ods_dwxk_wk_sales_deal_ctime
                group by
                    cck_uid
            ) as n2
            on 
                n1.cck_uid = n2.cck_uid
            where
                n2.fir_time = n1.create_time
        ) as p1 
        inner join
        (
            select
                cck_uid,
                third_tradeno
            from
                origin_common.cc_ods_dwxk_wk_sales_deal_ctime
            where
                ds = '${stat_date}'
            and
                product_id = '${product_id}'
        ) as p2
        on 
            p1.cck_uid = p2.cck_uid
    ) as o1
    inner join
    (
        select
            distinct 
            order_sn
        from
            origin_common.cc_ods_log_gwapp_order_track_hourly
        where
            ds = '${stat_date}'
        and
            source='cctui'
    ) as o2
    on 
        o1.third_tradeno = o2.order_sn
    group by
        o1.product_id
) as n9
on n1.product_id = n8.product_id

