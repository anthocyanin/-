USE ${hive.databases.rpt};

ALTER TABLE ${table_rpt_dwxk_product_info_daily}
DROP IF EXISTS PARTITION( ds= '${bizdate}');

ALTER TABLE ${table_rpt_dwxk_product_info_daily}
ADD IF NOT EXISTS PARTITION (ds = '${bizdate}')
LOCATION '${bizdate}';

INSERT OVERWRITE TABLE ${hive.databases.rpt}.${table_rpt_dwxk_product_info_daily}
PARTITION (ds = '${bizdate}')

SELECT 
    '${bizdate}'                     as date,--日期
    t1.product_id                    as product_id,--商品id
    t11.product_name                 as product_name,--商品名称
    t11.shop_id                      as shop_id,--店铺id
    t11.shop_name                    as shop_name,--店铺名称
    t1.first_cid                     as product_c1,--一级类目
    t11.product_c2                   as product_c2,--二级类目
    t11.product_c3                   as product_c3,--三级类目
    (case when t1.first_cid=-1 then '女装'
        when t1.first_cid=-2 then '箱包'
        when t1.first_cid=-3 then '配饰'
        when t1.first_cid=-4 then '美妆个护'
        when t1.first_cid=-5 then '食品'
        when t1.first_cid=-6 then '家居百货'
        when t1.first_cid=-7 then '运动户外'
        when t1.first_cid=-8 then '母婴'
        when t1.first_cid=-9 then '手机数码'
        when t1.first_cid=-10 then '家用电器'
        when t1.first_cid=-11 then '虚拟类目'
        when t1.first_cid=-12 then '鞋靴'
        when t1.first_cid=-13 then '男装'
    else '女士内衣/男士内衣/家居服' end)  as product_cname1,--一级类目名称
    t11.product_cname2               as product_cname2,--二级类目名称
    t11.product_cname3               as product_cname3,--三级类目名称
    t11.shop_type                    as shop_type,--店铺类型
    COALESCE(t1.pay_cnt_30,0)        as pay_cnt_30,--30天内的购买数
    COALESCE(t1.cck_commission_30,0) as cck_commission_30,--30天内的楚客佣金额
    COALESCE(t1.pay_fee_30,0)        as pay_fee_30,--30天内成交金额
    COALESCE(t2.pay_cnt,0)           as pay_cnt,--指定日期购买数
    COALESCE(t2.cck_commission,0)    as cck_commission,--指定日期的楚客佣金额
    COALESCE(t2.pay_fee,0)           as pay_fee,--指定日期成交金额
    COALESCE(t3.add_cnt,0)           as add_cnt,--指定日期下单数量
    COALESCE(t3.add_fee,0)           as add_fee,--指定日期下单金额
    COALESCE(t4.ipv_30,0)            as ipv_30,--30天内访问次数
    COALESCE(t4.ipv,0)               as ipv,--指定日期访问次数
    COALESCE(t5.refund_cnt,0)        as refund_cnt,--退款数
    COALESCE(t6.refund_cnt_30,0)     as refund_cnt_30,--30天内的退款数
    COALESCE(t7.bad_cnt,0)           as bad_cnt,--指定日期差评数
    COALESCE(t7.eva_rate_cnt,0)      as eva_rate_cnt,--指定日期评价数
    COALESCE(t7.bad_cnt_30,0)        as bad_cnt_30,--30天内差评数
    COALESCE(t7.eva_rate_cnt_30,0)   as eva_rate_cnt_30,--30天内评价数
    COALESCE(t8.order_count_ship_success,0)     as order_count_ship_success,--30天内签收单数
    COALESCE(t8.ship_time_sum,0)                as ship_time_sum,--物流运货时长之和
    COALESCE(t9.order_count_delivery_success,0) as order_count_delivery_success,--30天内发货订单数
    COALESCE(t9.delivery_time_sum,0)            as delivery_time_sum,--发货时长之和
    COALESCE(t10.pay_uv_30d,0)       as pay_uv_30d,--30天内购买用户数
    COALESCE(t10.again_pay_uv_30d,0) as again_pay_uv_30d,--30天内有回购的用户数
    COALESCE(t12.fx_cnt,0)           as fx_cnt--分享数
FROM
(
    SELECT
        product_id,
        first_cid,
        count(distinct third_tradeno) as pay_cnt_30,
        sum(cck_commission/100) as cck_commission_30,
        sum(item_price/100) as pay_fee_30
    FROM 
        ${hive.databases.ori}.cc_ods_dwxk_wk_sales_deal_ctime
    WHERE 
        ds>='${bizdate-29}' and ds<='${bizdate}'
    GROUP BY 
        product_id,first_cid
) t1
LEFT JOIN
(
    SELECT
        product_id,
        count(distinct third_tradeno) as pay_cnt,
        sum(cck_commission/100) as cck_commission,
        sum(item_price/100) as pay_fee
    FROM 
        ${hive.databases.ori}.cc_ods_dwxk_wk_sales_deal_ctime
    WHERE 
        ds='${bizdate}'
    GROUP BY 
        product_id
) t2
ON t1.product_id = t2.product_id
LEFT JOIN
(
    SELECT
        product_id,
        count(distinct order_sn) as add_cnt,--指定日期下单数量
        sum(product_discount_price*product_count) as add_fee--指定日期下单金额
    FROM 
        ${hive.databases.ori}.cc_order_products_user_add_time
    WHERE 
        ds='${bizdate}' 
    and 
        !(source_channel&1=0 and source_channel&2=0 and source_channel&4=0)
    GROUP BY product_id
) t3
ON t1.product_id = t3.product_id
LEFT JOIN
(
    select 
        s1.product_id as product_id, 
        sum(if(s1.ds='${bizdate}',1,0)) as ipv,--指定日期的商品详情页面的浏览次数
        count(1) as ipv_30--商品详情页面的浏览次数
    from
    (
        select
            ds, 
            product_id
        from 
            ${hive.databases.ori}.cc_ods_log_cctui_product_coupon_detail_hourly
        where 
            ds>='${bizdate-29}' 
        and 
            ds<='${bizdate}' 
        and 
            detail_type='item'
        union all
        select
            ds, 
            product_id
        from 
            ${hive.databases.ori}.cc_ods_log_gwapp_product_detail_hourly 
        where 
            ds>='${bizdate-29}' 
        and 
            ds<='${bizdate}'
    ) s1
    group by s1.product_id
) t4
ON t1.product_id = t4.product_id
LEFT JOIN
(
    select
        s2.product_id as product_id,
        count(distinct s1.order_sn) as refund_cnt--指定日期退款订单数
    from
    (
        select
            order_sn
        from 
            origin_common.cc_ods_fs_refund_order
        where 
            stop_time>='${bizdate_ts}' 
        and 
            stop_time<'${gmtdate_ts}' 
        and 
            status =1
    ) s1
    inner join
    (
        select
            product_id,
            third_tradeno as order_sn
        FROM 
            ${hive.databases.ori}.cc_ods_dwxk_wk_sales_deal_ctime
        WHERE 
            ds>='${bizdate-30}' 
        and 
            ds<='${bizdate}'
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
        from 
            origin_common.cc_ods_fs_refund_order
        where 
            stop_time>='${bizdate_ts-29}' 
        and 
            stop_time<'${gmtdate_ts}' 
        and 
            status =1
    ) s1
    inner join
    (
        select
            product_id,
            third_tradeno as order_sn
        FROM 
            ${hive.databases.ori}.cc_ods_dwxk_wk_sales_deal_ctime
        WHERE 
            ds>='${bizdate-60}' 
        and 
            ds<='${bizdate}'
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
        FROM 
            origin_common.cc_rate_star
        WHERE 
            ds>='${bizdate-29}' 
        and 
            ds <= '${bizdate}' 
        and 
            rate_id > 0 
        and 
            order_sn != '170213194354LFo017564wk'    
    ) s1
    INNER JOIN
    (
        SELECT
            distinct third_tradeno as order_sn
        FROM 
            ${hive.databases.ori}.cc_ods_dwxk_wk_sales_deal_ctime
        WHERE 
            ds>='${bizdate-60}' 
        and 
            ds<='${bizdate}'
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
        from 
            data.cc_cct_product_ship_info--
        where 
            ds>='${bizdate-29}' 
        and 
            ds <= '${bizdate}'
    ) s1
    inner join
    (
        select 
            distinct product_id,
            third_tradeno as order_sn
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where 
            ds>='${bizdate-60}' 
        and 
            ds <= '${bizdate}'
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
        from 
            origin_common.cc_order_user_delivery_time
        where 
            ds>='${bizdate-29}' 
        and 
            ds <= '${bizdate}'
    ) as s1
    inner join
    (
        select 
            distinct product_id,
            third_tradeno as order_sn,
            create_time
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where 
            ds>='${bizdate-40}' 
        and 
            ds <= '${bizdate}'
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
            select 
                distinct
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
        n3.c2       as product_c2,
        n4.c3       as product_c3,
        n3.name as product_cname2,
        n4.name as product_cname3,
        n1.shop_id as shop_id,
        n6.shop_name as shop_name,
        (case when n1.shop_id in (18164,18335,17801,18628,18635,19141,19268) then '自营'
        when n1.shop_id = 17791 then '京东'
        when n1.shop_id = 18455 then '网易严选'
        when n1.shop_id = 18470 then '冰冰购'
        else 'pop' end) as shop_type
    from
    (
        select
            distinct shop_id as shop_id,
            app_item_id as product_id,
            category_id as category_id
        from origin_common.cc_ods_dwxk_fs_wk_items
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
    count(m1.user_id) as fx_cnt,--分享数量
    count(distinct m1.user_id) as fx_user_cnt--分享用户数量
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
                distinct 
                ad_material_id as ad_material_id,
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


