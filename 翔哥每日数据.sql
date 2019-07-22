######商品各资源位产出情况
select
    p1.ds,
    p1.product_id,
    p2.ad_type,
    count(p1.third_tradeno) as order_count,
    sum(p1.item_price/100) as pay_fee,
    sum(p1.cck_commission/100) as commission
from
(
    select
        ds,
        product_id, 
        third_tradeno,
        item_price,
        cck_commission
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where
        ds = ${stat_date}
    and
        product_id in (${product_id})
) as p1
left join
(
    select
        order_sn,
        ad_type
    from
        origin_common.cc_ods_log_gwapp_order_track_hourly
    where
        ds = ${stat_date}
) as p2
on p1.third_tradeno = p2.order_sn
group by
    p1.ds,p1.product_id,p2.ad_type
order by
    order_count desc
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
###产品数据线周报代码
###每日各资源位产出
select
    t1.ds,
    t3.ad_type,
    sum(t1.item_price/100) as item_price
from
(
    select
        ds,
        cck_uid,
        third_tradeno,
        item_price,
        uid
    from  
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where 
        ds>='${start_date}'
        and
        ds<='${end_date}'
)t1
join
(
    select
        ds,
        cck_uid,
        cct_uid
    from  
        origin_common.cc_ods_dwxk_fs_wk_cck_user
    where 
        ds>='${start_date}' 
        and 
        ds<='${end_date}'
        and 
        platform=14
)t2
on t1.cck_uid=t2.cck_uid and t1.ds=t2.ds
left join
(
    select
        ds,
        order_sn,
        ad_type
    from 
        origin_common.cc_ods_log_gwapp_order_track_hourly
    where 
        ds>='${start_date}' 
        and 
        ds<='${end_date}'
)t3
on t1.third_tradeno=t3.order_sn and t1.ds=t3.ds
group by t1.ds,t3.ad_type





###全平台总支付人数、支付金额、支付订单数、销量
select
    t1.ds as ds,
    count(distinct t1.uid) as user_count,
    sum(t1.item_price/100) as pay_fee,
    count(distinct t1.third_tradeno) as pay_count,
    sum(t1.sale_num) as sale_num
from
(
    select 
        ds,
        cck_uid,
        uid,
        sale_num,
        third_tradeno,
        item_price,
        create_time
    from 
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where 
        ds>='${start_date}'
)t1
join
(
    select
        ds,
        cck_uid
    from 
        origin_common.cc_ods_dwxk_fs_wk_cck_user
    where 
        ds>='${start_date}' 
        and 
        platform=14
)t2
on t1.cck_uid=t2.cck_uid and t1.ds=t2.ds
group by t1.ds





###新用户支付人数、支付金额、支付订单数、销量
select
    t1.ds as ds,
    count(distinct t1.uid) as user_count,
    sum(t1.item_price/100) as pay_fee,
    count(distinct t1.third_tradeno) as pay_count,
    sum(t1.sale_num) as sale_num
from
(
    select 
        ds,
        cck_uid,
        uid,
        sale_num,
        third_tradeno,
        item_price,
        create_time
    from 
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where 
        ds>='${start_date}'
)t1
join
(
    select
        ds,
        cck_uid
    from 
        origin_common.cc_ods_dwxk_fs_wk_cck_user
    where 
        ds>='${start_date}' 
        and 
        platform=14
)t2
on t1.cck_uid=t2.cck_uid and t1.ds=t2.ds
left join
(
    select
        uid,
        min(create_time) as first_time
    from 
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    group by 
        uid
) t3
on t1.uid = t3.uid
where t3.first_time = t1.create_time
group by t1.ds





###本周支付金额商品top10
select
    t1.ds as ds,
    t1.product_id as product_id,
    t3.product_title as product_title,
    count(distinct t1.uid) as user_count,
    sum(t1.item_price/100) as pay_fee,
    count(distinct t1.third_tradeno) as pay_count,
    sum(t1.sale_num) as sale_num
from
(
    select 
        ds,
        cck_uid,
        uid,
        product_id,
        sale_num,
        third_tradeno,
        item_price,
        create_time
    from 
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where 
        ds>='${start_date}'
)t1
join
(
    select
        ds,
        cck_uid
    from 
        origin_common.cc_ods_dwxk_fs_wk_cck_user
    where 
        ds>='${start_date}' 
        and 
        platform=14
)t2
on t1.cck_uid=t2.cck_uid and t1.ds=t2.ds
left join
(
    select
        distinct
        product_id,
        product_title
    from 
        data.cc_dw_fs_products_shops
)t3
on t1.product_id=t3.product_id
group by t1.ds,t1.product_id,t3.product_title
order by pay_fee desc
limit 10





###本周热门购买top10
select
    t1.ds as ds,
    t1.product_id as product_id,
    t3.product_title as product_title,
    count(distinct t1.uid) as user_count,
    sum(t1.item_price/100) as pay_fee,
    count(distinct t1.third_tradeno) as pay_count,
    sum(t1.sale_num) as sale_num
from
(
    select 
        ds,
        cck_uid,
        uid,
        product_id,
        sale_num,
        third_tradeno,
        item_price,
        create_time
    from 
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where 
        ds>='${start_date}'
)t1
join
(
    select
        ds,
        cck_uid
    from 
        origin_common.cc_ods_dwxk_fs_wk_cck_user
    where 
        ds>='${start_date}' 
        and 
        platform=14
)t2
on t1.cck_uid=t2.cck_uid and t1.ds=t2.ds
left join
(
    select
        distinct
        product_id,
        product_title
    from 
        data.cc_dw_fs_products_shops
)t3
on t1.product_id=t3.product_id
group by t1.ds,t1.product_id,t3.product_title
order by user_count desc
limit 10