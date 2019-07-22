select
    '${bizdate}'                                            as date,
    cast(sum(h1.cck_commission/100) as decimal(20,2))       as cck_commission,
    cast(sum(h1.item_price/100) as decimal(20,2))           as pay_fee,
    count(distinct h1.third_tradeno)                        as order_cnt,
    count(distinct h1.cck_uid)                              as saled_cck_cnt,
    count(distinct h1.product_id)                           as saled_prd_cnt,
    cast(avg(h1.cck_rate/10) as decimal(20,2))              as cck_rate,
    cast(sum(if(h3.order_sn is not null,h1.commission,0)/100) as decimal(20,2))           as cct_commission,
    cast(sum(if(h3.order_sn is not null,h3.pb_price,0)) as decimal(20,2))                 as cct_ori_fee,
    cast(sum(if(h3.order_sn is not null,h1.item_price,0)/100) as decimal(20,2))           as cct_pay_fee,
    count(distinct if(h3.order_sn is not null,h1.third_tradeno,null))                     as cct_order_cnt
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

////////////////////////////////////////////////////////////////////
select
    t1.ds as ds,
    s1.product_id as product_id,
    s1.product_sku_id as product_sku_id,
    s1.third_tradeno as third_tradeno,
    (t5.pb_price*t1.sale_num) as pb_price,
    t1.commission as commission,
    t1.item_price as item_price
from
(
    select
        s1.ds as ds,
        s1.product_id as product_id,
        s1.product_sku_id as product_sku_id,
        s1.third_tradeno as third_tradeno,
        s1.sale_num as sale_num,
        (s1.commission/100) as commission,
        (s1.item_price/100) as item_price
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
    join 
        origin_common.cc_ods_dwxk_fs_wk_cck_user s2
    on 
        s1.cck_uid = s2.cck_uid
    where 
        s1.ds >= '${begin_date}'
    and 
        s1.ds <= '${end_date}'
    and 
        s2.ds  = '${end_date}'
    and
        s2.platform = 14
) t1
left join 
(
    select
        s1.order_sn as order_sn,
        s1.sku_id   as sku_id,
        s2.pb_price as pb_price
    from
    (
        select
            distinct 
            ob_pb_id as ob_pb_id,
            ob_order_sn as order_sn,
            ob_sku_id as sku_id,
            ob_count
        from origin_common.cc_ods_op_order_batches
        where ds= '${end_date}'
    ) s1
    inner join
    (
        select
            pb_id,
            pb_price
        from origin_common.cc_ods_fs_op_product_batches
    ) s2
    on s1.ob_pb_id=s2.pb_id
) t5 
on t1.third_tradeno=t5.order_sn and t1.product_sku_id=t5.sku_id
//////////////////////////////////////////////////////////////////////////////////
select
    t1.ds as ds,
    t1.product_id as product_id,
    t1.product_sku_id as product_sku_id,
    t3.product_cname1 as product_cname1,
    t3.product_cname2 as product_cname2,
    t3.shop_id as shop_id,
    t5.pb_price as pb_price,
    t1.commission as commission,
    t1.item_price as item_price
from
(
    select
        s1.ds as ds,
        S1.product_id as product_id,
        s1.product_sku_id as product_sku_id,
        s1.third_tradeno as third_tradeno,
        (s1.commission/100) as commission,
        (s1.item_price/100) as item_price
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
    join 
        origin_common.cc_ods_dwxk_fs_wk_cck_user s2
    on 
        s1.cck_uid = s2.cck_uid
    where 
        s1.ds >= 20181101
    and 
        s1.ds <= 20181102
    and 
        s2.ds = '${end_date}'
    and
        s2.platform = 14
) t1
left join 
(
    select
        product_id,
        product_cname1,
        product_cname2,
        shop_id
    from
        data.cc_dw_fs_products_shops
) t3
on t1.product_id = t3.product_id
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
        where ds= '${end_date}'
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
) t5 
on t1.third_tradeno=t5.order_sn and t1.product_sku_id=t5.sku_id
////////////////////////////////////////////////////////////////////////////
下面代码证明cc_ods_op_order_batches他是全量表，因为表格数据量很大且每日数据递增。
select
    ds,
    count(*) as bum
from origin_common.cc_ods_op_order_batches
where ds>=20181101 and ds <=20181129
group by ds 
////////////////////////////////////////////////////////////
排查cc_ods_op_order_batches这张表： 这张表不是全量表吗， 但是ds不同关联出来的供货价就会不同。
ds=20181129 供货价总和5万多，ds=20181102 供货价总和170多万。问题就是出在这
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
    where ds= '${end_date}' 
        and ob_ctime>=1541001600 --11.1
        and ob_ctime<=1541088000 --11.2
) s1
inner join
(
    select
        pb_id,
        pb_price
    from origin_common.cc_ods_fs_op_product_batches
) s2
on s1.ob_pb_id=s2.pb_id
group by  s1.order_sn,s1.sku_id