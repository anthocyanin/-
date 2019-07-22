select
    t1.cck_uid   as cck_uid,
    t3.real_name as cck_name,
    t3.phone     as cck_phone,
    t2.type      as cck_level,
    t4.gm_uid    as gm_uid,
    t5.real_name as gm_name,
    t5.phone     as gm_phone,
    s1.product_id as product_id,
    s1.product_sku_id as product_sku_id,
    t1.third_tradeno  as third_tradeno,
    t1.sale_num       as sale_num,
    t1.cck_commission as cck_commission,
    t1.item_price  as item_price,
    t1.uid         as uid,
    t1.create_time as create_time,
    if(t6.order_sn is not null,'自买','非自买') as is_self_buy
from
(
    select
        s1.cck_uid as cck_uid,
        s1.uid as uid,
        s1.product_id as product_id,
        s1.product_sku_id as product_sku_id,
        s1.third_tradeno as third_tradeno,
        s1.sale_num as sale_num,
        (s1.cck_commission/100) as cck_commission,
        (s1.item_price/100) as item_price,
        from_unixtime(s1.create_time,'yyyyMMdd HH:mm:ss') as create_time 
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
    inner join 
        origin_common.cc_ods_dwxk_fs_wk_cck_user s2
    on 
        s1.cck_uid = s2.cck_uid
    where 
        s1.ds = '${state_date}'
    and 
        s1.product_id = '${product_id}'
    and
        s2.ds= '${state_date}'
    and
        s2.platform = 14
) t1
left join 
(
    select
        cck_uid,
        if(type=0,'VIP',if(type=1,'总监','总经理')) as type
    from 
        cc_ods_fs_wk_cct_layer_info
) t2
on t1.cck_uid=t2.cck_uid
left join
(
    select
        distinct
        cck_uid,
        real_name,
        phone
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where
        ds='${state_date}'
) t3
on t1.cck_uid = t3.cck_uid
left join 
(
    select
        distinct
        cck_uid,
        gm_uid
    from 
        cc_ods_fs_wk_cct_layer_info
    where
        gm_uid !=0
    union all
    select
        distinct
        gm_uid as cck_uid,
        gm_uid
    from 
        cc_ods_fs_wk_cct_layer_info
    where
        gm_uid !=0
) t4
on t1.cck_uid=t4.cck_uid
left join
(
    select
        distinct
        cck_uid,
        real_name,
        phone
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where
        ds='${state_date}'
) t5
on t4.gm_uid = t5.cck_uid
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
) t6
on t1.third_tradeno=t6.order_sn