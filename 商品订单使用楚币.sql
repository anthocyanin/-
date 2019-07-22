select
    t1.product_id as product_id,
    t1.product_sku_id as product_sku_id,
    t1.third_tradeno as third_tradeno,
    t1.commission as commission,
    t1.item_price as item_price,
    if(t1.item_price=0,t4.used_money,(t1.item_price*t4.used_money/t4.order_price)) as product_coupon--楚币
from
(
    select
        s1.product_id as product_id,
        s1.product_sku_id as product_sku_id,
        s1.third_tradeno as third_tradeno,
        (s1.commission/100) as commission,
        (s1.item_price/100) as item_price
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
    inner join 
        origin_common.cc_ods_dwxk_fs_wk_cck_user s2
    on 
        s1.cck_uid = s2.cck_uid
    where 
        s1.ds = '${state_date}'
    and 
        s2.ds = '${state_date}'
    and
        s2.platform = 14
) t1
left join
(
    select
        n1.third_tradeno as third_tradeno,
        n1.order_price as order_price,
        COALESCE(n2.used_money,0) as used_money
    from
    (
        select
            s1.third_tradeno as third_tradeno,
            sum(s1.item_price/100) as order_price
        from
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
        join 
            origin_common.cc_ods_dwxk_fs_wk_cck_user s2
        on 
            s1.cck_uid = s2.cck_uid
        where 
            s1.ds = '${state_date}'
        and 
            s2.ds = '${state_date}'
        and
            s2.platform = 14
        group by 
            s1.third_tradeno
    ) n1
    left join 
    (
        select
            s1.order_sn as order_sn,
            s1.used_money as used_money
        from
        (
            select
                order_sn,
                used_money,
                template_id
            from 
                origin_common.cc_order_coupon_paytime
            where 
                ds='${state_date}'
        ) s1
        inner join
        (
            select
                id
            from 
                origin_common.cc_ods_fs_coupon_temp
            where 
                platform='ccj_cct' and range!='shop' and shop_id=0
        ) s2
        on s1.template_id=s2.id
    ) n2
    on n1.third_tradeno = n2.order_sn
) t4
on t1.third_tradeno = t4.third_tradeno 
