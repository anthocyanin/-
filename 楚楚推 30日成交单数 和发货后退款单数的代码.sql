select
    a1.product_id,
    count(distinct a1.third_tradeno) as pay_count_30d,--30日订单数
    count(distinct a2.order_sn) as refund_count_30d--30日发货后退款数
from
(
    select
        s1.product_id,
        s1.product_sku_id,
        s1.item_price,
        s1.cck_commission,
        s1.third_tradeno,
        s1.create_time
    from
    (
        select
            product_id,
            product_sku_id,
            item_price,
            cck_commission,
            third_tradeno,
            create_time,
            cck_uid
        from
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where
            ds >= '${begin_date_30d}'
        and
            ds <= '${end_date}' 
    )s1
    inner join
    (
        select
            cck_uid
        from
            origin_common.cc_ods_dwxk_fs_wk_cck_user 
        where
            ds = '${end_date}'
        and
            platform = 14
    )s2
    on s1.cck_uid=s2.cck_uid
) a1
left join
(
    select 
        s1.order_sn,
        s3.product_id,
        s3.sku_id
    from 
    (
        select
            n1.refund_sn, 
            n1.order_sn
        from 
        (
            select
                t1.refund_sn, 
                t1.order_sn,
                if(t1.status=2,if(t1.success_price=0,0,1),t1.status) as status
            from
                origin_common.cc_ods_fs_refund_order t1
            where
                from_unixtime(t1.create_time,'yyyyMMdd') >= '${begin_date_30d}' 
            and
                from_unixtime(t1.create_time,'yyyyMMdd') <= '${end_date}'
            and 
                t1.status in (1,2)
        ) n1
        where n1.status=1
    ) s1
    inner join
    (
        select 
            order_sn
        from 
            origin_common.cc_order_user_delivery_time
        where
            ds >= '${begin_date_30d}'
        and
            ds <= '${end_date}' 
    ) s2
    on s1.order_sn=s2.order_sn
    left join 
    (
        select
            refund_sn, 
            order_sn,
            product_id,
            sku_id
        from
            origin_common.cc_refund_products 
        where
            ds = '${end_date}' 
    ) s3
    on s1.refund_sn=s3.refund_sn
) a2
on a1.third_tradeno=a2.order_sn and a1.product_id=a2.product_id and a1.product_sku_id=a2.sku_id
group by 
    a1.product_id