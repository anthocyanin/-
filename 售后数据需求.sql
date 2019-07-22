元数据代码
select
    n1.product_id     as product_id,--商品id--30日内有订单=有成交的
    n1.order_count    as order_count,--30日内订单数
    n1.pay_fee        as pay_fee,--30日内支付金额
    if(n2.refund_cnt is null,0,n2.refund_cnt) as refund_cnt,--30日内发货后8日内又退款的订单数
    n3.product_title  as product_title,--商品名称
    n3.shop_id        as shop_id,--店铺id
    n3.shop_title     as shop_title,--店铺名称
    n3.product_cname1 as product_cname1,--商品一级类目
    n3.product_cname2 as product_cname2,--商品二级类目
    n3.product_cname3 as product_cname3--商品三级类目
from
(
    select
        product_id,
        count(third_tradeno) as order_count,
        sum(item_price/100) as pay_fee
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where
        ds >= '${begin_date}'
    and
        ds <= '${end_date}'
    group by
        product_id
) as n1
left join
(
    select
        a1.product_id,
        count(a1.third_tradeno) as refund_cnt
    from
    (
        select
            ds,
            product_id,
            third_tradeno
        from
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where
            ds >= '${begin_date}'
        and
            ds <= '${end_date}'
    ) as a1
    inner join
    (
        select
            order_sn
        from
            origin_common.cc_order_user_delivery_time
        where
            ds >= '${begin_date}'  
    ) as a2
    on a1.third_tradeno=a2.order_sn
    inner join
    (
        select
            order_sn,
            from_unixtime(create_time,'yyyyMMdd') as refund_date
        from
            origin_common.cc_ods_fs_refund_order
        where
            create_time>=unix_timestamp('${begin_date}','yyyyMMdd')
    ) as a3
    on a1.third_tradeno = a3.order_sn
    where
       unix_timestamp(a3.refund_date,'yyyyMMdd')-unix_timestamp(a1.ds,'yyyyMMdd') <= 8*3600*24
    group by
       a1.product_id
) as n2
on n1.product_id = n2.product_id
left join
(
    select
        product_id,
        product_title,
        shop_id,
        shop_title,
        product_cname1,
        product_cname2,
        product_cname3
    from
        data.cc_dw_fs_products_shops
) as n3
on n1.product_id = n3.product_id
where
    n3.shop_id not in (17791,18731,18455)
//////////////////////////////////////////////////////////////////////////////////
三级类目数据代码
select
    p1.product_cname1,--商品一级类目
    p1.product_cname2,--商品二级类目
    p1.product_cname3,--商品三级类目
    sum(p1.order_count),--订单数
    sum(p1.refund_cnt)--退款数
from 
(
    select
        n1.product_id,
        n1.order_count,
        n1.pay_fee,
        if(n2.refund_cnt is null,0,n2.refund_cnt) as refund_cnt,
        n3.product_title,
        n3.shop_id,
        n3.shop_title,
        n3.product_cname1,
        n3.product_cname2,
        n3.product_cname3,
        if(n2.refund_cnt is null,0,n2.refund_cnt/n1.order_count) as refund_rate
    from
    (
        select
            product_id,
            count(third_tradeno) as order_count,
            sum(item_price/100) as pay_fee
        from
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where
            ds >= '${begin_date}'
        and
            ds <= '${end_date}'
        group by
            product_id
    ) as n1
    left join
    (
        select
            a1.product_id,
            count(a1.third_tradeno) as refund_cnt
        from
        (
            select
                ds,
                product_id,
                third_tradeno
            from
                origin_common.cc_ods_dwxk_wk_sales_deal_ctime
            where
                ds >= '${begin_date}'
            and
                ds <= '${end_date}'
        ) as a1
        inner join
        (
            select
                order_sn
            from
                origin_common.cc_order_user_delivery_time
            where
                ds >= '${begin_date}'  
        ) as a2
        on a1.third_tradeno = a2.order_sn
        inner join
        (
            select
                order_sn,
                from_unixtime(create_time,'yyyyMMdd') as refund_date
            from
                origin_common.cc_ods_fs_refund_order
            where
                create_time >= unix_timestamp('${begin_date}','yyyyMMdd')
        ) as a3
        on a1.third_tradeno = a3.order_sn
        where
            unix_timestamp(a3.refund_date,'yyyyMMdd') - unix_timestamp(a1.ds,'yyyyMMdd') <= 8*3600*24
        group by
            a1.product_id
    ) as n2
    on n1.product_id = n2.product_id
    left join
    (
        select
            product_id,
            product_title,
            shop_id,
            shop_title,
            product_cname1,
            product_cname2,
            product_cname3
        from
            data.cc_dw_fs_products_shops
    ) as n3
    on n1.product_id = n3.product_id
    where
        n3.shop_id not in (17791,18731,18455)
) as p1
group by p1.product_cname1,p1.product_cname2,p1.product_cname3
///////////////////////////////////////////////////////////////////////////////////////
top商家数据数据代码
select
    o1.*
from
(
    select
        p1.*,
        p2.refund_rate,
        p2.prd_cnt,
        rank() over(partition by p1.product_cname1,p1.product_cname2,p1.product_cname3 order by p1.refund_rate/p2.refund_rate desc) as num
    from
    (
        select
            n1.product_id,
            n1.order_count,
            n1.pay_fee,
            if(n2.refund_cnt is null,0,n2.refund_cnt) as refund_cnt,--商品退款数
            n3.product_title,
            n3.shop_id as shop_id,
            n3.shop_title,
            n3.product_cname1,
            n3.product_cname2,
            n3.product_cname3,
            if(n2.refund_cnt is null,0,n2.refund_cnt/n1.order_count) as refund_rate--商品退款率
        from
        (
            select
                product_id,
                count(third_tradeno) as order_count,
                sum(item_price/100) as pay_fee
            from
                origin_common.cc_ods_dwxk_wk_sales_deal_ctime
            where
                ds >= '${begin_date}'
            and
                ds <= '${end_date}'
            group by
                product_id
        ) as n1
        left join
        (
            select
                a1.product_id,
                count(a1.third_tradeno) as refund_cnt
            from
            (
                select
                    ds,
                    product_id,
                    third_tradeno
                from
                    origin_common.cc_ods_dwxk_wk_sales_deal_ctime
                where
                    ds >= '${begin_date}'
                and
                    ds <= '${end_date}'
            ) as a1
            inner join
            (
                select
                    order_sn
                from
                    origin_common.cc_order_user_delivery_time
                where
                    ds >= '${begin_date}'  
            ) as a2
            on a1.third_tradeno = a2.order_sn
            inner join
            (
                select
                    order_sn,
                    from_unixtime(create_time,'yyyyMMdd') as refund_date
                from
                    origin_common.cc_ods_fs_refund_order
                where
                    create_time >= unix_timestamp('${begin_date}','yyyyMMdd')
            ) as a3
            on a1.third_tradeno = a3.order_sn
            where
                unix_timestamp(a3.refund_date,'yyyyMMdd') - unix_timestamp(a1.ds,'yyyyMMdd') <= 8*3600*24
            group by
              a1.product_id
        ) as n2
        on n1.product_id = n2.product_id
        left join
        (
            select
               product_id,
               product_title,
               shop_id,
               shop_title,
               product_cname1,
               product_cname2,
               product_cname3
            from
               data.cc_dw_fs_products_shops
        ) as n3
        on n1.product_id = n3.product_id 
    ) as p1
    left join
    (
        select
            n3.product_cname1,
            n3.product_cname2,
            n3.product_cname3,
            sum(n2.refund_cnt)/sum(n1.order_count) as refund_rate,--三级类目退款率
            count(n1.product_id) as prd_cnt--三级类目动销商品数
        from
        (
            select
                product_id,
                count(third_tradeno) as order_count,
                sum(item_price/100) as pay_fee
            from
                origin_common.cc_ods_dwxk_wk_sales_deal_ctime
            where
                ds >= '${begin_date}'
            and
                ds <= '${end_date}'
            group by
                product_id
        ) as n1
        left join
        (
            select
                a1.product_id,
                count(a1.third_tradeno) as refund_cnt
            from
            (
                select
                    ds,
                    product_id,
                    third_tradeno
                from
                    origin_common.cc_ods_dwxk_wk_sales_deal_ctime
                where
                    ds >= '${begin_date}'
                and
                    ds <= '${end_date}'
            ) as a1
            inner join
            (
                select
                    order_sn
                from
                    origin_common.cc_order_user_delivery_time
                where
                    ds >= '${begin_date}'  
            ) as a2
            on a1.third_tradeno = a2.order_sn
            inner join
            (
                select
                    order_sn,
                    from_unixtime(create_time,'yyyyMMdd') as refund_date
                from
                    origin_common.cc_ods_fs_refund_order
                where
                    create_time >= unix_timestamp('${begin_date}','yyyyMMdd')
            ) as a3
            on a1.third_tradeno = a3.order_sn
            where
                unix_timestamp(a3.refund_date,'yyyyMMdd') - unix_timestamp(a1.ds,'yyyyMMdd') <= 8*3600*24
            group by
            a1.product_id
        ) as n2
        on n1.product_id = n2.product_id
        left join
        (
            select
                product_id,
                product_cname1,
                product_cname2,
                product_cname3
            from
               data.cc_dw_fs_products_shops
        ) as n3
        on n1.product_id = n3.product_id
        group by
           n3.product_cname1,n3.product_cname2,n3.product_cname3
    ) as p2
    on p1.product_cname1 = p2.product_cname1 and p1.product_cname2 = p2.product_cname2 and p1.product_cname3 = p2.product_cname3
    where
    p1.order_count > 100 and p2.prd_cnt >10 and p2.refund_rate > 0 and p1.shop_id not in (17791,18731,18455)
) as o1
where
   o1.num <= 3
////////////////////////////////////////////////////////////////////////////
退款理由数据代码
select
    n1.product_id,
    n2.refund_reason
from
(
    select
        ds,
        product_id,
        third_tradeno
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where
        ds >= '${begin_date}'
    and
        ds <= '${end_date}'
    and
        product_id in ( ) 
) as n1
inner join
(
    select
        order_sn,
        from_unixtime(create_time,'yyyyMMdd') as refund_date,
        refund_reason
    from
        origin_common.cc_ods_fs_refund_order
    where
        create_time >= unix_timestamp('${begin_date}','yyyyMMdd')
) as n2
on n1.third_tradeno = n2.order_sn
inner join
(
    select
        order_sn
    from
        origin_common.cc_order_user_delivery_time
    where
        ds >= '${begin_date}'  
) as n3
on n1.third_tradeno = n3.order_sn
where
    unix_timestamp(n2.refund_date,'yyyyMMdd')-unix_timestamp(n1.ds,'yyyyMMdd')<=8*3600*24
/////////////////////////////////////////////////////////////////////////////////////////
售后明细
select
    n2.refund_sn      as refund_sn,--退款退货编号
    n2.order_sn       as order_sn,--订单编号
    n4.shop_id        as shop_id,--店铺id
    n1.product_id     as product_id,--商品id
    n4.product_title  as product_title,--商品名称
    n4.product_cname1 as product_cname1,--商品一级类目
    n4.product_cname2 as product_cname2,--商品二级类目
    n4.product_cname3 as product_cname3,--商品三级类目
    n2.status         as status,--退款状态
    n2.refund_reason  as refund_reason,--退款类型/理由
    n2.description    as description,--退款描述
    n2.success_price  as success_price,--退款金额
    n2.refund_date_1  as refund_date_1,--退款申请时间
    n2.refund_date_2  as refund_date_2--退款成功时间
from
   (
    select
        s1.ds            as ds,
        s1.product_id    as product_id,
        s1.third_tradeno as third_tradeno
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1 
    inner join
        origin_common.cc_ods_dwxk_fs_wk_cck_user      s2
    on  s1.cck_uid=s2.cck_uid
    where
        s1.ds>='${begin_date}'
    and
        s1.ds<='${end_date}'
    and 
        s2.platform=14
    and 
        s2.ds='${end_date}'
   )  n1
   left join
   (
    select
        product_id,
        shop_id,
        product_title,
        product_cname1,--商品一级类目
        product_cname2,--商品二级类目
        product_cname3--商品三级类目
    from 
        data.cc_dw_fs_products_shops
   ) n4
   on n1.product_id=n4.product_id
   inner join
   (
    select
        order_sn,--订单编号
        refund_sn,--退款退货编号
        from_unixtime(create_time,'yyyyMMdd') as refund_date_1,--退款申请时间
        from_unixtime(stop_time,'yyyyMMdd')   as refund_date_2,--退款成功时间
        refund_reason,--退款类型
        description,
        status,--退款状态
        success_price--退款金额
    from
        origin_common.cc_ods_fs_refund_order
    where
        create_time>=unix_timestamp('${begin_date}','yyyyMMdd')
   )  n2
   on n1.third_tradeno = n2.order_sn
   inner join
   (
    select
        order_sn
    from
        origin_common.cc_order_user_delivery_time
    where
        ds>='${begin_date}'  
   )  n3
   on n1.third_tradeno = n3.order_sn
   where
      unix_timestamp(n2.refund_date_1,'yyyyMMdd') - unix_timestamp(n1.ds,'yyyyMMdd') <= 8*3600*24

