select
    h1.ad_type_new as ad_type_new,
    sum(h1.pb_price) as origin_price,
    sum(h1.commission) as commission,
    sum(h1.product_coupon) as product_coupon,
    sum(h1.item_price) as item_price,
    sum(h1.gross_profit) as gross_profit,
    sum(h1.gross_profit)/(sum(h1.item_price)+sum(h1.product_coupon)) as gross_profit_rate
from
(
    select
        (
        case
        when p1.ad_type in ('special','seckill-tab-hot.productList','seckill-tab-hot.productList*pay_list') then '爆款'
        when p1.ad_type like 'cct-past-product%' then '往期爆款'
        when p1.ad_type like 'seckill-tab%' and p1.ad_type != 'seckill-tab-hot.productList' then '秒杀'
        when p1.ad_type in ('search','searchS','shareSearch') then '搜索'
        when p1.ad_type in ('wxkcategory','category') then '分类'
        when p1.ad_type = 'special-activity' then '活动页'
        when p1.ad_type = 'cct-new-people-buy.productList' then '新人专区'
        when p1.ad_type = '9_cell' then '朋友圈'
        else '其他' end
        ) as ad_type_new,
        p1.pb_price as pb_price,
        p1.commission as commission,
        p1.item_price as item_price,
        p1.product_coupon as product_coupon,
        p1.gross_profit
    from
    (
        select
            m1.ad_type as ad_type,
            m1.pb_price as pb_price,
            m1.commission as commission,
            m1.item_price as item_price,
            m1.product_coupon as product_coupon,--楚币
            (m1.item_price+m1.product_coupon-m1.pb_price-0.7993*m1.commission) as gross_profit--毛利额
        from
        (---自营的商品毛利额计算
            select
                n1.product_id as product_id,
                n1.ad_type as ad_type,
                n1.pb_price as pb_price,
                n1.commission as commission,
                n1.item_price as item_price,
                if(n2.product_id is not null,n1.product_coupon,0) as product_coupon
            from
            ( 
                select
                    t1.product_id as product_id,
                    t3.ad_type as ad_type,
                    t5.pb_price as pb_price,
                    if(t5.order_sn is not null,t1.commission,0) as commission,
                    if(t5.order_sn is not null,t1.item_price,0) as item_price,
                    if(t1.item_price=0,t4.used_money,(t1.item_price*t4.used_money/t4.order_price)) as product_coupon
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
                        ds,
                        order_sn,
                        ad_type
                    from  
                        origin_common.cc_ods_log_gwapp_order_track_hourly
                    where 
                        ds = '${state_date}'
                ) t3
                on t1.third_tradeno = t3.order_sn 
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
                        where ds= '${state_date}' and ob_ctime>= 1543939200
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
            ) n1
            left join 
            (
                select
                    pm_pid as product_id
                from origin_common.cc_ods_op_products_map
                where ds='${state_date}'
            ) n2
            on n1.product_id = n2.product_id
        ) m1
        union all
        select
            m2.ad_type as ad_type,
            m2.pb_price as pb_price,
            m2.commission as commission,
            m2.item_price as item_price,
            m2.product_coupon as product_coupon,--楚币
            (m2.commission*0.2007) as gross_profit--毛利额
        from
        (---pop的商品毛利额计算
            select
                n1.product_id as product_id,
                n1.ad_type as ad_type,
                n1.pb_price as pb_price,
                n1.commission as commission,
                n1.item_price as item_price,
                if(n2.product_id is null,n1.product_coupon,0) as product_coupon
            from
            ( 
                select
                    t1.product_id as product_id,
                    t3.ad_type as ad_type,
                    0 as pb_price,
                    if(t5.order_sn is null,t1.commission,0) as commission,
                    if(t5.order_sn is null,t1.item_price,0) as item_price,
                    if(t1.item_price=0,t4.used_money,(t1.item_price*t4.used_money/t4.order_price)) as product_coupon
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
                        ds,
                        order_sn,
                        ad_type
                    from  
                        origin_common.cc_ods_log_gwapp_order_track_hourly
                    where 
                        ds = '${state_date}'
                ) t3
                on t1.third_tradeno = t3.order_sn 
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
                        where ds= '${state_date}' and ob_ctime>= 1543939200
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
            ) n1
            left join 
            (
                select
                    pm_pid as product_id
                from origin_common.cc_ods_op_products_map
                where ds='${state_date}'
            ) n2
            on n1.product_id = n2.product_id
        ) m2
    ) p1     
) h1
group by h1.ad_type_new