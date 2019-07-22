廖宁临时需求 商品ID 名称  单数 佣金 劵后价格  推广人数   推广次数   
select
    t1.product_id as product_id, 
    t1.product_order_cnt_30 as product_order_cnt_30, --30日商品维度订单数
    t1.product_pay_fee_30 as product_pay_fee_30,--30日商品维度支付金额
    (t1.product_pay_fee_30/t1.total_sale_num) as price_after_coupon,--券后价格
    t1.product_cck_commission_30 as product_cck_commission_30,--30日商品维度佣金
    t2.product_cname1 as product_cname1,--商品一级类目
    t2.product_cname2 as product_cname2,--商品二级类目
    t2.product_cname3 as product_cname3,--商品三级类目
    t2.product_title as product_title,--商品名称 
    t3.fx_cnt as fx_cnt,--30日商品维度推广次数
    t3.fx_user_cnt as fx_user_cnt--30日商品维度推广人数
from
(
    select
        product_id,--商品id
        count(distinct third_tradeno) as product_order_cnt_30,--30日商品维度订单数
        sum(item_price/100) as product_pay_fee_30,--30日商品维度支付金额
        sum(sale_num) as total_sale_num,--30日商品维度销售数
        sum(cck_commission/100) as product_cck_commission_30--30日商品维度佣金
    from 
        cc_ods_dwxk_wk_sales_deal_ctime
    where 
        ds>=20180527 
    and 
        ds<=20180627
    group by product_id
) t1
inner join 
(
    select
        product_id,--商品id
        product_title,--商品名称
        product_cname1,--商品一级类目
        product_cname2,--商品二级类目
        product_cname3--商品三级类目
    from data.cc_dw_fs_products_shops
) t2
on t1.product_id=t2.product_id 
left join
(
    select
        m3.product_id,
        count(m1.user_id) as fx_cnt,--30日商品维度推广次数
        count(distinct m1.user_id,m1.ds) as fx_user_cnt--30日商品维度推广人数
    from
    (
        select
            ad_material_id as ad_id,
            user_id,
            ds
        from 
            origin_common.cc_ods_log_cctapp_click_hourly
        where 
            ds>=20180527 
        and 
            ds<=20180627 
        and 
            ad_type in ('search','category') 
        and 
            module = 'detail_material' 
        and 
            zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC')
        union all
        select
            ad_id,
            user_id,
            ds
        from 
            origin_common.cc_ods_log_cctapp_click_hourly
        where 
            ds>=20180527 
        and 
            ds<=20180627 
        and 
            ad_type not in ('search','category') 
        and 
            module = 'detail_material' 
        and 
            zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC')
        union all
        select
            s2.ad_id,
            s1.user_id,
            s1.ds
        from
        (
            select
                ad_material_id,
                user_id,
                ds
            from 
                origin_common.cc_ods_log_cctapp_click_hourly
            where 
                ds>=20180527 
            and 
                ds<=20180627 
            and 
                ad_type in ('single_product','9_cell') 
            and 
                module='vip' 
            and 
                zone in ('material_group-share','material_moments-share')
        ) s1
        inner join
        (
            select
                distinct 
                ad_material_id as ad_material_id,
                ad_id,
                ds
            from 
                data.cc_dm_gwapp_new_ad_material_relation_hourly
            where 
                ds>=20180527 
            and 
                ds<=20180627
        ) s2
        on s1.ad_material_id = s2.ad_material_id
    ) m1
    inner join
    (
        select
            ad_id,
            item_id
        from 
            origin_common.cc_ods_fs_dwxk_ad_items_daily
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
) t3
on t1.product_id=t3.product_id
where product_cname1='母婴'
////////////////////////////////////////////////////////////
客满中心临时需求退货原因
select
    s2.product_id as product_id,
    s1.refund_reason as refund_reason
from 
(
    select
        distinct order_sn as order_sn,
        refund_reason
    from 
        cc_ods_fs_refund_order 
    where 
        create_time>=1529683200 
    and 
        create_time<=1530201600 
) s1
inner join
(
    select
        third_tradeno as order_sn, 
        product_id
    from 
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where 
        product_id in(1100184427,110015728239,100180548,1100194365,10008163259,11011237311,1100192841,11001679825)    
) s2
on s1.order_sn=s2.order_sn
/////////////////////////////////////////////////////////////////////
东哥临时需求
select
    t1.order_sn as order_sn,--订单号
    t1.product_id as product_id,--商品id
    t2.product_title as product_title,--商品名称
    t2.shop_id as shop_id,--店铺id
    t2.shop_title as shop_title,--店铺名称
    t2.shop_cname1 as shop_cname1,--一级类目
    t1.refund_reason as refund_reason,--退款原因
    t1.description as description,--退款说明
    t1.delivery_status as delivery_status--是否发货
from 
(
    select
        s1.order_sn as order_sn,--订单号
        s1.refund_reason as refund_reason,--退款原因
        s1.description as description,--退款说明
        if(s2.delivery_time>0,1,0) as delivery_status,--是否发货
        s3.product_id as product_id--商品id
    from 
    (
        select
            order_sn,--订单号
            refund_reason,--退款原因
            description--退款说明
        from cc_ods_fs_refund_order
        where create_time>=1527782400 and create_time<=1530288000
    ) s1
    left join 
    (
        select
            order_sn,--订单号
            delivery_time
        from cc_order_user_delivery_time
        where ds >= 20180601
    ) s2 
    on s1.order_sn=s2.order_sn
    inner join 
    (
        select
            product_id,--商品id
            third_tradeno as order_sn--订单编号
        from cc_ods_dwxk_wk_sales_deal_ctime
        where ds>=20180401 and ds<=20180630
    ) s3
    on s1.order_sn=s3.order_sn
) t1
left join 
(
    select
        product_id,--商品id
        product_title,--商品名称
        shop_id,--店铺id
        shop_title,--店铺名称
        shop_cname1--一级类目
    from data.cc_dw_fs_products_shops
) t2
on t1.product_id=t2.product_id
//////////////////////////////////////////////////////////// 
廖宁临时需求某日 某商品 楚客信息及总监，总经理信息
select
    p1.product_id as product_id,
    p1.cck_uid as cck_uid,
    p1.real_name as real_name,
    p1.phone as phone,
    p1.leader_uid as leader_uid,--直属总监
    p1.leader_real_name as leader_real_name,--直属总监姓名
    p1.gm_uid as gm_uid,--直属总经理
    p2.real_name as gm_real_name,--直属经理姓名
    p1.order_count as order_count,--订单数
    p1.sales_num as sales_num,--销量
    p1.item_price as item_price,--支付金额
    p1.cck_commission as cck_commission --佣金
from
(
    select
        t1.product_id as product_id,
        t1.cck_uid as cck_uid,
        t1.real_name as real_name,
        t1.phone as phone,
        t1.leader_uid as leader_uid,--直属总监
        t2.real_name as leader_real_name,--直属总监姓名
        t1.gm_uid as gm_uid,--直属总经理
        t1.order_count as order_count,--订单数
        t1.sales_num as sales_num,--销量
        t1.item_price as item_price,--支付金额
        t1.cck_commission as cck_commission --佣金
    from
    (
        select
            n1.product_id as product_id,
            n1.cck_uid as cck_uid,
            n3.real_name as real_name,
            n3.phone as phone,
            n2.leader_uid as leader_uid,--直属总监
            n2.gm_uid as gm_uid,--直属总经理
            n1.order_count as order_count,--订单数
            n1.sales_num as sales_num,--销量
            n1.item_price as item_price,--支付金额
            n1.cck_commission as cck_commission --佣金
        from
        (
            select
                product_id,
                cck_uid,
                count(third_tradeno) as order_count, --订单数
                sum(sale_num) as sales_num,  --销量
                sum(item_price/100) as item_price,  --支付金额
                sum(cck_commission/100) as cck_commission --佣金
            from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
            where ds = '${stat_date}' and product_id = '${product_id}'
            group by product_id,cck_uid
        ) n1
        inner join 
        (
            select 
                cck_uid, --楚客id 
                leader_uid,--直属总监
                gm_uid--直属总经理
            from cc_ods_fs_wk_cct_layer_info
            union all
            select 
                gm_uid as cck_uid,--楚客id 
                leader_uid,--直属总监
                gm_uid--直属总经理
            from cc_ods_fs_wk_cct_layer_info
        ) n2
        on n1.cck_uid=n2.cck_uid
        inner join 
        (
            select
                cck_uid,
                real_name,
                phone
            from cc_ods_dwxk_fs_wk_business_info where ds=20180702
        ) n3
        on n1.cck_uid=n3.cck_uid
    ) t1
    left join 
    (
        select
            cck_uid,
            real_name
         from cc_ods_dwxk_fs_wk_business_info where ds=20180702
    ) t2
    on t1.leader_uid=t2.cck_uid
) p1
left join 
(
    select
       cck_uid,
       real_name
    from cc_ods_dwxk_fs_wk_business_info where ds=20180702
) p2
  on p1.gm_uid=p2.cck_uid
//////////////////////////////////////////////////////////
廖宁临时需求20180601-20180703限时抢购单坑商品推广人数、推广次数、Gmv、客单、佣金额
select
    m1.date as date,
    m1.product_id as product_id,
    m3.product_title as product_title,--商品名称
    m3.product_cname1,--一级类目
    m3.product_cname2,--二级类目
    m2.fx_cnt as fx_cnt,--总推广次数
    m2.fx_user_cnt as fx_user_cnt,--总推广人数
    m1.pay_count,--订单数
    m1.fee,--支付金额
    m1.cck_commission--佣金
from
(
    select
         p1.date as date,
         p1.product_id as product_id,
         count(distinct p2.third_tradeno) as pay_count,
         sum(p2.item_price/100) as fee,
         sum(p2.cck_commission/100) as cck_commission
    from
    (
        select
            n1.date,
            n2.product_id
        from
        (
            select
                from_unixtime(begin_time,'yyyyMMdd') as date,
                ad_material_id
            from 
                origin_common.cc_ods_fs_cck_xb_policies_hourly
            where 
                from_unixtime(begin_time,'yyyyMMdd')>='${begin_date}' 
            and 
                from_unixtime(begin_time-86400,'yyyyMMdd')<'${end_date}'
            and 
                ad_key like 'seckill-tab%'
        ) n1
        inner join
        (
            select
                product_id,
                ad_material_id
            from 
                origin_common.cc_ods_fs_cck_ad_material_products_hourly
            where 
                ad_material_id>0
        ) n2
        on n1.ad_material_id=n2.ad_material_id
    ) p1
    left join
    (
        select
            ds as date,
            product_id,
            third_tradeno,
            item_price,
            cck_commission,
            create_time
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where 
            ds>='${begin_date}' 
        and 
            ds<= '${end_date}'
    ) p2
    on p1.product_id=p2.product_id and p1.date=p2.date
    group by p1.date,p1.product_id
) m1 
left join
(
    select
        t1.ds as date,
        t3.product_id,
        count(t1.user_id) as fx_cnt,--总推广次数
        count(distinct t1.user_id) as fx_user_cnt--总推广人数
    from
    (
        select
            ds,
            ad_material_id as ad_id,
            user_id
        from 
            origin_common.cc_ods_log_cctapp_click_hourly
        where 
            ds>='${begin_date}' 
        and 
            ds<='${end_date}' 
        and 
            ad_type in ('search','category') 
        and 
            module in ('detail','detail_app') 
        and 
            zone='spread'
        union all
        select
            ds,
            ad_id,
            user_id
        from 
            origin_common.cc_ods_log_cctapp_click_hourly
        where 
            ds>='${begin_date}' 
        and 
            ds<='${end_date}' 
        and 
            ad_type not in ('search','category') 
        and 
            module in ('detail','detail_app') 
        and 
            zone='spread'
        union all
        select
            s1.ds,
            s2.ad_id,
            s1.user_id
        from
        (
            select
                ds,
                ad_material_id,
                user_id
           from 
               origin_common.cc_ods_log_cctapp_click_hourly
           where 
               ds>='${begin_date}'
           and 
               ds<='${end_date}' 
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
                ds>='${begin_date}'
            and 
                ds<='${end_date}'
        ) s2
        on s1.ad_material_id = s2.ad_material_id
    ) t1
    inner join
    (
        select
            ad_id,
            item_id
        from origin_common.cc_ods_fs_dwxk_ad_items_daily
    ) t2
    on t1.ad_id = t2.ad_id
    inner join
    (
        select
            item_id,
            app_item_id as product_id
        from origin_common.cc_ods_dwxk_fs_wk_items
    ) t3
    on t3.item_id = t2.item_id
    group by t1.ds,t3.product_id
) m2
on m1.product_id=m2.product_id and m1.date=m2.date
left join
(
    select
        product_id as product_id,
        product_title,--商品名称
        product_cname1,
        product_cname2
    from data.cc_dw_fs_products_shops
) m3
on m1.product_id= m3.product_id
///////////////////////////////////////////////////////////////////////////////////////////
钟江波需求0614-0713甄选的订单及收货人信息
select
    t1.ds,
    t1.product_id,
    t2.product_title,
    t2.shop_id, 
    t1.order_sn as order_sn,--订单数
    t1.sale_num,--销量
    t1.item_price as item_price,--支付金额
    t1.create_time,  
    t1.delivery_name, 
    t1.delivery_mobilephone,
    t1.delivery_address
from
   (select
        s1.ds as ds,
        s1.product_id as product_id,
        s1.order_sn as order_sn,--订单数
        s1.sale_num as sale_num ,--销量
        s1.item_price as item_price,--支付金额
        s1.create_time as create_time,  
        s2.delivery_name as delivery_name, 
        s2.delivery_mobilephone as delivery_mobilephone,
        s2.delivery_address as delivery_address
    from
       (select
            ds,
            product_id,
            third_tradeno as order_sn,--订单数
            sale_num,--销量
            (item_price/100) as item_price,--支付金额
            from_unixtime(create_time,'yyyyMMdd hh:mm' ) as create_time
        from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where ds>=20180614 and ds<=20180703 
       ) s1
       left join
       (select
            order_sn,
            delivery_name, 
            delivery_mobilephone,
            delivery_address
        from cc_order_user_delivery_time 
        where ds>=20180614 and ds<=20180709
       ) s2 
       on s1.order_sn=s2.order_sn
   ) t1
   inner join
   (select
        product_id,--商品id
        product_title,
        shop_id--店铺id
    from data.cc_dw_fs_products_shops
    where shop_id=18532
   ) t2
   on t1.product_id=t2.product_id
///////////////////////////////////////////////////////////////////////////////////////
徐冲需求推广id，审核状态，
select
   t1.shop_id as shop_id,
   t4.shop_title as shop_title,
   t4.shop_cname1 as shop_cname1,
   t3.app_item_id as product_id,
   t1.ad_name as product_name, 
   t1.ad_id as ad_id,
   t1.audit_status as audit_status, ---审核状态（0待审核，1审核通过，2驳回）
   t1.status as status---商品状态（0下架，1在架）  
from
   (
    select
        id,
        app_shop_id as shop_id,--店铺id
        ad_id,--推广id
        ad_name,--推广名称
        item_id, 
        audit_status, 
        status
    from 
        cc_ods_fs_dwxk_ad_items_daily
    where 
        app_shop_id in ()
   ) t1
   inner join
   (
    select
        max(id) as id,--最新的推广id
        app_shop_id as shop_id,
        item_id
    from 
        cc_ods_fs_dwxk_ad_items_daily
    where 
        app_shop_id in ()
    group by 
        item_id,app_shop_id
   ) t2
   on t1.id=t2.id
   inner join
   (
    select 
        item_id, 
        app_item_id
    from cc_ods_dwxk_fs_wk_items
    where shop_id in ()
   ) t3
   on t1.item_id=t3.item_id
   inner join 
   (
    select
        product_id,
        product_title,
        shop_id,
        shop_title,
        shop_cname1
    from data.cc_dw_fs_products_shops
   ) t4
   on t3.app_item_id=t4.product_id
//////////////////////////////////////////////////////////////////////////
徐媛媛临时需求0620-0710楚楚推每个行业的gmv、订单笔数、客单价
select
    t2.shop_cname1,
    sum(t1.order_count) as order_count,
    sum(t1.pay_fee) as pay_fee,
    sum(t1.pay_fee)/sum(t1.order_count) as per_order_price
from     
   (
    select
        product_id,
        count(distinct third_tradeno) as order_count,
        sum(item_price/100) as pay_fee 
    from cc_ods_dwxk_wk_sales_deal_ctime s1
    inner join 
         cc_ods_dwxk_fs_wk_cck_user      s2
    on s1.cck_uid=s2.cck_uid    
    where from_unixtime(s1.create_time,'yyyyMMdd')>=20180620 and from_unixtime(s1.create_time,'yyyyMMdd')<=20180710 and s2.platform=14 and s2.ds=20180710
    group by product_id
   ) t1
   left join 
   (
    select
        product_id,
        shop_cname1
    from data.cc_dw_fs_products_shops
   ) t2
   on t1.product_id=t2.product_id
   group by t2.shop_cname1
////////////////////////////////////////////////////////////////
徐媛媛临时需求0620-0710楚楚推每个行业的gmv、订单笔数、客单价 按日期分
select
    t1.ds,
    t2.shop_cname1,
    sum(t1.order_count) as order_count,
    sum(t1.pay_fee) as pay_fee,
    sum(t1.pay_fee)/sum(t1.order_count) as per_order_price
from     
   (
    select
        from_unixtime(s1.create_time,'yyyyMMdd') as ds,
        product_id,
        count(distinct third_tradeno) as order_count,
        sum(item_price/100) as pay_fee 
    from cc_ods_dwxk_wk_sales_deal_ctime s1
    inner join 
         cc_ods_dwxk_fs_wk_cck_user      s2
    on   s1.cck_uid=s2.cck_uid    
    where 
        from_unixtime(s1.create_time,'yyyyMMdd')>=20180620 
    and 
        from_unixtime(s1.create_time,'yyyyMMdd')<=20180710 
    and 
        s2.platform=14 
    and 
        s2.ds=20180710
    group by s1.create_time,product_id
   ) t1
   left join 
   (
    select
        product_id,
        shop_cname1
    from data.cc_dw_fs_products_shops
   ) t2
   on t1.product_id=t2.product_id
   group by t1.ds,t2.shop_cname1
////////////////////////////////////////////////////////////////////////
厉惠萱需求商品类目及券前价券后价
select
    h2.shop_id as shop_id,--店铺id
    h1.app_item_id as product_id,--商品id
    h3.product_title as product_title,--商品名称
    h3.product_cname1 as product_cname1,--一级类目
    h3.product_cname2 as product_cname2,--二级类目
    h3.product_cname3 as product_cname3,--三级类目
    h2.ad_id as ad_id,--推广id
    h2.ad_name as ad_name,--推广名称
    (h2.ad_price/100)as ad_price,---券前价
    (h2.cck_rate/1000) as cck_rate,---楚客佣金率
    (h2.cck_price/100) as cck_price,---楚客佣金额
    ((h2.cck_price/100)/(h2.cck_rate/1000)) as discount_price,---券后价
    h2.audit_status as audit_status,---审核状态（0待审核，1审核通过，2驳回）
    h2.status as status---商品状态（0下架，1在架）  
from
   (
    select 
        item_id,
        app_item_id--商品id
    from 
        cc_ods_dwxk_fs_wk_items
    where 
        shop_id in (18164,18335,17801,19141,19268,18532,19347)
   ) h1
   inner join
   (
    select
        t1.item_id as item_id,
        t1.app_shop_id as shop_id,--店铺id
        t1.ad_id as ad_id,--推广id
        t1.ad_name as ad_name,--推广名称
        t1.ad_price as ad_price,--券前价格 
        t1.cck_rate as cck_rate,---楚客佣金率
        t1.cck_price as cck_price,---楚客佣金额 
        t1.audit_status as audit_status,---审核状态（0待审核，1审核通过，2驳回） 
        t1.status as status---商品状态（0下架，1在架）
    from
       (
        select
            id,--id
            item_id,
            app_shop_id,--店铺id
            ad_id,--推广id 
            ad_name,--推广名称
            ad_price,--券前价格 
            cck_rate,---楚客佣金率 
            cck_price,---楚客佣金额 
            audit_status,---审核状态（0待审核，1审核通过，2驳回） 
            status---商品状态（0下架，1在架）  
        from 
           cc_ods_fs_dwxk_ad_items_daily
        where 
           app_shop_id in (18164,18335,17801,19141,19268,18532,19347)
       ) t1
       inner join
       (
        select
            max(id) as id, 
            item_id
        from 
            cc_ods_fs_dwxk_ad_items_daily
        where
            app_shop_id in (18164,18335,17801,19141,19268,18532,19347)
        group by 
            item_id
       ) t2
       on t1.id=t2.id
   ) h2
   on h1.item_id=h2.item_id
   left join
   (
    select
        product_id,--商品id
        shop_id,--店铺id
        shop_title,--店铺名称
        product_title,
        product_cname1,--一级类目
        product_cname2,--二级类目
        product_cname3--三级类目
   from 
        data.cc_dw_fs_products_shops
   where
        shop_id in (18164,18335,17801,19141,19268,18532,19347)
   ) h3
   on h1.app_item_id=h3.product_id
//////////////////////////////////////////////////////////////////////////////////////////
沈卫琴临时需求订单号 及是否发货后退款及理由
select
  n1.order_sn,
  n1.refund_reason
from
   (select
      a1.order_sn as order_sn,
      a1.product_id as product_id,
      a3.refund_reason as refund_reason
    from
       (select
           ds,
           product_id as product_id,
           third_tradeno as order_sn
        from
           origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where
           ds >= '${begin_date}'
        and
           ds <= '${end_date}'
       ) as a1
       inner join
       (select
           order_sn
        from
           origin_common.cc_order_user_delivery_time
        where
           ds >= '${begin_date}'  
       ) as a2
       on a1.order_sn = a2.order_sn
       inner join
       (select
           order_sn,
           from_unixtime(create_time,'yyyyMMdd') as refund_date,
           refund_reason
        from
           origin_common.cc_ods_fs_refund_order
        where
           create_time >= unix_timestamp('${begin_date}','yyyyMMdd')
       ) as a3
       on a1.order_sn = a3.order_sn
       where
          unix_timestamp(a3.refund_date,'yyyyMMdd')-unix_timestamp(a1.ds,'yyyyMMdd') <= 8*3600*24
   ) n1
   inner join 
   (select
       product_id,
       shop_id
    from 
       data.cc_dw_fs_products_shops
    where shop_id in (17791,18731) 
   ) n2
   on n1.product_id=n2.product_id
/////////////////////////////////////////////////////////////////////////////////////////////////
0717的廖宁临时需求
select
    t1.product_id as product_id,--商品id
    t2.product_title as product_title,--商品名称
    t2.product_cname1 as product_cname1,--商品一级类目
    t2.product_cname2 as product_cname2,--商品二级类目
    t2.product_cname3 as product_cname3,--商品三级类目
    (t4.user_count/t1.user_count) as self_buy_user_rate,--自买用户比例
    t1.order_count as order_count,--订单数
    t1.pay_fee as pay_fee,--支付金额
    t1.cck_commission as cck_commission,--直接佣金
    t1.pay_fee/t1.order_count as per_order_price,--客单价
    t3.fx_cnt as fx_cnt,--总推广次数
    t3.fx_user_cnt as fx_user_cnt--总推广人数
from     
(
    select
        s1.product_id as product_id,
        count(distinct cck_uid) as user_count, --有成交用户数
        count(distinct s1.third_tradeno) as order_count,
        sum(s1.item_price/100) as pay_fee, 
        sum(s1.cck_commission/100) as cck_commission--佣金
    from 
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1 
    where 
        s1.ds='${stat_date}'
    and   
        s1.product_id in ()
    group by 
        s1.product_id
) t1
left join 
(
    select
        product_id,
        product_title,
        product_cname1,--商品一级类目
        product_cname2,--商品二级类目
        product_cname3--商品三级类目
    from data.cc_dw_fs_products_shops
) t2
on t1.product_id=t2.product_id
left join
(
    select
        m3.product_id as product_id,
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
            zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC')
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
            zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC')
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
        m3.product_id in ()
    group by
        m3.product_id
) as t3
on t1.product_id = t3.product_id
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
          cck_uid,
          item_price,
          cck_commission
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where 
            ds = '${stat_date}' 
        and 
            product_id in ()
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
) as t4 
on t1.product_id = t4.product_id
/////////////////////////////////////////////////////////////////////////////////////
东哥 临时需求 生鲜店铺的超时发货数按大于48小时算 此使用与
select
    t3.shop_id as shop_id,--店铺id 
    sum(t6.count_delivery) as shop_delivery_num,--30日店铺维度发货数量
    sum(t6.count_delivery_overtime) as shop_count_delivery_overtime--30日店铺维度超时发货数
from
(
    select
        distinct 
        s1.product_id--商品id
    from 
        cc_ods_dwxk_wk_sales_deal_ctime s1
    inner join
        cc_ods_dwxk_fs_wk_cck_user s2
    on  
        s1.cck_uid=s2.cck_uid
    where 
        s1.ds>='${begin_date}' 
    and 
        s1.ds<='${end_date}' 
    and 
        s2.ds='${end_date}'
    and 
        s2.platform =14 
) t1
left join 
(
    select
        product_id,--商品id
        shop_id--店铺id
    from
        data.cc_dw_fs_products_shops
) t3
on t1.product_id=t3.product_id
left join
(        
    select
        a1.product_id,
        count(a2.order_sn) as count_delivery,--30日发货数
        sum(if(a2.delivery_time - a1.create_time > 172800,1,0)) as count_delivery_overtime--30超时发货数
    from
    (
        select
            distinct
            s1.product_id,
            s1.third_tradeno,
            s1.create_time
        from
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
        join
            origin_common.cc_ods_dwxk_fs_wk_cck_user s2
        on 
            s1.cck_uid=s2.cck_uid
        where
            s1.ds>= '${begin_date_30d}'
        and
            s1.ds<= '${end_date}'
        and
            s2.ds = '${end_date}'
        and
            s2.platform =14
    )a1
    left join
    (
        select
           order_sn,
           delivery_time
        from 
            origin_common.cc_order_user_delivery_time
        where 
            ds>='${begin_date_30d}' 
        and 
            ds<='${end_date}'
    )a2
    on a1.third_tradeno=a2.order_sn
    group by
        a1.product_id
) t6
on t1.product_id=t6.product_id
where t3.shop_id in (18662,18729,18588,18740,18799,19319,19405,19402,20216,18965,20696)
group by t3.shop_id
/////////////////////////////////////////////////////////////////
lily需求某商品 某日 分小时记录付款单数 支付金额推广人数次数
select
    s1.product_id                    as product_id,--商品id
    s1.hour                          as hour,--小时
    count(distinct s1.third_tradeno) as order_count,
    sum(s1.item_price/100)           as item_price--支付金额
from
(
    select
        product_id                                 as product_id,
        from_unixtime(create_time,'yyyyMMdd HH')  as hour,
        third_tradeno,                              
        item_price                                 as item_price--支付金额
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where
        ds = '${stat_date}'
    and
        product_id = '${product_id}'
) s1
group by 
    s1.product_id,s1.hour
////////////////////////////////////////////////////////////////////
lily需求某商品 某日 分小时记录推广人数次数 和上表没成功关联在一张表里，因为hour格式不一样。
select
    m3.product_id     as product_id,--商品id
    m1.hour           as hour,
    count(m1.user_id) as fx_cnt,--总推广次数
    count(distinct m1.user_id) as fx_user_cnt--总推广人数
from
(
    select
       hour,
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
       hour,
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
       s1.hour as hour, 
       s2.ad_id,
       s1.user_id
    from
    (
        select
            hour,
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
    ) as s1
    inner join
    (
        select
           distinct ad_material_id as ad_material_id,
           ad_id
        from 
           data.cc_dm_gwapp_new_ad_material_relation_hourly
        where 
           ds = '${stat_date}'
    ) as s2
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
) as m2
on 
    m1.ad_id = m2.ad_id
inner join
(
    select
       item_id,
       app_item_id as product_id
    from 
       origin_common.cc_ods_dwxk_fs_wk_items
) as m3
on 
    m3.item_id = m2.item_id
where
    m3.product_id = '${product_id}'
group by
    m3.product_id,m1.hour
////////////////////////////////////////////////////////////////////////////////////////////
张梨娜需求 某日 某商品 订单号，销售金额 收货地区,看某地区销售占比
注意：取每单收货地址，要用cc_order_user_pay_time，因为发货表有点滞后。
select
    t1.product_id,
    t1.order_sn,
    t1.pay_time,
    t1.pay_fee,
    t2.delivery_address,
    t2.area_id,
    t3.city_name,
    t3.province_name 
from 
(
    select
        product_id       as product_id,
        third_tradeno    as order_sn,
        from_unixtime(create_time,'yyyyMMdd HH:mm') as pay_time,
        (item_price/100) as pay_fee
    from 
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime 
    where
        ds='${stat_date}'
    and
        product_id = '${product_id}'
) t1
left join 
(
    select
        order_sn,
        delivery_address,
        area_id
    from
        origin_common.cc_order_user_pay_time
) t2
on t1.order_sn = t2.order_sn
left join 
(
    select
        area_id,
        city_name,
        province_name
    from 
        origin_common.cc_area_city_province 
) t3
on t2.area_id = t3.area_id
/////////////////////////////////////////////////////////////////////////////////////////////
王来平需求某时间段 楚楚推 所有商品退款单详细信息
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
////////////////////////////////////////////////////////////////////////
王来平需求0601-0716商品退款单详细信息
select
    n3.shop_id       as shop_id,--店铺id
    n3.product_title as product_title,--商品名称
    n1.product_id    as product_id,--商品id
    n2.order_sn      as order_sn,--订单编号
    n1.order_time    as order_time,--付款时间
    n1.order_price   as order_price,--付款金额
    n2.refund_reason as refund_reason,--退款原因
    n2.success_price as success_price--退款金额
from
   (
    select
        distinct
        product_id,
        third_tradeno,
        from_unixtime(create_time,'yyyyMMdd HH:mm:ss') as order_time,
        (item_price/100) as order_price
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where
        ds >= '${begin_date_30}'
    and
        ds <= '${end_date}'
   )    n1
   inner join
   (
    select
        distinct
        order_sn,--订单编号
        refund_reason,--退款原因
        success_price--退款金额
    from
        origin_common.cc_ods_fs_refund_order
    where
        create_time >= unix_timestamp('${begin_date}','yyyyMMdd')
    and
        create_time <= unix_timestamp('${end_date}','yyyyMMdd')
    and 
        shop_id=18704
    and 
        status=1
   )    n2
   on n1.third_tradeno = n2.order_sn
   inner join
   (
    select
        product_id,--商品id
        product_title,
        shop_id--店铺id
    from 
        data.cc_dw_fs_products_shops
    where 
        shop_id =18704
   )    n3
   on n1.product_id=n3.product_id
////////////////////////////////////////////////////////////////////////////////////
徐媛媛临时需求楚楚推上半年 每月 每个类目的支付金额、订单数/*中间的都是注释 */ 
/*select
    t1.month,
    t2.product_cname1,
    sum(t1.order_count) as order_count,
    sum(t1.pay_fee) as pay_fee
from     
   (
    select
        from_unixtime(s1.create_time,'yyyyMM') as month,
        product_id,
        count(distinct third_tradeno) as order_count,
        sum(item_price/100) as pay_fee 
    from cc_ods_dwxk_wk_sales_deal_ctime s1   
    where from_unixtime(s1.create_time,'yyyyMMdd')>=20180101 and from_unixtime(s1.create_time,'yyyyMMdd')<=20180630 
    group by s1.create_time,product_id
   ) t1
left join 
   (
    select
        product_id,
        product_cname1
    from data.cc_dw_fs_products_shops
   ) t2
on t1.product_id=t2.product_id
group by t1.month,t2.product_cname1*/
//////////////////////////////////////////////////////////////////////////
廖宁需求某日某商品 销售或自购 TOP10 总监名单
select
    *
from 
   (
      select
          t2.leader_uid,
          t3.real_name,
          t3.phone,
          sum(t1.item_price ) as item_price
      from 
        (
          select
              cck_uid,
              (item_price/100) as item_price
          from
              origin_common.cc_ods_dwxk_wk_sales_deal_ctime
          where 
              ds='${stat_date}'
          and 
              product_id='${product_id}'
        ) t1
        inner join
        (
          select
              distinct
              n1.leader_uid as leader_uid,
              n1.cck_uid    as cck_uid
          from
          (
            select
                leader_uid,
                cck_uid
            from 
                origin_common.cc_ods_fs_wk_cct_layer_info
            union all
            select
                leader_uid,
                leader_uid as cck_uid
            from 
                origin_common.cc_ods_fs_wk_cct_layer_info
          ) n1
        ) t2
        on t1.cck_uid=t2.cck_uid
        inner join
        (
          select
              cck_uid,
              real_name,
              phone
          from 
              origin_common.cc_ods_dwxk_fs_wk_business_info 
          where 
              ds='${stat_date}'
        ) t3
        on t2.leader_uid=t3.cck_uid
        group by t2.leader_uid, t3.real_name,t3.phone
        order by item_price DESC
    ) p1
    limit 10
////////////////////////////////////////////////////////////////////////////////
廖宁需求某日某商品 销售TOP10 总监名单
select
    *
from
   (  
    select
        p1.*,
        rank () over (partition by p1.leader_uid order by p1.item_price/p2.item_price DESC) as num
    from 
      (
       select
           t2.leader_uid as leader_uid,
           t1.cck_uid    as cck_uid,
           t3.real_name  as real_name,
           t3.phone      as phone,
           t1.item_price as item_price
       from 
         (
          select
              cck_uid,
              (item_price/100) as item_price
          from
              origin_common.cc_ods_dwxk_wk_sales_deal_ctime
          where 
              ds='${stat_date}'
          and 
              product_id='${product_id}'
         ) as t1
         inner join
         (
          select
              distinct
              n1.leader_uid as leader_uid,
              n1.cck_uid    as cck_uid
          from
             (
              select
                  leader_uid,
                  cck_uid
              from 
                  origin_common.cc_ods_fs_wk_cct_layer_info
              union all
              select
                  leader_uid,
                  leader_uid as cck_uid
              from 
                  origin_common.cc_ods_fs_wk_cct_layer_info
             ) n1
         ) as t2
         on t1.cck_uid=t2.cck_uid
         inner join
         (
          select
              cck_uid,
              real_name,
              phone
          from 
              origin_common.cc_ods_dwxk_fs_wk_business_info 
          where 
              ds='${stat_date}'
         ) as t3
          on t1.cck_uid=t3.cck_uid
      ) p1
      inner join 
      (
       select
          *
       from 
         (
          select
              t2.leader_uid      as leader_uid,
              t3.real_name       as real_name,
              t3.phone           as phone,
              sum(t1.item_price) as item_price
          from 
             (
              select
                  cck_uid,
                  (item_price/100) as item_price
              from
                  origin_common.cc_ods_dwxk_wk_sales_deal_ctime
              where 
                  ds='${stat_date}'
              and 
                  product_id='${product_id}'
             ) as t1
             inner join
             (
              select
                  distinct
                  n1.leader_uid as leader_uid,
                  n1.cck_uid    as cck_uid
              from
                 (
                  select
                      leader_uid,
                      cck_uid
                  from 
                      origin_common.cc_ods_fs_wk_cct_layer_info
                  union all
                  select
                      leader_uid,
                      leader_uid as cck_uid
                  from 
                      origin_common.cc_ods_fs_wk_cct_layer_info
                 ) n1
             ) as t2
             on t1.cck_uid=t2.cck_uid
             inner join
             (
              select
                  cck_uid,
                  real_name,
                  phone
              from 
                  origin_common.cc_ods_dwxk_fs_wk_business_info 
              where 
                  ds='${stat_date}'
             ) as t3
             on t2.leader_uid=t3.cck_uid
             group by t2.leader_uid,t3.real_name,t3.phone
             order by item_price DESC
         ) s1
       limit 10
      ) p2
      on p1.leader_uid=p2.leader_uid
   ) as H
   where H.num<=10
/////////////////////////////////////////////////////////////////////////////////////////////
魏薇需求 某日某商品 部分vip的销售及推广数据
select
    t1.product_id as product_id,--商品id
    t2.product_title as product_title,--商品名称
    t2.product_cname1 as product_cname1,--商品一级类目
    t2.product_cname2 as product_cname2,--商品二级类目
    t2.product_cname3 as product_cname3,--商品三级类目
    (t4.user_count/t1.user_count) as self_buy_user_rate,--自买用户比例
    t1.order_count as order_count,--订单数
    t1.pay_fee as pay_fee,--支付金额
    t1.cck_commission as cck_commission,--直接佣金
    t1.pay_fee/t1.order_count as per_order_price,--客单价
    t3.fx_cnt as fx_cnt,--总推广次数
    t3.fx_user_cnt as fx_user_cnt--总推广人数
from     
   (
    select
        n1.product_id as product_id,
        count(distinct n1.cck_uid) as user_count, --有成交用户数
        count(distinct n1.third_tradeno) as order_count,
        sum(n1.item_price/100) as pay_fee, 
        sum(n1.cck_commission/100) as cck_commission--佣金
    from 
        (
         select
             product_id,
             cck_uid, 
             third_tradeno,
             item_price, 
             cck_commission
         from 
             cc_ods_dwxk_wk_sales_deal_ctime 
         where 
             ds='${stat_date}'
         and   
             product_id in (1100185322351)
        ) n1
        inner join
        (
         select
             distinct
             s1.gm_uid as gm_uid,
             s1.cck_uid as cck_uid
         from 
            (              
             select
                 gm_uid,
                 cck_uid
             from 
                 cc_ods_fs_wk_cct_layer_info
             union all
             select
                 gm_uid,
                 gm_uid as cck_uid
             from
                 cc_ods_fs_wk_cct_layer_info
            ) as s1
             where 
                 s1.gm_uid not in(1199168,1199210,1199214,1199305,1199321,1199365,1199515,1199621,1199749,1199956,1199978,1199985,1200483,1200648,
                           1201128,1201288,1202494,1204007,1204049,1205821,1209314,1210498,1240629,1240633,1241239)
        ) n2
        on n1.cck_uid=n2.cck_uid
        group by n1.product_id
   ) t1
   left join 
   (
    select
        product_id,
        product_title,
        product_cname1,--商品一级类目
        product_cname2,--商品二级类目
        product_cname3--商品三级类目
    from data.cc_dw_fs_products_shops
   ) t2
   on t1.product_id=t2.product_id
   left join
   (
    select
        m3.product_id as product_id,
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
            ds = '${stat_date}' and ad_type in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
        union all
        select
            ad_id,
            user_id
        from 
            origin_common.cc_ods_log_cctapp_click_hourly
        where 
            ds = '${stat_date}' and ad_type not in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
        union all
        select
            s2.ad_id,
            s1.user_id
        from
            (select
                ad_material_id,
                user_id
             from
                origin_common.cc_ods_log_cctapp_click_hourly
             where 
                ds = '${stat_date}' and module='vip' and ad_type in ('single_product','9_cell') and zone in ('material_group-share','material_moments-share')
            ) s1
            inner join
            (select
                distinct ad_material_id as ad_material_id,
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
       (select
            ad_id,
            item_id
        from 
            origin_common.cc_ods_fs_dwxk_ad_items_daily
       ) as m2
       on m1.ad_id = m2.ad_id
       inner join
       (select
            item_id,
            app_item_id as product_id
        from 
            origin_common.cc_ods_dwxk_fs_wk_items
       ) as m3
       on m3.item_id = m2.item_id
       inner join
       (select
            distinct
            a2.cct_uid as cct_uid
        from
           (
            select
                distinct
                s1.gm_uid as gm_uid,
                s1.cck_uid as cck_uid
            from 
              (              
               select
                   gm_uid,
                   cck_uid
               from 
                   cc_ods_fs_wk_cct_layer_info
               union all
               select
                   gm_uid,
                   gm_uid as cck_uid
               from
                   cc_ods_fs_wk_cct_layer_info
              ) as s1 
               where 
                   s1.gm_uid not in(1199168,1199210,1199214,1199305,1199321,1199365,1199515,1199621,1199749,1199956,1199978,1199985,1200483,1200648,
                           1201128,1201288,1202494,1204007,1204049,1205821,1209314,1210498,1240629,1240633,1241239)
           ) as a1
           inner join 
           (
            select
                cck_uid,
                cct_uid
            from 
                origin_common.cc_ods_fs_tui_relation 
           ) as a2
           on a1.cck_uid=a2.cck_uid
       ) as m4
       on m1.user_id=m4.cct_uid
       where
          m3.product_id in (1100185322351)
       group by
          m3.product_id
   ) as t3
   on t1.product_id = t3.product_id
   left join
   (
    select
        n1.product_id,
        count(distinct n1.third_tradeno) as order_count,--自买订单数
        count(distinct n1.cck_uid) as user_count--自买用户数
    from
       (
        select
            a1.product_id as product_id,
            a1.third_tradeno as third_tradeno,
            a1.cck_uid as cck_uid
        from
           (
            select
                product_id,
                third_tradeno,
                cck_uid
            from 
                origin_common.cc_ods_dwxk_wk_sales_deal_ctime
            where 
                ds = '${stat_date}' and product_id in (1100185322351)
           ) as a1
           inner join
           (
            select
                distinct
                s1.gm_uid as gm_uid,
                s1.cck_uid as cck_uid
            from 
              (              
               select
                   gm_uid,
                   cck_uid
               from 
                   cc_ods_fs_wk_cct_layer_info
               union all
               select
                   gm_uid,
                   gm_uid as cck_uid
               from
                   cc_ods_fs_wk_cct_layer_info
              ) as s1
               where 
                   s1.gm_uid not in(1199168,1199210,1199214,1199305,1199321,1199365,1199515,1199621,1199749,1199956,1199978,1199985,1200483,1200648,
                           1201128,1201288,1202494,1204007,1204049,1205821,1209314,1210498,1240629,1240633,1241239)
           ) as a2
           on a1.cck_uid=a2.cck_uid
       ) as n1 
       inner join
       (
        select 
            distinct 
            order_sn
        from  
            origin_common.cc_ods_log_gwapp_order_track_hourly
        where 
            ds = '${stat_date}' and source='cctui'
       ) as n2
       on n1.third_tradeno = n2.order_sn
       group by n1.product_id
   ) as t4 
   on t1.product_id = t4.product_id
////////////////////////////////////////////////////////////////////////////////////
东哥需求某时间段，某店铺工单号等
select
    s1.shop_id,
    s1.id,--工单号
    s1.order_id,--订单号
    s1.is_overtime--超时工单数
from
(
    select
        shop_id,
        order_id,
        id,
        is_overtime
    from 
        cc_ods_fs_task
    where 
        from_unixtime(created_on,'yyyyMMdd')>='${begin_date}' 
    and 
        from_unixtime(created_on,'yyyyMMdd')<='${end_date}'
    and 
        shop_id in (11520,17738,17803,18335,18398,18588,18775,18846,18893,19405)
) s1 
inner join
(
    select
        distinct 
        m1.third_tradeno as order_sn--订单号
    from 
        cc_ods_dwxk_wk_sales_deal_ctime m1
    inner join
        cc_ods_dwxk_fs_wk_cck_user m2
    on  
        m1.cck_uid=m2.cck_uid
    where 
        m1.ds>='${begin_date}' 
    and 
        m1.ds<='${end_date}' 
    and 
        m2.platform =14 
    and 
        m2.ds='${end_date}'
) s2
on s1.order_id=s2.order_sn
/////////////////////////////////////////////////////////////////////////////
徐冲需求 某商品最新在售与否状态 大微信客小二后台商品最新推广信息
select
    h2.app_item_id  as product_id,--商品id
    h1.audit_status as audit_status,---审核状态（0待审核，1审核通过，2驳回）
    h1.status       as status---商品状态（0下架，1在架）  
from
(
    select 
        item_id,
        app_item_id
    from  cc_ods_dwxk_fs_wk_items
    where app_item_id in()
) h2
inner join
(
    select
        t1.item_id      as item_id, 
        t1.audit_status as audit_status,
        t1.status       as status
    from
    (
        select
            id,
            item_id, 
            audit_status, 
            status
        from cc_ods_fs_dwxk_ad_items_daily
    ) t1
    inner join
    (
        select
            max(id) as id, 
            item_id
        from  cc_ods_fs_dwxk_ad_items_daily
        group by item_id
    ) t2
    on t1.id=t2.id
) h1
on h2.item_id=h1.item_id
/////////////////////////////////////////////////////////////////////
徐冲 某店铺所有在售商品
select
    h3.shop_id,--店铺id
    h1.app_item_id  as product_id,--商品id
    h3.product_title,
    h3.product_cname1,
    h3.product_cname2,
    h3.product_cname3
from
(
    select 
        item_id,
        app_item_id
    from   
        origin_common.cc_ods_dwxk_fs_wk_items
    where shop_id in(17530)
) h1
inner join
(
    select
        item_id,
        max(id) as id
    from  
         origin_common.cc_ods_fs_dwxk_ad_items_daily
    where 
        audit_status =1 
    and 
        status = 1
    group by item_id
) h2
on h1.item_id=h2.item_id
left join 
(
    select
        product_id,--商品id
        product_title,
        shop_id,--店铺id
        product_cname1,
        product_cname2,
        product_cname3
    from data.cc_dw_fs_products_shops
) h3
on h1.app_item_id=h3.product_id
left join 
(
    select
        distinct
        item_id,
        ad_brand_id
    from 
        origin_common.cc_ods_dwxk_fs_wk_ad_items
) h4
on h1.item_id=h4.item_id
left join 
(
    select
        distinct
        id,
        brand_name
    from 
        origin_common.cc_brand
) h5 
on h4.ad_brand_id=h5.id
////////////////////////////////////////////////////////////////////
徐冲需求 所有pop店铺名称，一级类目，商品名称，商品ID，品牌名称
select
    s1.app_shop_id,
    s3.shop_title,
    s3.shop_cname1,
    s2.product_id,
    s3.product_title,
    s4.brand_name
from 
(
    select
        distinct
        app_shop_id,
        item_id,
        ad_brand_id
    from 
        origin_common.cc_ods_dwxk_fs_wk_ad_items
) s1
left join 
(
    select
        item_id,
        app_item_id as product_id
    from 
        origin_common.cc_ods_dwxk_fs_wk_items
) s2 
on s1.item_id = s2.item_id
left join
(
    select
        product_id,
        shop_title,
        shop_cname1,
        product_title
    from 
        data.cc_dw_fs_products_shops
) s3
on s2.product_id = s3.product_id
left join 
(
    select
        distinct
        id,
        brand_name
    from 
        origin_common.cc_brand
) s4
on s1.ad_brand_id = s4.id
where 
   s1.app_shop_id in (pop店铺id) 
//////////////////////////////////////////////////////////////////
商品部需求线上pma port=4035 ssd  op_product_map 拉取供应商系统商品库存、供货价
select
    t1.pm_sid,
    t1.pm_pid,
    t1.pm_title,
    t2.pb_stock,--库存数量
    t2.pb_price,--供货价
    max(t2.pb_batch),--商品批次
    t1.pm_mpid,--核验关联字段是否一致
    t2.pb_mpid
from
(
    select
        pm_sid,
        pm_pid,
        pm_mpid,
        pm_title
    from 
        op_products_map
    where 
        pm_sid in (18164,18335,17801,19141,19268,18532,19347)
) t1
join
(
    select 
        pb_mpid,
        pb_stock,
        pb_price,
        pb_batch
    from 
        op_product_batches
    where 
        pb_mpid>0
) t2
on t1.pm_mpid=t2.pb_mpid
group by t1.pm_sid,t1.pm_pid,t1.pm_title,t2.pb_stock,t2.pb_price,t1.pm_mpid,t2.pb_mpid
/////////////////////////////////////////////////////////////////////////////////////////////////////////
陈洁需求 pop店铺 所有商品 七月份销售数据
select
    t2.shop_id,
    t1.product_id,
    t2.product_title,
    t1.pay_fee
from
(
    select
        product_id,
        sum(item_price)/100 as pay_fee
    from 
        cc_ods_dwxk_wk_sales_deal_ctime
    where
        ds>=20180701
    and 
        ds<=20180731
    group by product_id 
) t1
left join 
(
    select
        product_id,
        product_title,
        shop_id
    from 
        data.cc_dw_fs_products_shops
) t2
on t1.product_id = t2.product_id
where t2.shop_id in(pop shop_id)
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
翔哥复购数据
select
    h0.first_cid,
    count(distinct h0.uid) as pay_uv_7d,--七天内有购买的人数
    sum(if(h0.pay_count>=2,1,0)) as again_pay_uv_7d,--七天内复购的人数
    count(distinct h1.uid) as again_pay_uv_15d--
from
 (
    select
      t1.first_cid as first_cid,
      t1.uid as uid,
      count(t1.third_tradeno) as pay_count
    from
    (
      select
          first_cid,
          cck_uid,
          uid,
          third_tradeno
      from 
          origin_common.cc_ods_dwxk_wk_sales_deal_ctime
      where ds>='${begin_date}' and ds <= '${begin_date_7d}' 
    )t1
    inner join
    (
    select
        cck_uid
    from origin_common.cc_ods_dwxk_fs_wk_cck_user 
    where ds=20180806 and platform=14  
    )t2
    on t2.cck_uid=t1.cck_uid
    group by t1.first_cid,t1.uid
) h0
left join
(
    select
      t3.first_cid as first_cid,
      t3.uid as uid
    from
    (
        select 
            distinct
            first_cid,
            cck_uid,
            uid
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where 
            ds>='${begin_date_7d}' 
        and 
            ds <= '${begin_date_15d}' 
    )t3
    inner join
    (
    select
        cck_uid
    from origin_common.cc_ods_dwxk_fs_wk_cck_user 
    where ds=20180806 and platform=14  
    )t4
    on t4.cck_uid=t3.cck_uid
)h1
        on h1.uid=h0.uid and h1.first_cid=h0.first_cid
group by h0.first_cid
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
翔哥新型写法
select
    g0.product_cname3 as product_cname3,
    g0.uid as uid,
    count(g0.third_tradeno) as pay_count
from
(
    select
        t1.product_id,
        t3.product_cname3,
        t1.cck_uid,
        t1.uid,
        t1.third_tradeno
    from 
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime t1
    inner join
        origin_common.cc_ods_dwxk_fs_wk_cck_user t2
    on
        t1.cck_uid=t2.cck_uid
    left join
        data.cc_dw_fs_products_shops t3
    on
        t3.product_id=t1.product_id
    where 
        t1.ds>'${begin_date}' 
    and 
        t1.ds <= '${begin_date_7d}' 
    and 
        t2.ds=20180806 
    and 
        t2.platform=14
) g0
group by 
    g0.product_cname3,g0.uid
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
我的需求每周 日均新增vip 日均dau 日均ipv_uv 日均推广人数 支付金额 直接佣金 付款单数
select
    t1.avg_new_cck_cnt,
    t2.avg_dau,
    t3.avg_ipv_uv,
    t4.avg_fx_cnt,
    t5.avg_pay_fee,
    t5.avg_cck_commission,
    t5.avg_order_count
from
(
    select
        1 as tab,
        avg(s1.new_cck_cnt) as avg_new_cck_cnt
    from
    (   
        select
            from_unixtime(create_time,'yyyyMMdd') as ds,
            count(distinct cck_uid)               as new_cck_cnt
        from
            origin_common.cc_ods_fs_wk_cct_layer_info 
        where
            from_unixtime(create_time,'yyyyMMdd') >= '${begin_date_7d}'
        and 
            from_unixtime(create_time,'yyyyMMdd') <= '${end_date}'
        and 
            platform = 14
        group by
            from_unixtime(create_time,'yyyyMMdd')
    ) s1
) t1
left join 
(
    select
        1 as tab,
        avg(s1.dau) as avg_dau
    from
    (
        select
            ds,
            count(distinct cct_uid) as dau
        from
            origin_common.cc_ods_log_gwapp_pv_hourly
        where
            ds >= '${begin_date_7d}'
        and 
            ds <= '${end_date}'
        and 
            module = 'https://app-h5.daweixinke.com/chuchutui/index.html'
        and 
            cct_uid is not null
        group by 
            ds
    ) s1
) t2
on t1.tab = t2.tab
left join 
(
    select
        1 as tab,
        avg(s1.ipv_uv) as avg_ipv_uv
    from
    (
        select
            m1.ds,
            sum(m1.ipv_uv) as ipv_uv
        from
        (
            select
                ds,
                product_id,
                count (distinct user_id) as ipv_uv
            from
                origin_common.cc_ods_log_cctui_product_coupon_detail_hourly
            where 
                ds >= '${begin_date_7d}'
            and 
                ds <= '${end_date}'
            and 
                detail_type = 'item'
            group by
                ds,product_id    
        ) m1
        group by 
            m1.ds
    ) s1
) t3
on t1.tab = t3.tab
left join
(
    select
        1 as tab,
        avg(s1.fx_cnt)      as avg_fx_cnt,
        avg(s1.fx_user_cnt) as avg_fx_user_cnt
    from 
    (
        select
            n1.ds as ds,
            count(n1.user_id)          as fx_cnt,
            count(distinct n1.user_id) as fx_user_cnt
        from 
        (
            select
                ds,
                user_id 
            from
                origin_common.cc_ods_log_cctapp_click_hourly
            where 
                ds >= '${begin_date_7d}'
            and 
                ds <= '${end_date}'
            and 
                ad_type in ('search','category')
            and 
                module in ('detail','detail_app')
            and 
                zone ='spread' 
            union all
            select
                ds,
                user_id 
            from
                origin_common.cc_ods_log_cctapp_click_hourly
            where 
                ds >= '${begin_date_7d}'
            and 
                ds <= '${end_date}'
            and 
                ad_type not in ('search','category')
            and 
                module in ('detail','detail_app')
            and 
                zone ='spread' 
            union all
            select
                ds,
                user_id
            from
                origin_common.cc_ods_log_cctapp_click_hourly
            where 
                ds >= '${begin_date_7d}'
            and 
                ds <= '${end_date}'
            and 
                module='vip' 
            and 
                ad_type in ('single_product','9_cell') 
            and 
                zone in ('material_group-share','material_moments-share')
        ) n1
        group by n1.ds
    ) s1
) t4
on t1.tab = t4.tab
left join 
(
    select
        1 as tab,
        avg(s1.pay_fee)        as avg_pay_fee,
        avg(s1.cck_commission) as avg_cck_commission,
        avg(s1.order_count)    as avg_order_count
    from
    ( 
        select
            m1.ds as ds,
            sum(m1.item_price/100) as pay_fee,
            sum(m1.cck_commission/100) as cck_commission,
            count(distinct m1.third_tradeno) as order_count
        from
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime m1
        join 
            origin_common.cc_ods_dwxk_fs_wk_cck_user      m2
        on  
            m1.cck_uid = m2.cck_uid
        where 
            m1.ds >= '${begin_date_7d}'
        and 
            m1.ds <= '${end_date}'
        and 
            m2.platform = 14
        and 
            m2.ds = '${end_date}'
        group by 
            m1.ds
    ) s1
) t5
on t1.tab=t5.tab
/////////////////////////////////////////////////////////////////////////////////////////////////////////
近期工作代码因每次编辑不保存，重装系统导致丢失，现回顾一下重要的内容
1.某段时间没发货的礼包(每个礼包对应一个商品ID)信息， 取法：从此表cc_ods_order_gift_products_user_pay_time 以商品ID为维度 
  取订单号等信息 在关联发货表
2.某段时间 买礼包 后无下单数的 楚客信息， 取法：cc_ods_dwxk_fs_wk_business_info ds=等于最新，create_time相应日期范围，pay_price!=0的所有cck_uid
  靠cck_uid关联，相应日期范围大微信客支付订单表 过滤条件where 要返回的cck_uid is null
3.某段时间 买礼包 后自买下单数为一的 楚客信息，
4.某段时间 买礼包 后推广一单再无下单的楚客信息，
5.曹邵斌 8.13-8.23 178团队每个总经理下面 邀请楚客购买399创业礼包的 vip人数  

///////////////////////////////////////////////////////////////////////////////////////////
廖宁需求某日某商品 销售TOP10 名单信息
select
    t1.*
from
(
    select
        n1.cck_uid,
        n2.real_name,
        n2.phone,
        sum(n1.sale_num) as sales_num,
        count(distinct n1.third_tradeno) as order_count,
        sum(n1.item_price/100) as pay_fee,
        sum(n1.cck_commission/100) as cck_commission
    from 
    (
        select
            cck_uid,
            third_tradeno,
            sale_num,
            item_price,
            cck_commission
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_hourly
        where 
            ds = '${stat_date}' 
        and 
            product_id = '${product_id}'
    ) n1
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
            ds = 20180829
    ) n2
    on n1.cck_uid = n2.cck_uid
    group by n1.cck_uid,n2.real_name,n2.phone
    order by pay_fee DESC
) t1
limit 30
/////////////////////////////////////////////////////
刘红艳
select n11.* 
from 
(
    select 
    n1.refund_sn,n1.product_id,n10.product_cname1,n2.order_sn,n2.refund_reason,n2.status,n2.create_time,n2.stop_time,n2.success_price,n6.pay_fee
from 
(select refund_sn,product_id
from 
   cc_ods_fs_refund_products)n1
left join
(select  order_sn,refund_sn,refund_reason,status,from_unixtime(create_time,'yyyyMMdd HH:mm:ss')as create_time,from_unixtime(stop_time,'yyyyMMdd  HH:mm:ss')as stop_time ,success_price
from origin_common.cc_ods_fs_refund_order
where  from_unixtime(create_time,'yyyyMMdd')>='${begin_date}'
and from_unixtime(create_time,'yyyyMMdd') <='${end_date}') as n2
on n1.refund_sn=n2.refund_sn
left join
(select refund_sn,sum(item_price/100) as pay_fee
from
(select product_id,refund_sn from cc_refund_products)as n4
left join 
(select product_id, third_tradeno,item_price
from 
 origin_common.cc_ods_dwxk_wk_sales_deal_ctime)as n5
on n4.product_id=n5.product_id 
group by refund_sn ) n6
on n1.refund_sn=n6.refund_sn
left join
(select product_id ,product_cname1 from
data.cc_dw_fs_products_shops )as n10
on  n1.product_id =n10.product_id)as n11
inner join 
origin_common.cc_ods_dwxk_wk_sales_deal_ctime n9
on n11.order_sn=n9.third_tradeno
group by n11.refund_sn,n11.product_id,n11.product_cname1,n11.order_sn,n11.refund_reason,n11.status,n11.create_time,n11.stop_time,n11.success_price,n11.pay_fee;
////////////////////////////////////////////////////////////////////////
曹邵斌 8.26-8.30178团队新增人数 
select
    gm_uid,
    count(distinct cck_uid) as new_vip  
from 
    origin_common.cc_ods_fs_wk_cct_layer_info
where 
    from_unixtime(create_time,'yyyyMMdd') >= 20180826
and 
    from_unixtime(create_time,'yyyyMMdd') <= 20180830 
and 
    gm_uid in (1199321,1199168,1197475,1202494,1210498,1200648,1204007,1240629,1199210,1209314,1199978,1199305,1199214,1199749,1201288,
               1205821,1201128,1240633,1199956,1199621,1199365,1204049,1241239,1200483,1252167,1199985,1199515,1199635,1257637)
group by 
    gm_uid
////////////////////////////////////////////////////////////////////////////
魏薇需求 连续28天有销售的楚客信息 我写的是累计了
select
    cck_uid,
    count(distinct ds) as num   
from
    origin_common.cc_ods_dwxk_wk_sales_deal_ctime
where 
    ds >= '${begin_date}'
and 
    ds <= '${end_date}'
group by
    cck_uid 
having 
    num>=28
////////////////////////////////////////////////////////////////////////////////////
郑羽佳 8月29号的JM面膜178团队的销售额占比
select
    n1.gm_uid as gm_uid,
    sum(n2.order_num )as order_num,
    sum(n2.pay_fee) as pay_fee  
from 
(
    select
        gm_uid,
        cck_uid
    from 
        origin_common.cc_ods_fs_wk_cct_layer_info
    where 
        gm_uid in (1199321,1199168,1197475,1202494,1210498,1200648,1204007,1240629,1199210,1209314,1199978,1199305,1199214,1199749,
                   1201288,1205821,1201128,1240633,1199956,1199621,1199365,1204049,1241239,1200483,1252167,1199985,1199515,1199635,1257637)
    union distinct
    select
        gm_uid,
        gm_uid as cck_uid
    from 
        origin_common.cc_ods_fs_wk_cct_layer_info
    where 
        gm_uid in (1199321,1199168,1197475,1202494,1210498,1200648,1204007,1240629,1199210,1209314,1199978,1199305,1199214,1199749,
                   1201288,1205821,1201128,1240633,1199956,1199621,1199365,1204049,1241239,1200483,1252167,1199985,1199515,1199635,1257637)
) n1
left join 
(
    select
        cck_uid,
        count(distinct third_tradeno) as order_num,
        sum(item_price/100) as pay_fee   
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where 
        ds = '${state_date}'
    and 
        product_id = '${product_id}'
    group by 
        cck_uid
) n2
on n1.cck_uid = n2.cck_uid
group by 
    n1.gm_uid
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
应夏毅需求
select
    n1.product_id     as product_id,--商品id
    n1.order_count    as order_count,--订单数
    n1.pay_fee        as pay_fee,--支付金额
    n2.delivery_cnt   as delivery_cnt,
    n3.refund_cnt     as refund_cnt,--30日内发货后8日内又退款的订单数
    n4.eva_cnt        as eva_cnt,
    n4.bad_eva_cnt    as bad_eva_cnt,
    n5.product_order_num_ship_success as product_order_num_ship_success,
    n5.product_ship_duration as product_ship_duration
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
    and 
        product_id in (110019405576,110019405615,110019405627,110019405628)
    group by
        product_id
) as n1
left join
(
    select
        a1.product_id,
        count(a1.third_tradeno) as delivery_cnt
    from
    (
        select
            product_id,
            third_tradeno
        from
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where
            ds >= '${begin_date}'
        and
            ds <= '${end_date}'
        and 
            product_id in (110019405576,110019405615,110019405627,110019405628)
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
    group by a1.product_id
) n2
on n1.product_id = n2.product_id
left join 
(
    select
        a1.product_id,
        count(a1.third_tradeno) as refund_cnt
    from
    (
        select
            product_id,
            third_tradeno
        from
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where
            ds >= '${begin_date}'
        and
            ds <= '${end_date}'
        and 
            product_id in (110019405576,110019405615,110019405627,110019405628)
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
        and 
            status = 1
    ) as a3
    on a1.third_tradeno = a3.order_sn
    group by
       a1.product_id
) n3
on n1.product_id = n3.product_id
left join
(
    select
        a1.product_id,
        count(a2.rate_id) as eva_cnt,--评价数
        sum(if(a2.star_num=1,1,0)) as bad_eva_cnt--差评数
    from
    (
        select
            product_id,
            third_tradeno
        from
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where
            ds >= '${begin_date}'
        and
            ds <= '${end_date}'
        and 
            product_id in (110019405576,110019405615,110019405627,110019405628)
    )a1
    inner join
    (
        select 
            distinct
            order_sn,
            rate_id,
            star_num
        from
            origin_common.cc_rate_star
        where
            ds >= '${begin_date}'
        and
            rate_id != 0
        and 
            order_sn!='170213194354LFo017564wk'
    )a2
    on a1.third_tradeno = a2.order_sn
    group by
        a1.product_id
) n4
on n1.product_id = n4.product_id
left join 
(
    select  
        s1.product_id as product_id,
        count(s1.third_tradeno) as product_order_num_ship_success,--7日商品维度签收订单数
        sum(s2.ship_time)  as product_ship_duration--7日商品维度物流总时长
    from
    (
        select
            product_id,
            third_tradeno
        from
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where
            ds >= '${begin_date}'
        and
            ds <= '${end_date}'
        and 
            product_id in (110019405576,110019405615,110019405627,110019405628)
    ) s1
    inner join
    (
        select
            order_sn,
            (update_time-create_time) as ship_time--物流时长
        from 
            data.cc_cct_product_ship_info
        where 
            ds>='${begin_date}' 
    ) s2
    on s1.third_tradeno = s2.order_sn
    group by 
        s1.product_id
) n5
on n1.product_id = n5.product_id
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
王来平 某段时间某商品 退款 明细
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
    on  
        s1.cck_uid=s2.cck_uid
    where
        s1.ds='${begin_date}'
    and
        s1.product_id ='${product_id}'
    and 
        s2.platform=14
    and 
        s2.ds='${begin_date}'
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
    and 
        status != 0
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
/////////////////////////////////////////////////
刘红艳
select  
    distinct
    p1.product_id,
    p1.app_shop_id,
    p2.sale_cnt,
    p2.commission,
    p2.sale_money,
    p3.product_title,
    p3.product_cname1,
    p3.product_cname2,
    p3.product_cname3,
    p4.danjia,
    p5.refund_cnt,
    p5.refund_success
from
(
    select 
        n2.product_id,
        n1.app_shop_id
    from   
    (
        select 
            item_id,
            app_shop_id
         from  
            origin_common.cc_ods_fs_dwxk_ad_items_daily
         where 
             status =1
         and 
         app_shop_id in 
          (18898,18944,19125,18911,18532,18735,18848,18706,18662,18558,18896,18775,18891,18729,18860,18722,18733,18281,18838,18543,18815,18704,18245,
        18110,18757,18212,17772,18611,18323,18714,18678,18730,18588,18664,18683,18649,18686,18723,18709,18685,18708,18690,18676,18277,18693,18692,
        18608,18674,18537,18673,18684,18398,18740,18849,18799,18893,18633,18472,18447,9565,12016,18575,18442,18514,18640,18625,18636,18635,18590,
        19543,18374,18586,18595,18576,18494,18542,18533,18581,18660,18579,18569,18546,18510,18547,18518,18404,12502,18471,18526,14661,2776,18516,18501,
        18467,18505,18243,18502,18492,18455,18901,19127,18878,19141,18666,14560,17114,16907,10338,17200,18065,15279,18217,17815,16315,13278,18196,17624,
        17648,18226,17684,18224,17839,17697,17809,18007,16439,17218,8036,17831,17582,10668,17698,17699,17819,17820,17686,14359,17461,17888,954,15801,
        18117,18197,17726,17947,9601,17692,18255,17884,18309,8839,11760,18002,17791,14515,11677,17428,1374,17957,9241,5529,17944,16717,17896,12929,8032,
        17500,14975,13352,16819,18241,16814,18304,11184,13896,16530,18285,18142,4128,8106,17705,5138,8089,5666,18091,16510,9151,8481,7479,533,17690,
        9238,16270,16567,6521,18330,1796,17704,17769,16561,4121,17653,17951,17531,10078,15811,10141,18417,11548,17850,12922,13773,9250,10036,17902,
        16798,17851,17649,16743,9772,17982,17597,8270,7992,17405,8622,18317,18238,496,9912,13879,17315,17950,2995,18382,17005,17920,10878,17913,18161,
        18335,16298,9665,17757,18385,18133,11646,14502,13589,266,16785,10639,168,14390,6621,12545,18174,17776,18145,19572,16540,18338,17665,17788,
        16355,12846,16538,9950,12854,17961,7199,16853,16
    )
    inner join
    (
        select 
              item_id,
              app_item_id as product_id
         from  
             origin_common.cc_ods_dwxk_fs_wk_items
    ) n2
    on n1.item_id=n2.item_id
 ) p1
left join
(
    select  
         product_id,
         sum(sale_num) as sale_cnt,
         sum(commission/100) as commission, 
         sum(item_price/100) as sale_money
    from 
         origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where 
         ds >='${begin_date}'
    and  ds <='${end_date}'
    group by product_id
) as p2
on p1.product_id=p2.product_id
left join
(
    select 
         product_id,
         product_title,
         product_cname1,
         product_cname2,
         product_cname3
    from 
        data.cc_dw_fs_products_shops
) as p3
on p1.product_id=p3.product_id
left join
(
    select
        n6.cck_commission,
        n6.cck_rate,
        n6.cck_commission/n6.cck_rate as danjia
    from 
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime as n6
    inner join 
    (
        select
             max(id) as id 
        from  
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    ) as n5
    on n6.id=n5.id 
) p4
on p1.product_id=p4.product_id
left join
(
    select
        n3.product_id ,
        n4.refund_cnt,
        n4.refund_success
    from 
    (
        select 
             third_tradeno,
             max(product_id) as product_id
        from 
             origin_common.cc_ods_dwxk_wk_sales_deal_ctime 
        group by third_tradeno
    ) as n3
    inner join
    (
        select
            order_sn,
            count(id)as refund_cnt,
            sum(success_price) as refund_success
        from   
            origin_common.cc_ods_fs_refund_order
        where 
            from_unixtime(create_time,'yyyyMMdd') >='${begin_date}'
        and 
            from_unixtime(create_time,'yyyyMMdd') <='${end_date}'
        group by order_sn
    ) as n4
    on n3.third_tradeno=n4.order_sn
 ) as p5
 on p1.product_id =p5.product_id;
///////////////////////////////////////////////////////
昨日所有在线商品，店铺维度
select
    t1.app_shop_id as shop_id,
    t3.shop_title as shop_title,
    (case
    when t1.app_shop_id in(19903,20305,20322) then '百诺优品'
    when t1.app_shop_id in(18470) then '冰冰购'
    when t1.app_shop_id in(18532,19141,19268,19347,20471) then '代发'
    when t1.app_shop_id in(18635,18240) then '极便利'
    when t1.app_shop_id in(17791,18731) then '京东'
    when t1.app_shop_id in(18704,18636) then '每日优鲜'
    when t1.app_shop_id in(18730,18723,18542,17636,18482,19089,19667,20203,20314,20343,20065,20548,18871) then '其他'
    when t1.app_shop_id in(18898,18735,18848,18662,18558,18896,18775,18891,18729,18733,18543,18815,18245,17772,18588,18740,18849,18799,18893,19543,18581,18660,18666,19572,19254,19319,18535,18408,18582,18488,18732,19405,19441,18303,18400,19257,19085,17303,18655,18671,19402,19530,19869,18883,20216,18965) then '生鲜'
    when t1.app_shop_id in(18706,18586,18569,18262,19392,18606,15426,18314,19534,2873,19708,2369,9872,19871,19756,19755,19709,16851,20179,17691,20242,456,3559,13930,15907,20513,20652,20653) then '小团子'
    when t1.app_shop_id in(18455) then '严选'
    when t1.app_shop_id in(18838,19239,19505,19504,19486,19470,19404,19527,19521,19525,19542,19613,19609,19599,19580,19664,19701,19699,19683,19682,19678,19765,19742,19722,19753,20016,19906,19907,20063,20064,20178,20168,20236,20237,20202,20188) then '一亩田'
    when t1.app_shop_id in(18335,18164,17801) then '自营'
    when t1.app_shop_id in(19310,18928,19324,18746,19361,19340,19339,19432,19468,19298,19444,19410,19421,19506,18508,19476,19519,19552,18531,19545,19546,18491,19611,18436,19435,18500,19665,19749,19764,19891,18765,19870,19894,20109,20142,20273,20249,20292,19905,20332,18574,20334,20392,20423,20422,20444,20543,20549,20600,20588,20621,20620,20636,20655,20638,20654) then '总监店铺'
    else 'POP' end) as tab,
    count(distinct app_item_id) as product_count
from
(
    select
        item_id,
        ad_id,
        app_shop_id,
        ad_price,
        commission_rate,
        cck_rate,
        cck_price
    from 
        origin_common.cc_ods_fs_dwxk_ad_items_daily
    where 
        start_time < 1536076800 ---昨日结束时间from_unixtime(start_time,'yyyyMMdd hh:mm:ss
    and 
        end_time > 1535990400 ---昨日开始时间
    and 
        audit_status=1 
    and 
        status=1
    and
        app_shop_id not in(18636)
)t1
left join
(
    select
        item_id,
        app_item_id
    from origin_common.cc_ods_dwxk_fs_wk_items
)t2
on t1.item_id=t2.item_id
left join
(
    select
        product_id,
        product_title,
        shop_title
    from 
        data.cc_dw_fs_products_shops
)t3
on t2.app_item_id=t3.product_id
group by t1.app_shop_id,t3.shop_title        
//////////////////////////////////////////////////////////////////////////////////////////
曾荣荣 某段时间 某些店铺 发货后36小时无物流信息的订单明细
有物流但超过36h
select
    n1.shop_id,
    n1.shop_title,
    n1.product_id,
    n1.product_title,
    n2.third_tradeno
from 
(
    select
        shop_id,
        shop_title,
        product_id,
        product_title
    from
        data.cc_dw_fs_products_shops
    where shop_id = 
)   n1
inner join 
(
    select 
        product_id,
        third_tradeno
    from 
         origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where 
         ds='${stat_date}'  
)   n2
on  n1.product_id = n2.product_id
inner join 
(
    select
        order_sn,
        delivery_time
    from 
        origin_common.cc_order_user_delivery_time
    where 
        ds >=20180825
)   n3
on  n2.third_tradeno = n3.order_sn
inner join 
(
    select
        order_sn,
        cast(update_time as int) as update_time
    from 
        data.cc_cct_product_ship_info
    where 
        ds >=20180825
)   n4
on  n3.order_sn = n4.order_sn
where (n4.update_time-n3.delivery_time)>129600
/////////////////////////////////////////////////////////
没有有物流
select
    t1.shop_id,
    t1.shop_title,
    t1.product_id,
    t1.product_title,
    t1.third_tradeno
from
(
    select
        n1.shop_id,
        n1.shop_title,
        n1.product_id,
        n1.product_title,
        n2.third_tradeno
    from 
    (
        select
            shop_id,
            shop_title,
            product_id,
            product_title
        from
            data.cc_dw_fs_products_shops
        where shop_id = 
    )   n1
    inner join 
    (
        select 
            product_id,
            third_tradeno
        from 
             origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where 
             ds='${stat_date}'  
    )   n2
    on  n1.product_id = n2.product_id
    inner join 
    (
        select
            order_sn,
            delivery_time
        from 
            origin_common.cc_order_user_delivery_time
        where 
            ds >=20180825
    )   n3
    on  n2.third_tradeno = n3.order_sn
)   t1
inner join 
(
    select
        order_sn,
        cast(update_time as int) as update_time
    from 
        data.cc_cct_product_ship_info
    where 
        ds >=20180825
)   t2
on  t1.third_tradeno = t2.order_sn
///////////////////////////////////////
首页分时段dau
select 
    ds,
    hour,
    count(distinct cct_uid)
from 
    origin_common.cc_ods_log_gwapp_pv_hourly  
where 
    ds=20180723 
and 
    module='https://app-h5.daweixinke.com/chuchutui/index.html' 
and
    app_partner_id=14
and 
    cct_uid!=0
group by 
    ds,hour
///////////////////////////////////////
首页分时段pv最新
select
    ds,
    hour,
    count(cct_uid)
from 
    origin_common.cc_ods_log_gwapp_pv_hourly
where 
    ds=20180723 
and 
    module like'%chuchutui/index.html%' 
and 
    method_param like '%/js/_ccj_/app/app.js%' 
and
    app_partner_id=14
and
    cct_uid!=0
group by 
    ds,hour
///////////////////////////////////////////////////
东哥需求 所有pop店铺名称，一级类目，商品名称，商品ID，品牌名称 商品 订单数 差评数 退款数
select
    s1.app_shop_id,
    s3.shop_title,
    s3.shop_cname1,
    s2.product_id,
    s3.product_title,
    s4.brand_name,
    s5.sales_num as sales_num,--销量
    s5.pay_fee as pay_fee,
    s5.order_count as order_count,
    s5.eva_cnt as eva_cnt,
    s5.bad_eva_cnt as bad_eva_cnt,--差评数
    s5.refund_num as refund_num
from 
(
    select
        distinct
        item_id,
        app_shop_id,
        ad_brand_id
    from 
        origin_common.cc_ods_dwxk_fs_wk_ad_items
) s1
left join 
(
    select
        item_id,
        app_item_id as product_id
    from 
        origin_common.cc_ods_dwxk_fs_wk_items
) s2 
on s1.item_id = s2.item_id
left join
(
    select
        product_id,
        product_title,
        shop_title,
        shop_cname1
    from 
        data.cc_dw_fs_products_shops
) s3
on s2.product_id = s3.product_id
left join 
(
    select
        distinct
        id,
        brand_name
    from 
        origin_common.cc_brand
) s4
on s1.ad_brand_id = s4.id
left join
(
    select
        n1.product_id as product_id,
        sum(n1.sale_num) as sales_num,--销量
        sum(n1.item_price/100) as pay_fee,
        count(n1.third_tradeno) as order_count,
        count(n2.order_sn) as eva_cnt,
        sum(if(n2.star_num=1,1,0)) as bad_eva_cnt,--差评数
        count(n3.order_sn) as refund_num
    from
    (
        select
            distinct
            product_id,
            item_price,
            sale_num,
            third_tradeno
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where 
            ds>='${begin_date}'
        and 
            ds<='${end_date}'
    ) n1
    left join 
    (
        select
            distinct
            order_sn,--订单号
            star_num--评价分数
        from 
            origin_common.cc_rate_star
        where   
            ds>='${begin_date}' 
        and 
            ds<='${end_date}' 
        and 
            rate_id>0 
        and 
            order_sn!='170213194354LFo017564wk'
    ) n2
    on n1.third_tradeno = n2.order_sn
    left join 
    (
        select
            distinct 
            m1.order_sn as order_sn
        from 
            origin_common.cc_ods_fs_refund_order m1
        inner join
            origin_common.cc_order_user_delivery_time m2
        on 
            m1.order_sn=m2.order_sn 
        where
            from_unixtime(m1.create_time,'yyyyMMdd')>='${begin_date}' 
        and
            from_unixtime(m1.create_time,'yyyyMMdd')<='${end_date}'
        and  
            m1.status=1
    ) n3
    on n1.third_tradeno = n3.order_sn
    group by n1.product_id
) s5
on s2.product_id = s5.product_id
where 
   s1.app_shop_id in () 
///////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////    
汪柯钰 7.13-9.10 178团队每个总经理下面 399创业礼包的销售额 
取n1表对union和 union distinct 进行了判断，发现是一样的！

select
    n1.gm_uid as gm_uid,
    sum(n2.pay_price/100) as pay_price
from 
(
    select
        distinct
        gm_uid,
        cck_uid
    from 
        origin_common.cc_ods_fs_wk_cct_layer_info
    where gm_uid in (1199321,1199168,1197475,1202494,1210498,1200648,1204007,1240629,1199210,1209314,1199978,1199305,1199214,1199749,
                     1201288,1205821,1201128,1240633,1199956,1199621,1199365,1204049,1241239,1200483,1252167,1199985,1199515,1199635,1257637)
    and 
        from_unixtime(create_time,'yyyyMMdd') > 20180803
    union 
    select
        distinct
        gm_uid,
        gm_uid as cck_uid
    from 
        origin_common.cc_ods_fs_wk_cct_layer_info
    where gm_uid in (1199321,1199168,1197475,1202494,1210498,1200648,1204007,1240629,1199210,1209314,1199978,1199305,1199214,1199749,
                     1201288,1205821,1201128,1240633,1199956,1199621,1199365,1204049,1241239,1200483,1252167,1199985,1199515,1199635,1257637)
) n1
left join 
(--对t1表进行条件限制评估，加平台=14前为164042，加之后为68386，再加status=1限制，还剩43624，再加is_del = 0还是43624，说明都删除条件影响不大
    select
        t1.invite_uid as invite_uid,
        t2.pay_price as pay_price
    from
    ( 
        select
            distinct
            cck_uid,
            invite_uid
        from 
            origin_common.cc_ods_fs_wk_cct_layer_info
        where 
            from_unixtime(create_time,'yyyyMMdd') >= 20180713
        and 
            from_unixtime(create_time,'yyyyMMdd') <= 20180910
        and 
            platform = 14 
        and 
            status = 1 
        and 
            is_del = 0
    ) t1
    inner join 
    (--对t2表进行条件限制评估，pay_price=39900，有164042， 加上status = 1仍然是164042，加上pay_status=1还剩158682，说明审核通过不影响，支付状态条件略有影响。
        select
            distinct
            cck_uid,
            pay_price
        from 
            origin_common.cc_ods_dwxk_fs_wk_business_info
        where 
            ds = 20180910
        and 
            from_unixtime(create_time,'yyyyMMdd') >= 20180713
        and 
            from_unixtime(create_time,'yyyyMMdd') <= 20180910
        and 
            pay_price = 39900
        and 
            status = 1
        and 
            pay_status = 1
    ) t2
    on  t1.cck_uid = t2.cck_uid
) n2
on n1.cck_uid = n2.invite_uid
group by
    n1.gm_uid
////////////////////////////////////////////////////////////////////////////////
刘红艳慢就赔偿需求 
select
    h1.ctime as ctime,
    h1.compensate_num as compensate_num,
    h2.pay_time as pay_time,
    h2.use_num as use_num,
    h2.total_fee as total_fee
from
(
    select
        from_unixtime(ctime,'yyyyMMdd') as ctime,
        count(distinct order_sn ) as compensate_num
    from 
        origin_common.cc_ods_fs_timeout_order_coupon_record
    where 
        from_unixtime(ctime,'yyyyMMdd') >= '${compensate_begin}'
    and 
        from_unixtime(ctime,'yyyyMMdd') <= '${compensate_end}'
    and 
        platform=1
    group by
        from_unixtime(ctime,'yyyyMMdd') 
) h1 
left join
(
    select 
        from_unixtime(n1.ctime,'yyyyMMdd') as ctime,
        count(distinct n1.order_sn ) as compensate_num,
        from_unixtime(n2.pay_time,'yyyyMMdd') as pay_time,
        count(distinct n2.order_sn ) as use_num,
        sum(n2.total_fee) as total_fee
    from 
    (
        select
            distinct
            coupon_id,     
            ctime,
            user_id,   
            order_sn
        from 
            origin_common.cc_ods_fs_timeout_order_coupon_record
        where 
            from_unixtime(ctime,'yyyyMMdd') >= '${compensate_begin}'
        and 
            from_unixtime(ctime,'yyyyMMdd') <= '${compensate_end}'
        and 
            platform=1
    ) n1
    left join
    (
        select
            distinct
            t1.user_id,
            t1.order_sn,
            t1.pay_time,
            t1.total_fee
        from 
        (
            select
                user_id,
                order_sn,
                pay_time,
                total_fee
            from 
                origin_common.cc_order_user_pay_time
            where 
                from_unixtime(pay_time,'yyyyMMdd') >= '${compensate_begin}'
            and
                from_unixtime(pay_time,'yyyyMMdd') <= '${statistic_date}'
        ) t1  
        inner join
        (
            select
                distinct
                order_sn
            from
                origin_common.cc_order_coupon_paytime
            where 
                from_unixtime(pay_time,'yyyyMMdd') >= '${compensate_begin}'
            and
                from_unixtime(pay_time,'yyyyMMdd') <= '${statistic_date}'
            and 
                template_id = 15984371
        ) t2
        on t1.order_sn = t2.order_sn
    ) n2
    on
        n1.user_id = n2.user_id
    where (n2.pay_time - n1.ctime )>0
    group by
        from_unixtime(n1.ctime,'yyyyMMdd'),
        from_unixtime(n2.pay_time,'yyyyMMdd')
) h2
on h1.ctime = h2.ctime 
//////////////////////////////////////////////////////////////////////////////
尝试写法
select
    h1.ctime as ctime,
    h1.compensate_num as compensate_num,
    h2.pay_time as pay_time,
    h2.use_num as use_num,
    h2.total_fee as total_fee
from
(
    select
        from_unixtime(ctime,'yyyyMMdd') as ctime,
        count(distinct order_sn ) as compensate_num
    from 
        origin_common.cc_ods_fs_timeout_order_coupon_record
    where 
        from_unixtime(ctime,'yyyyMMdd') >= '${compensate_begin}'
    and 
        from_unixtime(ctime,'yyyyMMdd') <= '${compensate_end}'
    and 
        platform=1
    group by
        from_unixtime(ctime,'yyyyMMdd') 
) h1 
left join
(
    select
        s1.ctime as ctime,
        s1.pay_time as pay_time,
        count(distinct s1.order_sn ) as use_num,
        sum(s1.total_fee) as total_fee
    from
    (
        select
            m1.ctime as ctime,
            m1.pay_time as pay_time,
            m1.order_sn  as order_sn,
            m1.total_fee as total_fee,
            row_number() over (partition by m1.order_sn order by m1.ctime desc) as num
        from
        (
            select 
                from_unixtime(n1.ctime,'yyyyMMdd') as ctime,
                from_unixtime(n2.pay_time,'yyyyMMdd') as pay_time,
                n2.order_sn  as order_sn,
                n2.total_fee as total_fee
            from 
            (
                select
                    distinct
                    coupon_id,     
                    ctime,
                    user_id,   
                    order_sn
                from 
                    origin_common.cc_ods_fs_timeout_order_coupon_record
                where 
                    from_unixtime(ctime,'yyyyMMdd') >= '${compensate_begin}'
                and 
                    from_unixtime(ctime,'yyyyMMdd') <= '${compensate_end}'
                and 
                    platform=1
            ) n1
            left join
            (
                select
                    distinct
                    t1.user_id,
                    t1.order_sn,
                    t1.pay_time,
                    t1.total_fee
                from 
                (
                    select
                        user_id,
                        order_sn,
                        pay_time,
                        total_fee
                    from 
                        origin_common.cc_order_user_pay_time
                    where 
                        from_unixtime(pay_time,'yyyyMMdd') >= '${compensate_begin}'
                    and
                        from_unixtime(pay_time,'yyyyMMdd') <= '${statistic_date}'
                ) t1  
                inner join
                (
                    select
                        distinct
                        order_sn
                    from
                        origin_common.cc_order_coupon_paytime
                    where 
                        from_unixtime(pay_time,'yyyyMMdd') >= '${compensate_begin}'
                    and
                        from_unixtime(pay_time,'yyyyMMdd') <= '${statistic_date}'
                    and 
                        template_id = 15984371
                ) t2
                on t1.order_sn = t2.order_sn
            ) n2
            on
                n1.user_id = n2.user_id
            where (n2.pay_time - n1.ctime )>0
        ) m1
    ) s1 
    where 
        s1.num = 1 
    group by
        s1.ctime,
        s1.pay_time
) h2
on h1.ctime = h2.ctime 
////////////////////////////////////////////////////////////////////////////////////////
-- or n2.pay_time is null
--或者last_value(m1.ctime) over (partition by m1.order_sn order by m1.ctime rows between unbounded preceding and unbounded following) as ano_ctime
--或者s1.ano_ctime = s1.ctime
////////////////////////////////////////////////////////////////////////////////////////
刘红艳慢就赔偿需求
select 
    from_unixtime(n1.ctime,'yyyyMMdd') as ctime,
    n1.order_sn  as compensate_order_sn,
    from_unixtime(n2.pay_time,'yyyyMMdd') as pay_time,
    n2.order_sn  as pay_order_sn,
    n2.total_fee as pay_fee,
    n4.cck_uid as cck_uid,
    n5.real_name as cck_name,
    n4.leader_uid as leader_uid,
    n6.real_name as leader_name,
    n4.gm_uid as gm_uid,
    n7.real_name as gm_name
from 
(
    select
        distinct
        coupon_id,     
        ctime,
        user_id,   
        order_sn
    from 
        origin_common.cc_ods_fs_timeout_order_coupon_record
    where 
        from_unixtime(ctime,'yyyyMMdd') >= '${compensate_begin}'
    and 
        from_unixtime(ctime,'yyyyMMdd') <= '${compensate_end}'
    and 
        platform=1
) n1
inner join
(
    select
        distinct
        t1.user_id,
        t1.order_sn,
        t1.pay_time,
        t1.total_fee
    from 
    (
        select
            user_id,
            order_sn,
            pay_time,
            total_fee
        from 
            origin_common.cc_order_user_pay_time
        where 
            from_unixtime(pay_time,'yyyyMMdd') >= '${compensate_begin}'
        and
            from_unixtime(pay_time,'yyyyMMdd') <= '${statistic_date}'
    ) t1  
    inner join
    (
        select
            distinct
            order_sn
        from
            origin_common.cc_order_coupon_paytime
        where 
            from_unixtime(pay_time,'yyyyMMdd') >= '${compensate_begin}'
        and
            from_unixtime(pay_time,'yyyyMMdd') <= '${statistic_date}'
        and 
            template_id = 15984371
    ) t2
    on t1.order_sn = t2.order_sn
) n2
on
    n1.user_id = n2.user_id
left join
(
    select
        distinct
        cct_uid,
        cck_uid
    from 
        origin_common.cc_ods_fs_tui_relation
) n3
on n1.user_id = n4.cct_uid
left join
(
    select
        distinct 
        cck_uid,
        leader_uid,
        gm_uid
    from 
        origin_common.cc_ods_fs_wk_cct_layer_info
    where 
        platform = 14  and leader_uid != 0
    union
    select
        distinct
        leader_uid as cck_uid,
        leader_uid as leader_uid,
        gm_uid as gm_uid,
    from 
        origin_common.cc_ods_fs_wk_cct_layer_info
    where 
        platform = 14
    union
    select
        distinct
        gm_uid as cck_uid
        gm_uid as leader_uid
        gm_uid as gm_uid
    from
        origin_common.cc_ods_fs_wk_cct_layer_info
    where 
        platform = 14 
) n4
on n3.cck_uid = n4.cck_uid
left join 
(
    select
        distinct
        cck_uid,
        real_name
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = '${compensate_end}'       
) n5
on n4.cck_uid = n5.cck_uid
left join 
(
    select        
        distinct
        cck_uid,
        pay_price
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = '${compensate_end}'       
) n6
on n4.leader_uid = n6.cck_uid
left join 
(
    select
        distinct
        cck_uid,
        pay_price
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = '${compensate_end}'       
) n7
on n4.gm_uid = n7.cck_uid
/////////////////////////////////////////////////////////////////
东哥 近一个月订单的发货地址
select
    t3.province_name,
    count(t1.order_sn) as order_num
from
(
    select
        distinct m1.third_tradeno as order_sn--订单号
    from 
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime m1
    inner join
        origin_common.cc_ods_dwxk_fs_wk_cck_user m2
    on 
        m1.cck_uid=m2.cck_uid
    where 
        m1.ds>='${begin_date}' 
    and 
        m1.ds<='${end_date}' 
    and 
        m2.platform =14 
    and 
        m2.ds='${end_date}'
) t1
left join
(
    select
        order_sn,
        area_id
    from
        origin_common.cc_order_user_pay_time
    where
        ds >= '${begin_date}' 
    and
        ds <= '${end_date}' 
) t2
on t1.order_sn = t2.order_sn
left join 
(
    select
        area_id,
        province_name
    from 
        origin_common.cc_area_city_province 
) t3
on t2.area_id = t3.area_id
group by
    t3.province_name
/////////////////////////////////////////////////////////////////
魏薇需求
商品ID：1100185321892 
订单时间：6.29
要求字段：用户名、订单数、销量 总的
select
    n1.cck_uid,
    n2.real_name,
    n1.order_count      as order_count, --订单数
    n1.sales_num           as sales_num,  --销量
    n1.item_price          as item_price,  --支付金额
    n1.cck_commission as cck_commission --佣金
from
(
    select
        cck_uid,
        count(third_tradeno)    as order_count, --订单数
        sum(sale_num)           as sales_num,  --销量
        sum(item_price/100)     as item_price,  --支付金额
        sum(cck_commission/100) as cck_commission --佣金
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where
        ds = '${stat_date}'
    and
        product_id = '${product_id}'
    group by cck_uid
) n1
left join 
(
    select        
        distinct
        cck_uid,
        real_name
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = 20180912
) n2
on n1.cck_uid = n2.cck_uid
////////////////////////////////////////////////////////////////////////////////////////////
魏薇需求
商品ID：1100185321892 
订单时间：6.29
要求字段：用户名、订单数、销量 推广订单的
select
    n1.cck_uid as cck_uid,
    n2.real_name as real_name,
    n1.order_count      as order_count, --订单数
    n1.sales_num           as sales_num,  --销量
    n1.item_price          as item_price,  --支付金额
    n1.cck_commission as cck_commission --佣金
from
(
    select
        a1.cck_uid as cck_uid,
        count(a1.third_tradeno)    as order_count, --订单数
        sum(a1.sale_num)           as sales_num,  --销量
        sum(a1.item_price/100)     as item_price,  --支付金额
        sum(a1.cck_commission/100) as cck_commission --佣金
    from
    (
        select
            cck_uid,
            third_tradeno,
            sale_num,--销量
            item_price,--支付金额
            cck_commission --佣金
        from
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where
            ds = '${stat_date}'
        and
            product_id = '${product_id}'
    ) as a1
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
    ) as a2
    on 
        a1.third_tradeno = a2.order_sn
    where
        a2.order_sn is null
    group by 
        a1.cck_uid
) n1
left join 
(
    select        
        distinct
        cck_uid,
        real_name
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = 20180912
) n2
on n1.cck_uid = n2.cck_uid
///////////////////////////////////////////////////////////////
SELECT 
* 
FROM 
    `cc_product_region` 
WHERE 
shop_id in (18532,18164,17801,19268,19347)
////////////////////////////////////////////////////////////////////////////////////////////
廖宁需求 某日 某商品 以楚客为维度 销售情况，并给出楚客所属是总经理 总监信息。
注意 因为要构造总经理自己做楚客时他的总经理是自己，所以带来两个问题：1.返回总监和总经理时要分开返回，即这两个字段不能放在同一张表里，否则造成总经理大量重复。
2.总经理自己做楚客的销售数据被重复记录了
select
    n1.cck_uid        as cck_uid,
    n4.real_name      as cck_name,
    n4.phone          as cck_phone,
    n2.leader_uid     as leader_uid,
    n5.real_name      as leader_name,
    n5.phone          as leader_phone,
    n3.gm_uid         as gm_uid,
    n6.real_name      as gm_name,
    n6.phone          as gm_phone,
    n1.order_count    as order_count, --订单数
    n1.sales_num      as sales_num,  --销量
    n1.item_price     as item_price,  --支付金额
    n1.cck_commission as cck_commission --佣金
from
(
    select
        cck_uid as cck_uid,
        count(third_tradeno)    as order_count, --订单数
        sum(sale_num)           as sales_num,  --销量
        sum(item_price/100)     as item_price,  --支付金额
        sum(cck_commission/100) as cck_commission --佣金
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where
        ds = '${stat_date}'
    and
        product_id = '${product_id}'
    group by 
        cck_uid
) n1 
left join 
(   
    select
        distinct
        cck_uid,
        leader_uid
    from
        origin_common.cc_ods_fs_wk_cct_layer_info
) n2
on n1.cck_uid = n2.cck_uid
left join 
(   
    select
        distinct
        cck_uid,
        gm_uid
    from
        origin_common.cc_ods_fs_wk_cct_layer_info
    union distinct
    select
        distinct
        gm_uid as cck_uid,
        gm_uid
    from
        origin_common.cc_ods_fs_wk_cct_layer_info
) n3
on n1.cck_uid = n3.cck_uid
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
        ds = 20180916
) n4 
on n1.cck_uid = n4.cck_uid
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
        ds = 20180916
) n5
on n2.leader_uid = n5.cck_uid
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
        ds = 20180916
) n6
on n3.gm_uid = n6.cck_uid
//////////////////////////////////////////////////////////////////
魏薇需求 总经理信息 8.1-8.30日 内拉新人数，总人数
select
    t1.gm_uid as gm_uid,
    t3.real_name as real_name,
    t3.phone as phone,
    t2.team_num_now as team_num_now,
    (t2.team_num_now - t1.team_num_before) as new_vip
from
(
    select
        n1.gm_uid as gm_uid,
        (n1.cck_num+n2.leader_num) as team_num_before
    from
    (
        select
            gm_uid,
            count(distinct cck_uid) as cck_num
        from
            origin_common.cc_ods_fs_wk_cct_layer_info
        where 
            type = 0
        and
            platform = 14
        and 
            from_unixtime(create_time,'yyyyMMdd') <='${stat_date_1}'
        group by 
            gm_uid
    ) n1
    left join 
    (
        select
            gm_uid,
            count(distinct cck_uid) as leader_num
        from
            origin_common.cc_ods_fs_wk_cct_layer_info
        where 
            type = 1
        and
            platform = 14
        and 
            from_unixtime(leader_ctime,'yyyyMMdd') <='${stat_date_1}'
        group by 
            gm_uid
    ) n2
    on n1.gm_uid = n2.gm_uid
) t1
left join
(
    select
        n1.gm_uid as gm_uid,
        (n1.cck_num+n2.leader_num) as team_num_now
    from
    (
        select
            gm_uid,
            count(distinct cck_uid) as cck_num
        from
            origin_common.cc_ods_fs_wk_cct_layer_info
        where 
            type = 0
        and
            platform = 14
        and 
            from_unixtime(create_time,'yyyyMMdd') <='${stat_date_2}'
        group by 
            gm_uid
    ) n1
    left join 
    (
        select
            gm_uid,
            count(distinct cck_uid) as leader_num
        from
            origin_common.cc_ods_fs_wk_cct_layer_info
        where 
            type = 1
        and
            platform = 14
        and 
            from_unixtime(leader_ctime,'yyyyMMdd') <='${stat_date_2}'
        group by 
            gm_uid
    ) n2
    on n1.gm_uid = n2.gm_uid
) t2
on t1.gm_uid  = t2.gm_uid
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
        ds = 20180919
) t3
on t1.gm_uid = t3.cck_uid

/////////////////////////////////////////////
周鹿 9月16号,1100185322925,订单编号，每单件数，金额 
select
    product_id,
    cck_uid,
    third_tradeno    as order_count, --订单数
    sale_num          as sales_num,  --销量
    (item_price/100)     as item_price  --支付金额
from
    origin_common.cc_ods_dwxk_wk_sales_deal_ctime
where
    ds = '${stat_date}'
and
    product_id = '${product_id}'
/////////////////////////////////////////////
刘红艳
select  
    distinct 
    s0.*,
    s2.platform
from  
(
    select 
    *
    from 
        cc_ods_fs_task
    where 
        from_unixtime(created_on,'yyyyMMdd')=20180924
) s0
 left join
(
    select 
        distinct
        third_tradeno,
        cck_uid
    from  
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime 
)s1
on 
    s0.order_id=s1.third_tradeno
left  join
(
 select 
  *
 from 
    origin_common.cc_ods_dwxk_fs_wk_cck_user
 where 
 ds=20180924
 ) s2
on 
    s1.cck_uid=s2.cck_uid
where 
    s2.platform =14;
////////////////////////////////////////////////////////////
张梨娜 某日 某商品 自买 销量top50 的销售及楚客信息数据
select
    t1.*
from
(
    select
        n1.cck_uid,
        n2.real_name,
        n2.phone,
        sum(n1.sale_num) as sales_num,
        count(distinct n1.third_tradeno) as order_count,
        sum(n1.item_price/100) as pay_fee,
        sum(n1.cck_commission/100) as cck_commission
    from 
    (
        select
            a0.cck_uid,
            a0.third_tradeno,
            a0.sale_num,
            a0.item_price,
            a0.cck_commission
        from
        (
            select
                cck_uid,
                third_tradeno,
                sale_num,
                item_price,
                cck_commission
            from 
                origin_common.cc_ods_dwxk_wk_sales_deal_hourly
            where 
                ds = '${stat_date}' 
            and 
                product_id = '${product_id}'
        ) a0
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
    ) n1
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
            ds = 20180829
    ) n2
    on n1.cck_uid = n2.cck_uid
    group by n1.cck_uid,n2.real_name,n2.phone
    order by pay_fee DESC
) t1
limit 30
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
张梨娜 某日 某商品 自买订单发货地址 及楚客信息数据
select
    t1.cck_uid,
    t5.real_name,
    t5.phone,
    t1.order_sn,
    t1.sale_num,
    (t1.item_price/100) as pay_fee,
    t3.delivery_address
from 
(
    select
        cck_uid,
        third_tradeno as order_sn,
        sale_num,
        item_price,
        cck_commission
    from 
        origin_common.cc_ods_dwxk_wk_sales_deal_hourly
    where 
        ds = '${stat_date}' 
    and 
        product_id = '${product_id}'
) t1
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
) as t2
on t1.order_sn = t2.order_sn
left join 
(
    select
        order_sn,
        delivery_address,
        area_id
    from
        origin_common.cc_order_user_pay_time
) t3
on t1.order_sn = t3.order_sn
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
        ds = 20180925
) t5
on t1.cck_uid = t5.cck_uid
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
获取首页十大金刚标示
select
    case
      when s.cck_vip_status = 0 and s.cck_vip_level = 0 then 0
      when s.cck_vip_status = 0 and s.cck_vip_level = 1 then 2
      else 1
    end as source,
    s.ad_id,
    s.pc,
    s.uc
from
(
    select
        m1.cck_vip_status,
        m1.cck_vip_level,
        m1.ad_id,
        count(*) pc,
        count(distinct m1.user_id) as uc
    from
    (
        select
            t1.user_id,
            t1.ad_id,
            t2.cck_vip_status,
            if(t2.cck_vip_status = 1, -1, t2.cck_vip_level) as cck_vip_level
        from
        (
            select 
                user_id,
                (case when ad_id =390521  then 257919
                      when ad_id =375203  then 260882
                      when ad_id =375208  then 257950
                      when ad_id =390522  then 257943
                      when ad_id =394367  then 394366
                      when ad_id =393512  then 393514
                else ad_id end 
                ) as ad_id
            from 
                origin_common.cc_ods_log_cctapp_click_hourly 
            where 
               ds = '{bizdate}' 
               and 
               module = 'cct-home-king' 
               and 
               zone = 'bannerList' 
               and 
               source in ('cct', 'cctui') 
        ) t1
        join
        (
            select 
                cct_uid, 
                cck_vip_status, 
                cck_vip_level 
            from origin_common.cc_ods_fs_tui_relation 
        ) t2
        on
            t1.user_id = t2.cct_uid
    ) m1
    group by
        m1.cck_vip_status,
        m1.cck_vip_level,
        m1.ad_id 
) s
///////////////////////////////////////////////////////////////////////////////////////////////////////////
394366,微信红包,weixinhongbao
257914,话费充值,huafeichongzhi
257943,网易严选,wangyiyanxuan
257945,高佣精选,gaoyongjingxuan
257947,巨划算,juhuasuan
257950,海外购,haiwaigou
260882,楚楚助农,chuchuzhunong
361701,游戏推广,youxituiguang
393514,楚楚自营,chuchuziying
257919,京东自营,jingdongziying
393145,新手必推,xinshoubitui
257955,新人必抢,xinrenbiqiang
257948,敬请期待,jingqingqidai
298671,保险专区,baoxianzhuanqu
390912,同程旅游,tongchenglvyouvip
390814,同程旅游,tongchenglvyou
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
select
    ad_id,
    (
    case 
        when ad_id = 394366 then '楚币任务_vip用户'
        when ad_id = 394367 then '楚币任务_非vip用户'
        when ad_id = 393145 then '新人专区'
        when ad_id = 406875 then '种草专区'
        when ad_id = 260882 then '楚币助农_vip用户'
        when ad_id = 375203 then '楚币助农_非vip用户'
        when ad_id = 257945 then '高佣精选_vip用户'
        when ad_id = 393514 then '楚楚优选_vip用户'
    else '楚楚优选_非vip用户' end 
    ) as jingang,
    count(*) pc,
    count(distinct user_id) as uc
from
    origin_common.cc_ods_log_cctapp_click_hourly 
where 
   ds >= '${begin_date}' 
   and 
   ds <= '${end_date}' 
   and 
   module = 'cct-home-king' 
   and 
   zone = 'bannerList' 
   and 
   ad_id in (394366,394367,393145,406875,260882,375203,257945,393514,393512) 
   and 
   source in ('cct', 'cctui') 
group by 
    ad_id
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SELECT 
    a4.module, 
    a4.zone,
    count(user_id) as pv,
    count(distinct user_id) as uv 
FROM
(
    SELECT 
        module,
        zone,
        user_id
    FROM 
        origin_common.cc_ods_log_cctapp_click_hourly
    WHERE 
        ds='${bizdate}'
        AND module IN ('detail','detail_app','detail_material','new')
        AND zone in ('enter','spread','promotion','wechatPro','circleFriendPro','pQrCode','wechatPQC','circleFriendPQC','show','new-share')
        AND source in ('cct','cctui')
        AND ad_type ='special-activity'
        AND ad_material_id=50487
) a4
GROUP BY a4.module,a4.zone
////////////////////////////////////////////////////////
select
    a.cck_uid,
    min(a.create_time) as create_time
from 
(
    select
        cck_uid,
        sale_num,
        create_time,
        sum(sale_num) over(partition by cck_uid order by create_time) as sales_num
    from 
       origin_common.cc_ods_dwxk_wk_sales_deal_hourly 
    where 
        ds= 20180927
) a 
where 
    a.sales_num > 10
group by 
    a.cck_uid
/////////////////////////////////////
val tmp = List((a,x1,t1),······)
val result = tmp.reduceLeft(((val1,val2,val3), (val4,val5,val6)) => if (val2 + val5 >= maxValue) {(val1, val2, val3)} else {(val4, val2 + val5, val6)})
result._3 //这个就是结果
//////////////////////////////
select
    a.base_uid as base_uid,
    min(a.c_time) as c_time
from
(
    select
        base_uid,
        total_fee,
        c_time,
        sum(total_fee) over(partition by base_uid order by c_time) as acc_fee
    from
        tmp.retain_user_20180822_wb
) as a
where 
    a.acc_fee > 100
group by a.base_uid
///////////////////////////////////////////////////////////////////////
汪柯钰需求 1199321 团队 9月商品加礼包总销售额
select
    t1.cck_uid,
    t2.real_name,
    t2.phone,
    coalesce(t3.pay_fee,0) as product_pay_fee,
    coalesce(t4.pay_price,0) as gift_pay_fee
from
(
    select
        distinct
        cck_uid
    from 
        cc_ods_fs_wk_cct_layer_info
    where 
        gm_uid = 1199321
    union
    select
        distinct
        gm_uid as cck_uid
    from 
        cc_ods_fs_wk_cct_layer_info
    where 
        gm_uid = 1199321
) t1
left join
(
    select
        distinct
        cck_uid,
        real_name,
        phone
    from 
        cc_ods_dwxk_fs_wk_business_info
    where 
        ds=20180927
) t2
on t1.cck_uid=t2.cck_uid
left join 
(
    select
        cck_uid,
        sum(item_price/100) as pay_fee
    from 
        cc_ods_dwxk_wk_sales_deal_ctime
    where 
        ds >= '${begin_date}' 
    and 
        ds <= '${end_date}' 
    group by 
        cck_uid
) t3
on t1.cck_uid=t3.cck_uid
left join
(--对t1表进行条件限制评估，加平台=14前为164042，加之后为68386，再加status=1限制，还剩43624，再加is_del = 0还是43624，说明都删除条件影响不大
    select
        t1.invite_uid as invite_uid,
        sum(t2.pay_price/100) as pay_price
    from
    ( 
        select
            distinct
            cck_uid,
            invite_uid
        from 
            origin_common.cc_ods_fs_wk_cct_layer_info
        where 
            from_unixtime(create_time,'yyyyMMdd') >= '${begin_date}' 
        and 
            from_unixtime(create_time,'yyyyMMdd') <= '${end_date}' 
        and 
            platform = 14 
        and 
            status = 1 
        and 
            is_del = 0
    ) t1
    inner join 
    (--对t2表进行条件限制评估，pay_price=39900，有164042， 加上status = 1仍然是164042，加上pay_status=1还剩158682，说明审核通过不影响，支付状态条件略有影响。
        select
            distinct
            cck_uid,
            pay_price
        from 
            origin_common.cc_ods_dwxk_fs_wk_business_info
        where 
            ds = 20180927
        and 
            from_unixtime(create_time,'yyyyMMdd') >= '${begin_date}' 
        and 
            from_unixtime(create_time,'yyyyMMdd') <= '${end_date}' 
        and 
            pay_price in (39900,49900)
        and 
            status = 1
        and 
            pay_status = 1
    ) t2
    on  t1.cck_uid = t2.cck_uid
    group by 
        t1.invite_uid
) t4 
on t1.cck_uid=t4.invite_uid
///////////////////////////////////////////////////////////////////////
汪柯钰需求 所有总监9.15 到现在 个人销售额  不含礼包
select
    t1.leader_uid as leader_uid,
    t2.real_name as real_name,
    t2.phone as phone,
    coalesce(t3.pay_fee,0) as product_pay_fee
from
(
    select
        distinct
        cck_uid as leader_uid
    from 
        cc_ods_fs_wk_cct_layer_info
    where 
        type = 1
) t1
left join
(
    select
        distinct
        cck_uid,
        real_name,
        phone
    from 
        cc_ods_dwxk_fs_wk_business_info
    where 
        ds=20181008
) t2
on t1.leader_uid = t2.cck_uid
left join 
(
    select
        cck_uid,
        sum(item_price/100) as pay_fee
    from 
        cc_ods_dwxk_wk_sales_deal_ctime
    where 
        ds >= '${begin_date}' 
    and 
        ds <= '${end_date}' 
    group by 
        cck_uid
) t3
on t1.leader_uid = t3.cck_uid
/////////////////////////////////////////////////////////////////////
郑羽佳 9.27 御泥坊 销量最快到达10个的 前200人 的订单号和订单时间
select
    n1.cck_uid,
    n1.order_sn,
    n1.create_time
from
(
    select
        t1.cck_uid as cck_uid,
        min(t1.third_tradeno) as order_sn,
        min(t1.create_time) as create_time
    from
    (
        select
            cck_uid,
            sale_num,
            third_tradeno,
            create_time,
            sum(sale_num) over (partition by cck_uid order by create_time) as sales_num
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where 
            ds = 20180927
        and 
            product_id = 1100185323136
    ) t1
    where 
        t1.sales_num >=10
    group by 
        t1.cck_uid
) n1
order by n1.create_time
limit 200
///////////////////////////////////////////////////////////////////////
郑羽佳 9.27 御泥坊 翔哥那200人  的订单数和 退款单数
select
    n1.cck_uid as cck_uid,
    n3.real_name as real_name,
    n3.phone as phone,
    n1.total_order_num as total_order_num,
    n1.total_sales_num as total_sales_num,
    n1.refund_order_num as refund_order_num,
    n2.real_order_num as real_order_num,
    n2.real_sales_num as real_sales_num  
from
(
    select
        t1.cck_uid as cck_uid,
        count(t1.third_tradeno) as total_order_num,
        sum(t1.sale_num) as total_sales_num,
        count(t2.order_sn) as refund_order_num
    from
    (
        select
            cck_uid,
            sale_num,
            third_tradeno 
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where 
            ds = 20180927
        and 
            product_id = 1100185323136
        and
            cck_uid in (715679,1157088,1199321,965507,1011353,483116,1211181,1374252,1304258,1058735,1118151,1240633,1380217,1199628,481585,949350,1252853,1280421,1303591,1206956,892663,1443956,1052413,997423,55,980086,1357272,1391021,986305,1211685,1227974,1024583,1207918,1018964,1144739,1082543,1200788,1326924,1250957,1312599,291991,1240632,580551,1129525,746661,1207930,555103,932562,1000938,1055840,1152340,1167078,1146692,752419,1244158,1275154,404900,389824,767822,769856,1035901,1251644,407153,932319,1033024,1002663,1049387,1467874,1467517,1318646,1048208,1052318,1241065,1242720,1287805,1438581,1297406,1195259,1105173,1148018,1256190,1431646,1209737,1235214,1350622,800308,1244720,1318759,553889,1242118,1423282,1378646,1090427,1243958,988813,1200449,1242641,1278519,1015432,1251823,1287483,1433507,1241236,366935,412696,1229685,1452064,1202494,1457157,1054511,1002677,1208915,1368227,1200495,1350631,1268505,581426,1303486,1276826,1199210,1477388,1344243,1443348,1252631,897734,1208952,1287638,1444722,1105402,1138946,1084054,1209012,848254,774362,1419802,532027,565618,1206880,877172,1242493,1081656,942729,828712,1312980,1267529,1241163,1021713,1240642,1202519,1199349,625045,1199603,856594,500301,1379881,455813,1153157,897783,718869,1010473,992083,1412927,1296618,1050573,1456830,747329,1288974,782570,1118777,897853,734025,331462,1246955,1318537,1138476,992729,1386599,1122085,934325,1134187,1060235,1248549,1199789,1140644,1037096,841241,1432528,626959,1196634,1307523,752522,783177,703378,1425678,1280657,1384305,582375,1375784,790020,990563)
    ) t1
    left join 
    (
        select
            distinct
            order_sn
        from
            origin_common.cc_ods_fs_refund_order
        where 
            from_unixtime(create_time,'yyyyMMdd') >= 20180927
        and
            status = 1
    ) t2
    on t1.third_tradeno = t2.order_sn
    group by 
        t1.cck_uid
) n1
left join 
(
    select
        t1.cck_uid as cck_uid,
        count(t1.third_tradeno) as real_order_num,
        sum(t1.sale_num) as real_sales_num
    from
    (
        select
            cck_uid,
            sale_num,
            third_tradeno 
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where 
            ds = 20180927
        and 
            product_id = 1100185323136
        and
            cck_uid in (715679,1157088,1199321,965507,1011353,483116,1211181,1374252,1304258,1058735,1118151,1240633,1380217,1199628,481585,949350,1252853,1280421,1303591,1206956,892663,1443956,1052413,997423,55,980086,1357272,1391021,986305,1211685,1227974,1024583,1207918,1018964,1144739,1082543,1200788,1326924,1250957,1312599,291991,1240632,580551,1129525,746661,1207930,555103,932562,1000938,1055840,1152340,1167078,1146692,752419,1244158,1275154,404900,389824,767822,769856,1035901,1251644,407153,932319,1033024,1002663,1049387,1467874,1467517,1318646,1048208,1052318,1241065,1242720,1287805,1438581,1297406,1195259,1105173,1148018,1256190,1431646,1209737,1235214,1350622,800308,1244720,1318759,553889,1242118,1423282,1378646,1090427,1243958,988813,1200449,1242641,1278519,1015432,1251823,1287483,1433507,1241236,366935,412696,1229685,1452064,1202494,1457157,1054511,1002677,1208915,1368227,1200495,1350631,1268505,581426,1303486,1276826,1199210,1477388,1344243,1443348,1252631,897734,1208952,1287638,1444722,1105402,1138946,1084054,1209012,848254,774362,1419802,532027,565618,1206880,877172,1242493,1081656,942729,828712,1312980,1267529,1241163,1021713,1240642,1202519,1199349,625045,1199603,856594,500301,1379881,455813,1153157,897783,718869,1010473,992083,1412927,1296618,1050573,1456830,747329,1288974,782570,1118777,897853,734025,331462,1246955,1318537,1138476,992729,1386599,1122085,934325,1134187,1060235,1248549,1199789,1140644,1037096,841241,1432528,626959,1196634,1307523,752522,783177,703378,1425678,1280657,1384305,582375,1375784,790020,990563)
    ) t1
    left join 
    (
        select
            distinct
            order_sn
        from
            origin_common.cc_ods_fs_refund_order
        where 
            from_unixtime(create_time,'yyyyMMdd') >= 20180927
        and
            status = 1
    ) t2
    on t1.third_tradeno = t2.order_sn
    where 
        t2.order_sn is null 
    group by 
        t1.cck_uid
) n2
on n1.cck_uid = n2.cck_uid
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
        ds = 20181007
) n3
on n1.cck_uid = n3.cck_uid
///////////////////////////////////////////////////////////////////////


///////////////////////////////////////////////////////////////////////
陈威
select
    m1.ds as ds,
    sum(case when m1.cname = '自营--家居百货' then m1.pay_fee end) as '自营--家居百货',
    sum(case when m1.cname = '自营--家用电器' then m1.pay_fee end) as '自营--家用电器',
    sum(case when m1.cname = '自营--美妆个护' then m1.pay_fee end) as '自营--美妆个护',
    sum(case when m1.cname = '自营--母婴' then m1.pay_fee end) as '自营--母婴',
    sum(case when m1.cname = '自营--手机数码' then m1.pay_fee end) as '自营--手机数码',
    sum(case when m1.cname = '自营--零食/坚果/特产' then m1.pay_fee end) as '自营--零食/坚果/特产',
    sum(case when m1.cname = '自营--传统滋补营养品' then m1.pay_fee end) as '自营--传统滋补营养品',
    sum(case when m1.cname = '自营合计' then m1.pay_fee end) as '自营合计',
    sum(case when m1.cname = 'POP--家居百货' then m1.pay_fee end) as 'POP--家居百货',
    sum(case when m1.cname = 'POP--家用电器' then m1.pay_fee end) as 'POP--家用电器',
    sum(case when m1.cname = 'POP--美妆个护' then m1.pay_fee end) as 'POP--美妆个护',
    sum(case when m1.cname = 'POP--母婴' then m1.pay_fee end) as 'POP--母婴',
    sum(case when m1.cname = 'POP--手机数码' then m1.pay_fee end) as 'POP--手机数码',
    sum(case when m1.cname = 'POP--零食/坚果/特产' then m1.pay_fee end) as 'POP--零食/坚果/特产',
    sum(case when m1.cname = 'POP--传统滋补营养品' then m1.pay_fee end) as 'POP--传统滋补营养品',
    sum(case when m1.cname = 'POP合计' then m1.pay_fee end) as 'POP合计',
    sum(case when m1.cname = '杂百总计' then m1.pay_fee end) as '杂百总计',
    sum(case when m1.cname = '食品总计' then m1.pay_fee end) as '食品总计',
    sum(case when m1.cname = '服饰' then m1.pay_fee end) as '服饰',
    sum(case when m1.cname = '生鲜' then m1.pay_fee end) as '生鲜',
    sum(case when m1.cname = '公司总计' then m1.pay_fee end) as '公司总计'
from
(
    select
        t1.ds as ds,
        t1.cname as cname, 
        sum(t1.pay_fee) as pay_fee --自营各一级类目数据
    from
    (
        select
            m1.ds as ds,
            concat('自营--',m1.cname) as cname,
            m1.pay_fee as pay_fee
        from
        (
            select
                n1.ds as ds,
                (
                case
                    when n2.product_cname1 = '母婴' then '母婴'
                    when n2.product_cname1 = '手机数码' then '手机数码'
                    when n2.product_cname1 = '家用电器' then '家用电器'
                    when n2.product_cname1 = '家居百货' then '家居百货'
                    when n2.product_cname1 = '美妆个护' and n2.product_cname3 = '卫生巾/护垫/成人尿裤' then '家居百货'
                    when n2.product_cname1 = '食品' and n2.product_cname2 in ('零食/坚果/特产','酒水/茶/冲饮') then '零食/坚果/特产'
                    when n2.product_cname1 = '食品' and n2.product_cname2 in ('传统滋补营养品','粮油米面/南北干货/调味品','保健食品/膳食营养补充食品') then '传统滋补营养品'
                else '美妆个护' end
                ) as cname, 
                n1.pay_fee as pay_fee 
            from
            (
                select
                    s1.ds as ds,
                    s1.product_id as product_id,
                    sum(s1.item_price/100) as pay_fee
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
                group by 
                    s1.ds,s1.product_id
            ) n1
            inner join
            (
                select
                    product_id, 
                    product_cname1, 
                    product_cname2, 
                    product_cname3
                from 
                    data.cc_dw_fs_products_shops
                where 
                    shop_id in (17801,18164,18335,18532,19141,19268,19347,19405)
                and 
                    product_cname1 in ('食品','母婴','手机数码','家用电器','家居百货','美妆个护')
                and
                    product_cname2 not in ('水产肉类/新鲜蔬果/熟食')
            ) n2
            on n1.product_id = n2.product_id
        ) m1
    ) t1 
    group by 
        t1.ds,t1.cname
    union all
    select
        t2.ds as ds,
        t2.cname as cname, 
        sum(t2.pay_fee) as pay_fee --自营合计
    from
    (
        select
            n1.ds as ds,
            '自营合计' as cname, 
            n1.pay_fee as pay_fee 
        from
        (
            select
                s1.ds as ds,
                s1.product_id as product_id,
                sum(s1.item_price/100) as pay_fee
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
            group by 
                s1.ds,s1.product_id
        ) n1
        inner join
        (
            select
                product_id
            from 
                data.cc_dw_fs_products_shops
            where 
                shop_id in ('自营的店铺id')
            and 
                product_cname1 in ('食品','母婴','手机数码','家用电器','家居百货','美妆个护')
            and
                product_cname2 not in ('水产肉类/新鲜蔬果/熟食')
        ) n2
        on n1.product_id = n2.product_id
    ) t2
    group by 
        t2.ds,t2.cname
    union all
    select
        t3.ds as ds,
        t3.cname as cname, 
        sum(t3.pay_fee) as pay_fee --POP各一级类目数据
    from
    (
        select
            m1.ds as ds,
            concat('POP--',m1.cname) as cname,
            m1.pay_fee as pay_fee
        from
        (
            select
                n1.ds as ds,
                (
                case
                    when n2.product_cname1 = '母婴' then '母婴'
                    when n2.product_cname1 = '手机数码' then '手机数码'
                    when n2.product_cname1 = '家用电器' then '家用电器'
                    when n2.product_cname1 = '家居百货' then '家居百货'
                    when n2.product_cname1 = '美妆个护' and n2.product_cname3 = '卫生巾/护垫/成人尿裤' then '家居百货'
                    when n2.product_cname1 = '食品' and n2.product_cname2 in ('零食/坚果/特产','酒水/茶/冲饮') then '零食/坚果/特产'
                    when n2.product_cname1 = '食品' and n2.product_cname2 in ('传统滋补营养品','粮油米面/南北干货/调味品','保健食品/膳食营养补充食品') then '传统滋补营养品'
                else '美妆个护' end
                ) as cname, 
                n1.pay_fee as pay_fee 
            from
            (
                select
                    s1.ds as ds,
                    s1.product_id as product_id,
                    sum(s1.item_price/100) as pay_fee
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
                group by 
                    s1.ds,s1.product_id
            ) n1
            inner join
            (
                select
                    product_id, 
                    product_cname1, 
                    product_cname2, 
                    product_cname3
                from 
                    data.cc_dw_fs_products_shops
                where 
                    shop_id not in ('自营的店铺id')
                and 
                    product_cname1 in ('食品','母婴','手机数码','家用电器','家居百货','美妆个护')
                and
                    product_cname2 not in ('水产肉类/新鲜蔬果/熟食')
            ) n2
            on n1.product_id = n2.product_id
        ) m1
    ) t3 
    group by 
        t3.ds,t3.cname
    union all
    select
        t4.ds as ds,
        t4.cname as cname, 
        sum(t4.pay_fee) as pay_fee --POP合计
    from
    (
        select
            n1.ds as ds,
            'POP合计' as cname, 
            n1.pay_fee as pay_fee 
        from
        (
            select
                s1.ds as ds,
                s1.product_id as product_id,
                sum(s1.item_price/100) as pay_fee
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
            group by 
                s1.ds,s1.product_id
        ) n1
        inner join
        (
            select
                product_id
            from 
                data.cc_dw_fs_products_shops
            where 
                shop_id not in ('自营的店铺id')
            and 
                product_cname1 in ('食品','母婴','手机数码','家用电器','家居百货','美妆个护')
            and
                product_cname2 not in ('水产肉类/新鲜蔬果/熟食')
        ) n2
        on n1.product_id = n2.product_id
    ) t4
    group by 
        t4.ds,t4.cname
    union all
    select
        t5.ds as ds,
        t5.cname as cname, 
        sum(t5.pay_fee) as pay_fee --杂百总计
    from
    (
        select
            n1.ds as ds,
            '杂百总计' as cname, 
            n1.pay_fee as pay_fee 
        from
        (
            select
                s1.ds as ds,
                s1.product_id as product_id,
                sum(s1.item_price/100) as pay_fee
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
            group by 
                s1.ds,s1.product_id
        ) n1
        inner join
        (
            select
                product_id
            from 
                data.cc_dw_fs_products_shops
            where 
                product_cname1 in ('食品','母婴','手机数码','家用电器','家居百货','美妆个护')
            and
                product_cname2 not in ('水产肉类/新鲜蔬果/熟食')
        ) n2
        on n1.product_id = n2.product_id
    ) t5
    group by 
        t5.ds,t5.cname
    union all
    select
        t6.ds as ds,
        t6.cname as cname, 
        sum(t6.pay_fee) as pay_fee --食品总计服饰生鲜
    from
    (
        select
            n1.ds as ds,
            (
            case
                when n2.product_cname1 = '食品' and n2.product_cname2 = '水产肉类/新鲜蔬果/熟食' then '生鲜'
                when n2.product_cname1 = '食品' and n2.product_cname2 in ('零食/坚果/特产','酒水/茶/冲饮','传统滋补营养品','粮油米面/南北干货/调味品','保健食品/膳食营养补充食品') then '食品总计'
            else '服饰' end
            ) as cname,
            n1.pay_fee as pay_fee 
        from
        (
            select
                s1.ds as ds,
                s1.product_id as product_id,
                sum(s1.item_price/100) as pay_fee
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
            group by 
                s1.ds,s1.product_id
        ) n1
        inner join
        (
            select
                product_id, 
                product_cname1, 
                product_cname2
            from 
                data.cc_dw_fs_products_shops
            where 
                product_cname1 in ('食品','男装','女装','鞋靴','箱包','配饰','运动户外','女士内衣/男士内衣/家居服') 
        ) n2
        on n1.product_id = n2.product_id
    ) t6
    group by 
        t6.ds,t6.cname
    union all
    select
        t7.ds as ds,
        t7.cname as cname,
        sum(t7.pay_fee) as pay_fee--公司总计
    from
    (
        select
            s1.ds as ds,
            '公司总计' as cname,
            (s1.item_price/100) as pay_fee
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
    ) t7
    group by 
        t7.ds,t7.cname
) m1
group by m1.ds

////////////////////////////////////////////////////////
廖宁需求某日某商品 10点 前十分钟数据
select
    n1.cck_uid as cck_uid,
    n2.real_name as real_name,
    n2.phone as phone,
    n1.order_sn as order_sn,
    n1.sales_num as sales_num,
    n1.pay_time as pay_time 
from
(
    select
        cck_uid,
        third_tradeno as order_sn,
        sale_num as sales_num,
        from_unixtime(create_time,'yyyyMMdd HH:mm:ss') as pay_time 
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_realtime
    where 
        ds = 20181010
    and
        product_id = 110019268217
    and
        create_time <= 1539101400
) n1
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
        ds = 20181009
) n2
on n1.cck_uid = n2.cck_uid

/////////////////////////////////////////////////////////
select
    n1.cck_uid as cck_uid,
    n2.real_name as real_name,
    n2.phone as phone,
    n1.sales_num as sales_num
from
(
    select
        s1.cck_uid as cck_uid,
        s1.sales_num as sales_num
    from
    (
        select
            cck_uid,
            sum(sale_num) as sales_num
        from
            origin_common.cc_ods_dwxk_wk_sales_deal_realtime
        where 
            ds = 20181011
        and
            product_id = 1100185323334
        and
            create_time >= 1539223200
        group by 
            cck_uid
    ) s1
    order by desc
        s1.sales_num
    limit 10
) n1
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
        ds = 20181010
) n2
on n1.cck_uid = n2.cck_uid
/////////////////////////////////////////////////
张梨娜
select
    t1.cck_uid as cck_uid,
    t2.real_name as real_name,
    t2.phone as phone,
    t1.create_time as create_time
from
(
    select
        distinct
        n1.cck_uid,
        from_unixtime(n1.create_time,'yyyyMMdd HH:mm:ss') as create_time
    from
    (
        select
            t1.cck_uid as cck_uid,
            min(t1.create_time) as create_time
        from
        (
            select
                cck_uid,
                sale_num,
                create_time,
                sum(sale_num) over (partition by cck_uid order by create_time) as sales_num
            from 
                origin_common.cc_ods_dwxk_wk_sales_deal_realtime
            where 
                ds = 20181011
            and 
                product_id = 1100185323334
        ) t1
        where 
            t1.sales_num >=3
        group by 
            t1.cck_uid
    ) n1
    order by 
        create_time
    limit 200
) t1
left join 
(
    select
        distinct
        cck_uid,
        real_name,
        phone
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info_realtime
    where 
        ds = 20181011
) t2
on t1.cck_uid = t2.cck_uid
////////////////////////////////////////////////////////////////////
汪柯钰需求 1209314 团队名单
select
    t1.gm_uid as gm_uid,
    t2.real_name as gm_name,
    t1.leader_uid as leader_uid,
    t3.real_name as leader_name,
    t3.phone as leader_phone,
    t1.cck_uid as cck_uid,
    t4.real_name as cck_name,
    t4.phone as cck_phone
from
(
    select
        distinct
        gm_uid,
        leader_uid,
        cck_uid,
        type
    from
        origin_common.cc_ods_fs_wk_cct_layer_info
    where 
        gm_uid = 1209314
    and 
        type = 0
) t1
left join
(
    select
        distinct
        cck_uid,
        real_name
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info_realtime
    where 
        ds = 20181011
) t2
on t1.gm_uid = t2.cck_uid
left join
(
    select
        distinct
        cck_uid,
        real_name,
        phone
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info_realtime
    where 
        ds = 20181011
) t3
on t1.leader_uid = t3.cck_uid
left join
(
    select
        distinct
        cck_uid,
        real_name,
        phone
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info_realtime
    where 
        ds = 20181011
) t4
on t1.cck_uid = t4.cck_uid
////////////////////////////////////////////////////////////////////
汪柯钰需求 1209314 团队名单
select
    t1.gm_uid as gm_uid,
    t1.cck_uid as cck_uid,
    t1.type as type,
    t2.real_name as real_name,
    t2.phone as phone,
    t3.pay_fee as pay_fee,
    t4.pay_price as pay_price
from
(
    select
        distinct
        gm_uid,
        cck_uid,
        type
    from
        origin_common.cc_ods_fs_wk_cct_layer_info
    where 
        gm_uid = 1209314
) t1
left join 
(
    select
        distinct
        cck_uid,
        real_name,
        phone
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info_realtime
    where 
        ds = 20181011
) t2
on t1.cck_uid = t2.cck_uid
left join 
(
    select
        cck_uid,
        sum(item_price/100) as pay_fee
    from 
        cc_ods_dwxk_wk_sales_deal_ctime
    where 
        ds >= '${begin_date}' 
    and 
        ds <= '${end_date}' 
    group by 
        cck_uid
) t3
on t1.cck_uid = t3.cck_uid
left join
(--对t1表进行条件限制评估，加平台=14前为164042，加之后为68386，再加status=1限制，还剩43624，再加is_del = 0还是43624，说明都删除条件影响不大
    select
        t1.invite_uid as invite_uid,
        sum(t2.pay_price/100) as pay_price
    from
    ( 
        select
            distinct
            cck_uid,
            invite_uid
        from 
            origin_common.cc_ods_fs_wk_cct_layer_info
        where 
            from_unixtime(create_time,'yyyyMMdd') >= '${begin_date}' 
        and 
            from_unixtime(create_time,'yyyyMMdd') <= '${end_date}' 
        and 
            platform = 14 
        and 
            status = 1 
        and 
            is_del = 0
    ) t1
    inner join 
    (--对t2表进行条件限制评估，pay_price=39900，有164042， 加上status = 1仍然是164042，加上pay_status=1还剩158682，说明审核通过不影响，支付状态条件略有影响。
        select
            distinct
            cck_uid,
            pay_price
        from 
            origin_common.cc_ods_dwxk_fs_wk_business_info
        where 
            ds = 20181010
        and 
            from_unixtime(create_time,'yyyyMMdd') >= '${begin_date}' 
        and 
            from_unixtime(create_time,'yyyyMMdd') <= '${end_date}' 
        and 
            pay_price in (39900,49900)
        and 
            status = 1
        and 
            pay_status = 1
    ) t2
    on  t1.cck_uid = t2.cck_uid
    group by 
        t1.invite_uid
) t4 
on t1.cck_uid=t4.invite_uid
//////////////////////////////////////////////////////////////////////////////////////////
汪柯钰需求 1199321 穆蓉 团队 10.16-10.19每日新增人数
select
    distinct
    gm_uid,
    cck_uid,
    from_unixtime(create_time,'yyyyMMdd') as ds 
from
    origin_common.cc_ods_fs_wk_cct_layer_info
where 
    gm_uid = 1199321
and
    from_unixtime(create_time,'yyyyMMdd') >= '${begin_date}' 
and 
    from_unixtime(create_time,'yyyyMMdd') <= '${end_date}' 

//////////////////////////////////////////////////////////////////////////////////////////
汪柯钰需求 1199321 穆蓉 团队 1号到今天  具体每一天的礼包销售额
select
    t1.gm_uid as gm_uid,
    t4.ds as ds, 
    sum(t4.pay_price) as pay_price
from
(
    select
        distinct
        gm_uid,
        cck_uid
    from
        origin_common.cc_ods_fs_wk_cct_layer_info
    where 
        gm_uid = 1199321
    union all
    select
        distinct
        gm_uid,
        gm_uid as cck_uid
    from
        origin_common.cc_ods_fs_wk_cct_layer_info
    where 
        gm_uid = 1199321
) t1
left join
(--对t1表进行条件限制评估，加平台=14前为164042，加之后为68386，再加status=1限制，还剩43624，再加is_del = 0还是43624，说明都删除条件影响不大
    select
        t1.ds as ds, 
        t1.invite_uid as invite_uid,
        sum(t2.pay_price/100) as pay_price
    from
    ( 
        select
            distinct
            from_unixtime(create_time,'yyyyMMdd') as ds,
            cck_uid,
            invite_uid
        from 
            origin_common.cc_ods_fs_wk_cct_layer_info
        where 
            from_unixtime(create_time,'yyyyMMdd') >= '${begin_date}' 
        and 
            from_unixtime(create_time,'yyyyMMdd') <= '${end_date}' 
        and 
            platform = 14 
        and 
            status = 1 
        and 
            is_del = 0
    ) t1
    inner join 
    (--对t2表进行条件限制评估，pay_price=39900，有164042， 加上status = 1仍然是164042，加上pay_status=1还剩158682，说明审核通过不影响，支付状态条件略有影响。
        select
            distinct
            from_unixtime(create_time,'yyyyMMdd') as ds,
            cck_uid,
            pay_price
        from 
            origin_common.cc_ods_dwxk_fs_wk_business_info
        where 
            ds = 20181018
        and 
            from_unixtime(create_time,'yyyyMMdd') >= '${begin_date}' 
        and 
            from_unixtime(create_time,'yyyyMMdd') <= '${end_date}' 
        and 
            pay_price in (39900,49900)
        and 
            status = 1
        and 
            pay_status = 1
    ) t2
    on  t1.cck_uid = t2.cck_uid and t1.ds = t2.ds 
    group by 
        t1.invite_uid,t1.ds
) t4 
on t1.cck_uid=t4.invite_uid
group by 
    t1.gm_uid,t4.ds
/////////////////////////////////////////////////////////////////////////////////////////////
张梨娜需求 
select
    n1.cck_uid,
    n2.real_name,
    n2.phone
from
(
    select
        cck_uid,
        sum(sale_num) as sales_num
    from 
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where 
        ds = 20181014
    and
        product_id = 1100185322344
    group by
        cck_uid

) n1
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
        ds = 20181014
) n2
on n1.cck_uid = n2.cck_uid
/////////////////////////////////////////////////////////////////////////////////
昝皓东 某礼包的个人销售数据
select
    t2.invite_uid as invite_uid,
    t1.cck_uid as cck_uid
from
( 
    select
        cck_uid
    from 
        origin_common.cc_ods_fs_wk_cck_gifts 
    where 
       from_unixtime(create_time,'yyyyMMdd') >= 20181012
    and
       from_unixtime(create_time,'yyyyMMdd') <= 20181014
    and
        product_id = 110020065100
    and 
        pay_status = 1
    and 
        platform = 14

) t1
left join 
(
    select
        cck_uid,
        invite_uid
    from 
        origin_common.cc_ods_fs_wk_cct_layer_info
    where 
        from_unixtime(create_time,'yyyyMMdd') >= 20181012
    and 
        from_unixtime(create_time,'yyyyMMdd') <= 20181014
    and 
        platform = 14 
    and 
        status = 1 
) t2
on  t1.cck_uid = t2.cck_uid
////////////////////////////////////////////////////////////////////////////////////
昝皓东 某礼包的团队销售数据 
select
    m1.gm_uid as gm_uid,
    m2.real_name as real_name,
    m2.phone as phone,
    m1.sales_num as sales_num
from
(
    select
        t1.gm_uid as gm_uid,
        count(t4.cck_uid) as sales_num
    from
    (
        select
            distinct
            gm_uid,
            cck_uid
        from 
            origin_common.cc_ods_fs_wk_cct_layer_info
        union all
        select
            distinct
            gm_uid as gm_uid,
            gm_uid as cck_uid
        from 
            origin_common.cc_ods_fs_wk_cct_layer_info
    ) t1 
    left join
    (--对t1表进行条件限制评估，加平台=14前为164042，加之后为68386，再加status=1限制，还剩43624，再加is_del = 0还是43624，说明都删除条件影响不大
        select
            t1.invite_uid as invite_uid,
            t2.cck_uid as cck_uid
        from
        ( 
            select
                distinct
                invite_uid,
                cck_uid
            from 
                origin_common.cc_ods_fs_wk_cct_layer_info
            where 
                from_unixtime(create_time,'yyyyMMdd') >= 20181012
            and 
                from_unixtime(create_time,'yyyyMMdd') <= 20181014
            and 
                platform = 14 
            and 
                status = 1 
        ) t1
        inner join 
        (
            select
                cck_uid
            from 
                origin_common.cc_ods_fs_wk_cck_gifts 
            where 
               from_unixtime(create_time,'yyyyMMdd') >= 20181012
            and
               from_unixtime(create_time,'yyyyMMdd') <= 20181014
            and
                product_id = 11002006596
            and 
                pay_status = 1
        ) t2
        on  t1.cck_uid = t2.cck_uid
    ) t4 
    on t1.cck_uid=t4.invite_uid
    group by
        t1.gm_uid 

) m1
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
        ds = 20181014
) m2
on m1.gm_uid = m2.cck_uid   
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 
昝皓东  在10月1日—7日期间销售3个创业礼包
select
    t1.invite_uid as cck_uid,
    t1.num as num,
    t3.cct_uid as cct_uid
from
(
    select
        invite_uid,
        count(cck_uid) as num
    from 
        origin_common.cc_ods_fs_wk_cct_layer_info
    where 
        from_unixtime(create_time,'yyyyMMdd') >= 20181001
    and 
        from_unixtime(create_time,'yyyyMMdd') <= 20181007
    and 
        platform = 14 
    and 
        status = 1 
    group by
        invite_uid
) t1
left join 
(
    select
        distinct
        cck_uid,
        cct_uid
    from 
        origin_common.cc_ods_fs_tui_relation
) t3 
on t1.invite_uid = t3.cck_uid
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 
昝皓东  一个是在10月1日—7日期间商品销售/自买超过2000元的用户
select
    n1.cck_uid as cck_uid,
    (n1.pay_fee+n1.discount_fee) as pay_fee,
    n2.cct_uid as cct_uid
from
(
    select
        t1.cck_uid as cck_uid,
        sum(t1.pay_fee) as pay_fee,
        sum(t1.discount_fee) as discount_fee
    from
    (
        select
            s1.cck_uid as cck_uid,
            s1.third_tradeno as third_tradeno,
            (s1.item_price/100) as pay_fee,
            (s1.discount_fee/100) as discount_fee
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
        inner join 
            origin_common.cc_ods_dwxk_fs_wk_cck_user s2
        on 
            s1.cck_uid = s2.cck_uid
        where 
            s1.ds >= 20181001
        and 
            s1.ds <= 20181007
        and 
            s2.ds = 20181022
        and 
            s2.platform = 14
    ) t1
    left join
    (
        select
            distinct
            order_sn
        from
            origin_common.cc_ods_fs_refund_order
        where 
            from_unixtime(create_time,'yyyyMMdd') >= 20181001
        and 
            from_unixtime(create_time,'yyyyMMdd') <= 20181019
        and
            status = 1
    ) t2
    on t1.third_tradeno = t2.order_sn
    where 
        t2.order_sn is null
    group by
            t1.cck_uid 
) n1 
left join 
(
    select
        distinct
        cck_uid,
        cct_uid
    from 
        origin_common.cc_ods_fs_tui_relation
) n2
on n1.cck_uid = n2.cck_uid
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
汪柯钰 1240953 范婷 从9月15号到10月12号的个人实际销售额不含礼包
(
    select
        cck_uid,
        sum(item_price/100) as pay_fee
    from 
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where 
        ds >= 20180915
    and 
        ds <= 20181012
    and 
        cck_uid = 1240953
    group by
        cck_uid
) n1
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
廖宁 11002047110   10月15日  服务经理 销售前40名 姓名 电话 销售额   销售单数 和件数
select
    m1.gm_uid as gm_uid,
    m2.real_name as real_name,
    m2.phone as phone,
    m1.pay_fee as pay_fee,
    m1.order_count as order_count,
    m1.sales_num as sales_num
from
(
    select
        t2.gm_uid as gm_uid,
        sum(t1.pay_fee) as pay_fee,
        sum(t1.order_count) as order_count,
        sum(t1.sales_num) as sales_num
    from
    (
        select
            cck_uid,
            sum(item_price/100) as pay_fee,
            count(distinct third_tradeno) as order_count,
            sum(sale_num) as sales_num
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where 
            ds = 20181018
        and 
            product_id = 1100209391 
        group by
            cck_uid  
    ) t1 
    left join 
    (
        select
            n1.gm_uid as gm_uid,
            n1.cck_uid as cck_uid
        from
        (
            select
                distinct
                gm_uid,
                cck_uid
            from 
                origin_common.cc_ods_fs_wk_cct_layer_info
            where gm_uid=0
        ) n1
        left join 
        (
            select
                distinct
                gm_uid as gm_uid,
                gm_uid as cck_uid
            from 
                origin_common.cc_ods_fs_wk_cct_layer_info
        ) n2
        on n1.cck_uid = n2.gm_uid
        where n2.cck_uid is null
        union all
        select
            distinct
            gm_uid,
            cck_uid
        from 
            origin_common.cc_ods_fs_wk_cct_layer_info
        where gm_uid != 0 
        union all
        select
            distinct
            gm_uid as gm_uid,
            gm_uid as cck_uid
        from 
            origin_common.cc_ods_fs_wk_cct_layer_info
    ) t2
    on t1.cck_uid =t2.cck_uid
    group by 
        t2.gm_uid
) m1 
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
        ds = 20181018
) m2 
on m1.gm_uid = m2.cck_uid   
//////////////////////////////////////////////////////////////////////////////////////////////////////
没有下属的总经理
select
    n1.gm_uid as gm_uid,
    n1.cck_uid as cck_uid
from
(
    select
        distinct
        gm_uid,
        cck_uid
    from 
        origin_common.cc_ods_fs_wk_cct_layer_info
    where gm_uid=0
) n1
left join 
(
    select
        distinct
        gm_uid as gm_uid,
        gm_uid as cck_uid
    from 
        origin_common.cc_ods_fs_wk_cct_layer_info
) n2
on n1.cck_uid = n2.gm_uid
where n2.cck_uid is null
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
魏薇 110019347636 时间：今天19：00-19：05 的订单
select 
    m1.cck_uid as cck_uid,
    m2.real_name as real_name,
    m2.phone as phone,
    m1.pay_fee as pay_fee,
    m1.order_sn as order_sn,
    m1.sale_num as sale_num,
    m1.create_time as create_time
from
(
    select
        distinct
        product_id,
        cck_uid,
        (item_price/100) as pay_fee,
        third_tradeno as order_sn,
        sale_num as sale_num,
        from_unixtime(create_time,'yyyyMMdd HH:mm:ss') as create_time
    from 
        origin_common.cc_ods_dwxk_wk_sales_deal_realtime
    where 
        ds = 20181016
    and 
        product_id = 110019347636 
    and 
        create_time >= 1539687600
    and 
        create_time <= 1539687900
) m1 
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
        ds = 20181016
) m2 
on m1.cck_uid = m2.cck_uid  
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
廖宁 某些人手机号得到cck_uid,cct_uid
select
    n1.cck_uid,
    n1.real_name,
    n1.phone,
    n2.cct_uid
from
(
    select
        distinct
        cck_uid,
        real_name,
        phone
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = 20190115
    and 
        phone in ()
) n1
left join 
(
    select
        distinct
        cck_uid,
        cct_uid
    from 
        origin_common.cc_ods_fs_tui_relation
) n2
on n1.cck_uid = n2.cck_uid
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
select        
    t1.cck_uid as cck_uid,
    t2.real_name as real_name,
    t2.phone as phone,
    t3.cct_uid as cct_uid,
    t1.pay_fee as pay_fee,
    t1.create_time as create_time
from
(
    select
        distinct
        product_id,
        cck_uid,
        (item_price/100) as pay_fee,
        third_tradeno as order_sn,
        sale_num as sale_num,
        from_unixtime(create_time,'yyyyMMdd HH:mm:ss') as create_time
    from 
        origin_common.cc_ods_dwxk_wk_sales_deal_realtime
    where 
        ds = 20181018
    and 
        product_id = 1100209391 
    and 
        create_time >= 1539864000
    and 
        create_time <= 1539871200
) t1
left join
(
    select
        distinct
        cck_uid,
        real_name,
        phone
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info_realtime
    where 
        ds = 20181018
) t2
on t1.cck_uid = t2.cck_uid
left join 
(
    select
        distinct
        cck_uid,
        cct_uid
    from 
        origin_common.cc_ods_fs_tui_relation
) t3 
on t1.cck_uid = t3.cck_uid

////////////////////////////////////////////////////////////////
张菊 
select
    t1.shop_id as shop_id,
    t1.pay_fee as pay_fee,--支付金额
    t1.order_count as order_count,--订单数
    t1.sales_num as sales_num,--销量
    t1.cck_commission as cck_commission,--直接佣金
    t1.pv as pv,
    t1.uv as uv,
    t1.fx_user_cnt as fx_user_cnt,--总推广人数
    t1.fx_cnt as fx_cnt,--总推广次数
    t2.shop_name as shop_name,
    t2.company_name as company_name
from
(
    select
        n1.shop_id as shop_id,
        sum(n2.pay_fee) as pay_fee,--支付金额
        sum(n2.order_count) as order_count,--订单数
        sum(n2.sales_num) as sales_num,--销量
        sum(n2.cck_commission) as cck_commission,--直接佣金
        sum(n3.pv) as pv,
        sum(n3.uv) as uv,
        sum(n5.fx_user_cnt) as fx_user_cnt,--总推广人数
        sum(n5.fx_cnt) as fx_cnt--总推广次数
    from
    (
        select
            shop_id,
            product_id
        from
            data.cc_dw_fs_products_shops
        where 
            product_cname1 = '美妆个护'
    ) n1 
    left join 
    (
        select
            product_id,
            sum(item_price/100) as pay_fee,
            count(distinct third_tradeno) as order_count,
            sum(sale_num) as sales_num,
            sum(cck_commission/100) as cck_commission
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where
            ds>= '${begin_date}'
        and
            ds<= '${end_date}'
        group by 
            product_id
    ) n2
    on n1.product_id = n2.product_id
    left join 
    (
        select
            a1.product_id as product_id,
            count(a1.cck_uid) as pv,--站内pv
            count(distinct a1.user_id) as uv--站内uv
        from
        (
            select
                product_id,
                cck_uid,
                user_id
            from 
                origin_common.cc_ods_log_cctui_product_coupon_detail_hourly 
            where 
                ds>= '${begin_date}'
            and
                ds<= '${end_date}'
            and 
                detail_type='item' 
            and 
                is_in_app = 1
        ) as a1 
        group by 
            a1.product_id
    ) n3
    on n1.product_id = n3.product_id
    left join 
    (
        select
            m3.product_id as product_id,
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
                ds>= '${begin_date}'
            and
                ds<= '${end_date}'
            and 
                ad_type in ('search','category') 
            and 
                module = 'detail_material' 
            and 
                zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC')
            union all
            select
                ad_id,
                user_id
            from 
                origin_common.cc_ods_log_cctapp_click_hourly
            where 
                ds>= '${begin_date}'
            and
                ds<= '${end_date}'
            and 
                ad_type not in ('search','category') 
            and 
                module = 'detail_material' 
            and 
                zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC')
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
                    ds>= '${begin_date}'
                and
                    ds<= '${end_date}'
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
                    ds>= '${begin_date}'
                and
                    ds<= '${end_date}'
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
        group by
            m3.product_id
    )  n5
    on n1.product_id = n5.product_id
    group by 
        n1.shop_id
) t1 
left join 
(
    select
        distinct
        shop_id,
        shop_name,
        company_name
    from
        cc_ods_fs_dwxk_business_basic -- 这张表是全面的
) t2
on t1.shop_id = t2.shop_id
//////////////////////////////////////////////////////////////////////////////////////////////////////////
张菊2
select
    n1.product_id,
    n1.product_title,
    n1.shop_id,
    n1.shop_title,
    n2.ad_price as ad_price,---券前价
    n2.cck_rate as cck_rate,---楚客佣金率
    n2.cck_price as cck_price,---楚客佣金额
    n3.pay_fee as pay_fee,
    n3.order_count as order_count,
    n3.sales_num as sales_num,
    n3.cck_commission as cck_commission,
    n4.pv as pv,
    n4.uv as uv,
    n5.fx_user_cnt as fx_user_cnt,--总推广人数
    n5.fx_cnt as fx_cnt--总推广次数
from
(
    select
        product_id,
        product_title,
        shop_id,
        shop_title
    from
        data.cc_dw_fs_products_shops
    where 
        product_cname1 = '美妆个护'
    and 
        shop_id not in (18706,18662,18729,18838,18704,18730,18588,18723,18740,18799,18636,18635,18586,18542,18569,18455,17791,18240,19319,19239,18262,19339,19405,19392,19468,18470,18606,15426,17636,18314,19298,19505,19504,19486,19470,19404,18731,18482,19527,19521,19525,19542,18491,19611,19613,19609,19599,19580,19534,19664,19435,19089,2873,19701,19699,19683,19682,19678,19402,19708,19667,19765,19742,19722,19753,18765,20016,19906,19907,19870,20063,20064,2369,9872,19871,19756,19755,19709,16851,20179,20142,20178,20168,20203,19903,17691,20242,20236,20237,20202,20188,456,3559,20314,20332,13930,20343,18574,20305,15907,20392,20322,20423,20216,20513,20065,20543,20548,20600,18965,20652,20653,4086,20696,20697,20725,20738,19517,20737,19627,20748,20770,18327,12902,11974,12334,15670,15912,14715,5649,16898,15729,2752,12375,4599,13706,15395,12461,19654,16293,4024,20353,17929,104,3037,19170,14948,1793,19207,4999,16137,3885,16671,18791,17210,5987,14956,1341,15499,1555,18381,16194,5107,16133,8670,2254,18253,3803,17773,13698,17576,14832,18565,20789,739,9349,7693,14720,15044,13638,7200,4318,12033,12766,17639,13363,16305,15853,6163,11500,9806,4539,20828,20818,17845,12523,13559,13991,1412,14823,14948,15129,1655,17157,17397,1802,18057,1831,18812,18814,1937,7572,9621,20784)
) n1 
inner join
(
    select
        s2.app_item_id as product_id,
        (s1.ad_price/100) as ad_price,
        (s1.cck_rate/1000) as cck_rate,---楚客佣金率
        (s1.cck_price/100) as cck_price---楚客佣金额
    from
        cc_ods_dwxk_fs_wk_ad_items s1
    inner join
        cc_ods_dwxk_fs_wk_items s2
    on 
        s1.item_id =s2.item_id
    where
        s1.audit_status=1
    and
        s1.status=1
) n2
on n1.product_id = n2.product_id
left join 
(
    select
        product_id,
        sum(item_price/100) as pay_fee,
        count(distinct third_tradeno) as order_count,
        sum(sale_num) as sales_num,
        sum(cck_commission/100) as cck_commission
    from 
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where
        ds>= '${begin_date}'
    and
        ds<= '${end_date}'
    group by 
        product_id
) n3
on n1.product_id = n3.product_id
left join 
(
    select
        a1.product_id as product_id,
        count(a1.cck_uid) as pv,--站内pv
        count(distinct a1.user_id) as uv--站内uv
    from
    (
        select
            product_id,
            cck_uid,
            user_id
        from 
            origin_common.cc_ods_log_cctui_product_coupon_detail_hourly 
        where 
            ds>= '${begin_date}'
        and
            ds<= '${end_date}'
        and 
            detail_type='item' 
        and 
            is_in_app = 1
    ) as a1 
    group by 
        a1.product_id
) n4
on n1.product_id = n4.product_id
left join 
(
    select
        m3.product_id as product_id,
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
            ds>= '${begin_date}'
        and
            ds<= '${end_date}'
        and 
            ad_type in ('search','category') 
        and 
            module = 'detail_material' 
        and 
            zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC')
        union all
        select
            ad_id,
            user_id
        from 
            origin_common.cc_ods_log_cctapp_click_hourly
        where 
            ds>= '${begin_date}'
        and
            ds<= '${end_date}'
        and 
            ad_type not in ('search','category') 
        and 
            module = 'detail_material' 
        and 
            zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC')
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
                ds>= '${begin_date}'
            and
                ds<= '${end_date}'
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
                ds>= '${begin_date}'
            and
                ds<= '${end_date}'
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
    group by
        m3.product_id
)  n5
on n1.product_id = n5.product_id

//////////////////////////////////////////////////////////////////////////////////////////////////////////
select
    distinct
    shop_id,
    shop_name,
    company_name
from
    cc_business_basic
where shop_id in ()

//////////////////////////////////////////////////////////////////////////////////////////////////////////
select
    *
from 
    cc_ods_fs_wk_cct_layer_info
where
    gm_uid = 1199321
and 
    leader_uid =  1200412

////////////////////////////////////////////////////////////////////////////////////////
select
    t1.product_id,
    count(t1.third_tradeno) as pay_count,
    count(t2.order_sn) as refund_count
from
(
    select
        product_id,
        third_tradeno as third_tradeno
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where 
        ds = '${stat_date}'
    and  
        product_id = '${product_id}'
) t1
left join 
(
    select 
        distinct
        order_sn
    from
        origin_common.cc_ods_fs_refund_order 
    where
        from_unixtime(create_time,'yyyyMMdd')>='${stat_date}'
    and
        status = 1
) t2
on t1.third_tradeno=t2.order_sn
group by 
    t1.product_id
////////////////////////////////////////////////////////////////////////////////////////
select
    cck_uid,
    type,
    is_del,
    platform
from
    wk_cct_layer_info_20181017
where 
    gm_uid = 1199321
////////////////////////////////////////////////////////////////////////////////////////
select
    cck_uid,
    type,
    leader_uid,
    is_del,
    platform
from
    wk_cct_layer_info_20181016
where 
    cck_uid in ()
//////////////////////////////////////////////////////////////////////
select
    cck_uid,
    real_name,
    phone
from wk_business_info
//////////////////////////////////////////////////////////////////////
汪柯钰需求 593678 1.22-10.23 总监的商品销售额
select
    t1.leader_uid as leader_uid,
    sum(t3.pay_fee) as pay_fee
from
(
    select
        distinct
        leader_uid,
        cck_uid
    from
        origin_common.cc_ods_fs_wk_cct_layer_info
    where 
        leader_uid = 593678
    union all
    select
        distinct
        leader_uid,
        leader_uid as cck_uid
    from
        origin_common.cc_ods_fs_wk_cct_layer_info
    where 
        leader_uid = 593678  
) t1
left join 
(
    select
        cck_uid,
        sum(item_price/100) as pay_fee
    from 
        cc_ods_dwxk_wk_sales_deal_ctime
    where 
        ds >= '${begin_date}' 
    and 
        ds <= '${end_date}' 
    group by 
        cck_uid
) t3
on t1.cck_uid = t3.cck_uid
group by t1.leader_uid
/////////////////////////////////////////////////////
查某团队培训津贴收入
select
    t1.gm_uid as gm_uid,
    t1.cck_uid as cck_uid,
    t2.cck_uid as cck_uid, 
    t2.source_uid as source_uid, 
    t2.source_name as source_name, 
    t2.train_fee as train_fee, 
    t2.remark as remark,
    t2.time as time
from
(
    select
        distinct
        gm_uid,
        cck_uid
    from 
        origin_common.cc_ods_fs_wk_cct_layer_info
    where 
        gm_uid = 1199635
) t1 
left join 
(
    select
        cck_uid, 
        source_uid, 
        source_name, 
        (money/100) as train_fee, 
        remark,
        from_unixtime(create_time,'yyyyMMdd HH:mm:dd') as time
    from  
        origin_common.cc_ods_dwxk_user_train_bill_mtime 
    where
        ds = 20181020
) t2
on t1.cck_uid = t2.cck_uid
/////////////////////////////////////////////////////////
查一个楚客的礼包购买情况
select
*
from 
    origin_common.cc_ods_fs_wk_cck_gifts 
where 
    cck_uid = 1208666

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
营销 手机号匹配cct_uid
select
    n1.cck_uid,
    n1.phone_number,
    n2.cct_uid
from
(
    select
        distinct
        cck_uid,
        phone_number
    from 
        origin_common.cc_ods_dwxk_fs_wk_cck_user
    where 
        ds = 20181023 
    and 
        platform=14
    and 
        phone_number in ('15642110717','15881064468','18195163662','17502980127','15267715168','13501739965','15084877172','15949745995','13806964359','13469505088','18695160865','15920587873','17395195825','13548944399','17392366592','13995290584','19992610558','18871568163','18220553272','13409589626','15379504676','18875155543','15994882591','15821222816','13709503487','18766928398')
) n1
left join 
(
    select
        distinct
        cck_uid,
        cct_uid
    from 
        origin_common.cc_ods_fs_tui_relation
) n2
on n1.cck_uid = n2.cck_uid
////////////////////////////////////////////////////////////////////
魏薇 110019347760 时间：20181024 整点前一分的数据 的订单
select 
    m1.cck_uid as cck_uid,
    m2.real_name as real_name,
    m2.phone as phone,
    m1.pay_fee as pay_fee,
    m1.order_sn as order_sn,
    m1.sale_num as sale_num,
    m1.create_time as create_time
from
(
    select
        distinct
        s1.cck_uid as cck_uid,
        (s1.item_price/100) as pay_fee,
        s1.third_tradeno as order_sn,
        s1.sale_num as sale_num,
        from_unixtime(s1.create_time,'yyyyMMdd HH:mm:ss') as create_time
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
    join
        origin_common.cc_ods_dwxk_fs_wk_cck_user s2
    on 
        s1.cck_uid=s2.cck_uid
    where
        s1.ds = 20181024
    and
        s1.product_id = 110019347760
    and 
        s1.create_time >= 
    and 
        s1.create_time <= 
    and
        s2.ds = 20181024
    and
        s2.platform = 14
) m1 
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
        ds = 20181024
) m2 
on m1.cck_uid = m2.cck_uid 
///////////////////////////////////////////////////////////////
姜子娣
select
    n1.product_id,
    n1.product_title,
    n1.shop_id,
    n1.shop_title,
    (n2.ad_price/100) as ad_price,---券前价
    (n2.cck_rate/1000) as cck_rate,---楚客佣金率
    (n2.cck_price/100) as cck_price---楚客佣金额
from
(
    select
        product_id,
        product_title,
        shop_id,
        shop_title
    from
        data.cc_dw_fs_products_shops
    where 
        product_cname1 = '美妆个护'
    and shop_id not in (18706,18662,18729,18838,18704,18730,18588,18723,18740,18799,18636,18635,18586,18542,18569,18455,17791,18240,19319,19239,18262,19339,19405,19392,19468,18470,18606,15426,17636,18314,19298,19505,19504,19486,19470,19404,18731,18482,19527,19521,19525,19542,18491,19611,19613,19609,19599,19580,19534,19664,19435,19089,2873,19701,19699,19683,19682,19678,19402,19708,19667,19765,19742,19722,19753,18765,20016,19906,19907,19870,20063,20064,2369,9872,19871,19756,19755,19709,16851,20179,20142,20178,20168,20203,19903,17691,20242,20236,20237,20202,20188,456,3559,20314,20332,13930,20343,18574,20305,15907,20392,20322,20423,20216,20513,20065,20543,20548,20600,18965,20652,20653,4086,20696,20697,20725,20738,19517,20737,19627,20748,20770,18327,12902,11974,12334,15670,15912,14715,5649,16898,15729,2752,12375,4599,13706,15395,12461,19654,16293,4024,20353,17929,104,3037,19170,14948,1793,19207,4999,16137,3885,16671,18791,17210,5987,14956,1341,15499,1555,18381,16194,5107,16133,8670,2254,18253,3803,17773,13698,17576,14832,18565,20789,739,9349,7693,14720,15044,13638,7200,4318,12033,12766,17639,13363,16305,15853,6163,11500,9806,4539,20828,20818,17845,12523,13559,13991,1412,14823,14948,15129,1655,17157,17397,1802,18057,1831,18812,18814,1937,7572,9621,20784)
) n1 
inner join
(
    select
        s2.app_item_id as product_id,
        s1.ad_price as ad_price,
        s1.cck_rate as cck_rate,---楚客佣金率
        s1.cck_price as cck_price---楚客佣金额
    from
        cc_ods_dwxk_fs_wk_ad_items s1
    inner join
        cc_ods_dwxk_fs_wk_items s2
    on 
        s1.item_id =s2.item_id
    where
        s1.audit_status=1
    and
        s1.status=1
) n2
on n1.product_id = n2.product_id
///////////////////////////////////////////////////////////////////////////////////////////////////////////////
select
    n1.gm_uid as gm_uid,
    n3.real_name as gm_name,
    n1.cck_uid as cck_uid,
    n2.real_name as cck_name,
    n1.pay_fee as pay_fee,
    n1.pay_price as pay_price
from
(
    select
        t1.gm_uid as gm_uid,
        t1.cck_uid as cck_uid,
        sum(t3.pay_fee) as pay_fee,
        sum(t4.pay_price) as pay_price
    from
    (
        select
            distinct
            gm_uid,
            cck_uid
        from
            origin_common.cc_ods_fs_wk_cct_layer_info
        where 
            gm_uid in (1199321,1199168,1197475,1202494,1210498,1200648,1204007,1240629,1199210,1209314,1199978,1199305,1199214,1199749,1201288,1205821,1201128,1240633,1199956,1199621,1199365,1204049,1241239,1200483,1252167,1199985,1199515,1199635,1257637,1199349,1200412) 
        union all
        select
            distinct
            gm_uid,
            cck_uid
        from
            origin_common.cc_ods_fs_wk_cct_layer_info
        where 
            gm_uid in (1199321,1199168,1197475,1202494,1210498,1200648,1204007,1240629,1199210,1209314,1199978,1199305,1199214,1199749,1201288,1205821,1201128,1240633,1199956,1199621,1199365,1204049,1241239,1200483,1252167,1199985,1199515,1199635,1257637,1199349,1200412)
    ) t1
    left join 
    (
        select
            cck_uid,
            sum(item_price/100) as pay_fee
        from 
            cc_ods_dwxk_wk_sales_deal_ctime
        where 
            ds >= '${begin_date}' 
        and 
            ds <= '${end_date}' 
        group by 
            cck_uid
    ) t3
    on t1.cck_uid = t3.cck_uid
    left join
    (--对t1表进行条件限制评估，加平台=14前为164042，加之后为68386，再加status=1限制，还剩43624，再加is_del = 0还是43624，说明都删除条件影响不大
        select
            t1.invite_uid as invite_uid,
            sum(t2.pay_price/100) as pay_price
        from
        ( 
            select
                distinct
                cck_uid,
                invite_uid
            from 
                origin_common.cc_ods_fs_wk_cct_layer_info
            where 
                from_unixtime(create_time,'yyyyMMdd HH:mm:ss') >= '${begin_date}' 
            and 
                from_unixtime(create_time,'yyyyMMdd HH:mm:ss') <= '${end_date}' 
            and 
                platform = 14 
            and 
                status = 1 
        ) t1
        inner join 
        (--对t2表进行条件限制评估，pay_price=39900，有164042， 加上status = 1仍然是164042，加上pay_status=1还剩158682，说明审核通过不影响，支付状态条件略有影响。
            select
                distinct
                cck_uid,
                pay_price
            from 
                origin_common.cc_ods_dwxk_fs_wk_business_info
            where 
                ds = 20181025
            and 
                from_unixtime(create_time,'yyyyMMdd HH:mm:ss') >= '${begin_date}' 
            and 
                from_unixtime(create_time,'yyyyMMdd HH:mm:ss') <= '${end_date}' 
            and 
                pay_price in (39900,49900)
            and 
                pay_status = 1
        ) t2
        on  t1.cck_uid = t2.cck_uid
        group by 
            t1.invite_uid
    ) t4 
    on t1.cck_uid=t4.invite_uid
    group by 
        t1.gm_uid,t1.cck_uid 
) n1
left join 
(
    select
        distinct
        cck_uid,
        real_name
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = 20181025
) n2
on n1.cck_uid = n2.cck_uid
left join 
(
    select
        distinct
        cck_uid,
        real_name
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = 20181025
) n3
on n1.gm_uid = n3.cck_uid
////////////////////////////////////////////////////////////////////////////////////////////
汪柯钰 某礼包最快完成直邀20人的前20名
select
    s1.invite_uid as invite_uid,
    s2.cct_uid as cct_uid,
    s3.cck_uid as cck_uid,
    s3.real_name as real_name,
    from_unixtime(s1.create_time,'yyyyMMdd HH:mm:ss') as pay_time,
    s1.total_invite_num 
from
(
    select
        n1.invite_uidss as invite_uid,
        n1.create_time,
        n1.total_invite_num
    from
    (
        select
            t1.invite_uidss as invite_uidss,
            min(t1.create_time) as create_time,
            max(t1.invite_num) as total_invite_num
        from
        (
            select
                invite_uidss,
                cck_uid,
                create_time,
                count(cck_uid) over (partition by invite_uidss order by create_time) as invite_num
            from 
                origin_common.cc_ods_fs_wk_cck_gifts
            where 
                product_id = 1100185324128
            and
                platform = 14
            and
                create_time >=
            and 
                create_time <=
        ) t1
        where 
            t1.invite_num >=20
        group by 
            t1.invite_uidss
    ) n1
    order by n1.create_time
    limit 20
) s1
left join 
(
    select
        distinct
        cck_uid,
        cct_uid
    from 
        origin_common.cc_ods_fs_tui_relation
) s2
on s1.invite_uid = s2.cck_uid
(
    select
        distinct
        cck_uid,
        real_name
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = 20181101
) s3
on s1.invite_uid = s3.cck_uid
////////////////////////////////////////////////////////////////////////////////////////////
汪柯钰 当天购买某礼包的所有楚客信息
select
    s1.cck_uid,
    s2.cct_uid,
    s3.real_name,
    s3.phone
from
(
    select
        cck_uid,
        create_time
    from 
        origin_common.cc_ods_fs_wk_cck_gifts
    where 
        product_id = 1100185324217
    and
        platform = 14
    and
        create_time >=1541088000
    and 
        create_time <=1541174399
) s1
left join 
(
    select
        distinct
        cck_uid,
        cct_uid
    from 
        origin_common.cc_ods_fs_tui_relation
) s2
on s1.cck_uid = s2.cck_uid
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
        ds = 20181102
) s3
on s1.cck_uid = s3.cck_uid
////////////////////////////////////////////////////////////////////////////////////////////
郑羽佳 某日某商品 截至今日 排除退款 实际销售数据 
select
    n2.cck_uid as cck_uid,
    n3.real_name as real_name,
    n3.phone as phone,
    n2.real_order_num as real_order_num,
    n2.real_sales_num as real_sales_num,
    n2.real_pay_fee as real_pay_fee
from
(
    select
        t1.cck_uid as cck_uid,
        count(t1.third_tradeno) as real_order_num,
        sum(t1.sale_num) as real_sales_num,
        sum(t1.item_price/100) as real_pay_fee
    from
    (
        select
            s1.cck_uid as cck_uid,
            s1.item_price as item_price,
            s1.third_tradeno as third_tradeno,
            s1.sale_num as sale_num
        from
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
        join
            origin_common.cc_ods_dwxk_fs_wk_cck_user s2
        on 
            s1.cck_uid=s2.cck_uid
        where
            s1.ds = 20181005  
        and
            s1.product_id = 110019405786
        and
            s2.ds = 20181031
        and
            s2.platform = 14
    ) t1
    left join 
    (
        select
            distinct
            order_sn
        from
            origin_common.cc_ods_fs_refund_order
        where 
            from_unixtime(create_time,'yyyyMMdd') >= 20181004
        and
            status = 1
    ) t2
    on t1.third_tradeno = t2.order_sn
    where 
        t2.order_sn is null 
    group by 
        t1.cck_uid
) n2
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
        ds = 20181031
) n3
on n2.cck_uid = n3.cck_uid
//////////////////////////////////////////////////////////////////////////////////
汤晓晖 某日 某资源位 各类目的GMV数据 用这个
select
    h1.ds as ds,
    h1.ad_type_new as ad_type_new,
    h1.product_cname1_new as product_cname1_new,
    sum(h1.cck_commission/100) as cck_commission,
    sum(h1.item_price/100) as GMV
from
(
    select
        m1.ds as ds,
        (
        case
        when m1.ad_type in ('special','seckill-tab-hot.productList','seckill-tab-hot.productList*pay_list') then '爆款'
        when m1.ad_type like 'cct-past-product%' then '往期爆款'
        when m1.ad_type like 'seckill-tab%' and m1.ad_type != 'seckill-tab-hot.productList' then '秒杀'
        when m1.ad_type in ('search','searchS','shareSearch') then '搜索'
        when m1.ad_type in ('wxkcategory','category') then '分类'
        when m1.ad_type = 'special-activity' then '活动页'
        when m1.ad_type = 'cct-new-people-buy.productList' then '新人专区'
        when m1.ad_type = '9_cell' then '朋友圈'
        else '其他' end
        ) as ad_type_new,
        (
        case
        when m1.product_cname1 in ('箱包','女装','运动户外','男装','配饰','鞋靴','女士内衣/男士内衣/家居服') then '服饰'
        when m1.product_cname1 in ('家用电器','手机数码') then '家电数码'
        when m1.product_cname1 = '家居百货' then '家居百货'
        when m1.product_cname1 = '美妆个护' then '美妆个护'
        when m1.product_cname1 = '母婴' then '母婴'
        when m1.product_cname1 = '食品' and m1.product_cname2 = '水产肉类/新鲜蔬果/熟食' then '生鲜'
        when m1.product_cname1 = '食品' and m1.product_cname2 in ('零食/坚果/特产','传统滋补营养品','酒水/茶/冲饮','粮油米面/南北干货/调味品','保健食品/膳食营养补充食品') then '食品'
        else '其他' end 
        ) as product_cname1_new,
        m1.cck_commission as cck_commission,
        m1.item_price as item_price
    from
    (
        select
            t1.ds as ds,
            t2.ad_type as ad_type,
            t3.product_cname1 as product_cname1,
            t3.product_cname2 as product_cname2,
            t1.cck_commission as cck_commission,
            t1.item_price as item_price
        from
        (
            select
                s1.ds as ds,
                s1.product_id as product_id,
                s1.third_tradeno as third_tradeno,
                s1.cck_commission as cck_commission,
                s1.item_price as item_price
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
                ds,
                order_sn,
                ad_type
            from  
                origin_common.cc_ods_log_gwapp_order_track_hourly
            where 
                ds >= '${begin_date}'
            and 
                ds <= '${end_date}'
        ) t2
        on t1.third_tradeno = t2.order_sn and t1.ds = t2.ds 
        left join 
        (
            select
                distinct
                product_id,
                product_cname1,
                product_cname2
            from
                data.cc_dw_fs_products_shops
        ) t3
        on t1.product_id = t3.product_id
    ) m1
) h1
group by h1.ds,h1.ad_type_new,h1.product_cname1_new

//////////////////////////////////////////////////////////////////


select
    invite_uidss,
    count(cck_uid) as invite_num
from 
    origin_common.cc_ods_fs_wk_cck_gifts 
where 
   from_unixtime(create_time,'yyyyMMdd') >= 20181222
and
   from_unixtime(create_time,'yyyyMMdd') <= 20181225
and
    product_id = 110020065126
and 
    pay_status = 1
and 
    platform = 14
group by 
    invite_uidss
////////////////////////////////////////////////////////////////////////////////////
昝皓东 某礼包的个人销售数据
select
    t2.invite_uid as invite_uid,
    t1.cck_uid as cck_uid,
    from_unixtime(t1.create_time,'yyyyMMdd') as create_time
from
( 
    select
        cck_uid,
        create_time
    from 
        origin_common.cc_ods_fs_wk_cck_gifts 
    where 
       from_unixtime(create_time,'yyyyMMdd') >= 20181019
    and
       from_unixtime(create_time,'yyyyMMdd') <= 20181021
    and
        product_id = 110020065100
    and 
        pay_status = 1
    and 
        platform = 14
) t1
left join 
(
    select
        cck_uid,
        invite_uid
    from 
        origin_common.cc_ods_fs_wk_cct_layer_info
    where 
        from_unixtime(create_time,'yyyyMMdd') >= 20181019
    and 
        from_unixtime(create_time,'yyyyMMdd') <= 20181021
    and 
        platform = 14 
    and 
        status = 1 
) t2
on t1.cck_uid = t2.cck_uid

////////////////////////////////////////////////////////////////////////////////////
select
    n2.leader_uid,
    count(n1.cck_uid) as invite_num
from
(
    select
        invite_uidss,
        cck_uid
    from 
        origin_common.cc_ods_fs_wk_cck_gifts
    where 
        product_id in (1100185324128,1100185324217)
    and
        platform = 14
    and
        create_time >=1541001600
    and 
        create_time <=1541260799
) n1
left join
(
    select
        distinct
        cck_uid,
        leader_uid
    from
        origin_common.cc_ods_fs_wk_cct_layer_info
    where 
        type =0
    union all 
    select
        distinct
        cck_uid,
        cck_uid as leader_uid
    from
        origin_common.cc_ods_fs_wk_cct_layer_info
    where 
        type =1
) n2
on n1.invite_uidss = n2.cck_uid
group by 
    n2.leader_uid
    
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
select
    n1.invite_uidss,
    sum(if(n1.create_time < n2.leader_ctime,1,0)) as origin_num,
    sum(if(n1.create_time >= n2.leader_ctime,1,0)) as self_num
from
(
    select
        invite_uidss,
        cck_uid,
        create_time
    from 
        origin_common.cc_ods_fs_wk_cck_gifts
    where 
        product_id in (1100185324128,1100185324217)
    and
        platform = 14
    and
        create_time >=1541001600
    and 
        create_time <=1541260799
) n1
inner join 
(
    select
        distinct
        cck_uid,
        leader_ctime as leader_ctime
    from
        origin_common.cc_ods_fs_wk_cct_layer_info
    where 
        type =1 
    and
        platform = 14
    and 
        leader_ctime >= 1541001600
) n2
on n1.invite_uidss = n2.cck_uid
group by n1.invite_uidss 
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 
张文庭 1-3号各个总经理新增vip人数 产生购买的人数 产生分享的人数 下载app人数  
select
    m1.gm_uid as gm_uid,
    m2.real_name,
    m2.phone,
    m1.new_vip_num as new_vip_num,
    m1.buy_vip_num as buy_vip_num,
    m1.vip_load_app_num as vip_load_app_num,
    m1.vip_share_num as vip_share_num 
from  
(
    select
        t1.gm_uid as gm_uid,
        count(t1.cck_uid) as new_vip_num,
        count(t2.cck_uid) as buy_vip_num,
        count(t3.cck_uid) as vip_load_app_num,
        count(t4.cck_uid) as vip_share_num 
    from
    (
        select
            gm_uid,
            cck_uid as cck_uid 
        from
            origin_common.cc_ods_fs_wk_cct_layer_info
        where 
            platform = 14
        and 
            create_time >=1541001600
        and 
            create_time <=1541260799
    ) t1
    left join 
    (
        select
            distinct
            s1.cck_uid as cck_uid
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
        inner join
            origin_common.cc_ods_dwxk_fs_wk_cck_user s2
        on 
            s1.cck_uid=s2.cck_uid
        where 
            s1.ds >= 20181101
        and 
            s2.platform =14 
        and 
            s2.ds = 20181105 
    ) t2
    on t1.cck_uid = t2.cck_uid
    left join 
    (
        select
            n1.cct_uid,
            n2.cck_uid as cck_uid
        from 
        (
            select 
                distinct 
                cct_uid
            from 
                origin_common.cc_ods_log_gwapp_pv_hourly  
            where 
                ds >= 20181101 
            and 
                module='https://app-h5.daweixinke.com/chuchutui/index.html' 
            and 
                cct_uid is not null 
            and 
                app_partner_id = 14
        )n1
        left join 
        (
            select
                distinct
                cck_uid,
                cct_uid
            from 
                origin_common.cc_ods_fs_tui_relation
        )n2
        on n1.cct_uid = n2.cct_uid
    ) t3
    on t1.cck_uid = t3.cck_uid
    left join 
    (
        select
            n1.user_id,
            n2.cck_uid as cck_uid
        from
        (
            select
                distinct
                user_id
            from 
                origin_common.cc_ods_log_cctapp_click_hourly
            where 
                ds >=20181101 
            and 
                module = 'detail_material' 
            and 
                zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC','link_Circle','link_friends','link_copy','small_routine')
        ) n1
        left join 
        (
            select
                distinct
                cck_uid,
                cct_uid
            from 
                origin_common.cc_ods_fs_tui_relation
        )n2
        on n1.user_id = n2.cct_uid
    ) t4
    on t1.cck_uid = t4.cck_uid
    group by 
            t1.gm_uid
) m1
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
        ds = 20181105
) m2
on m1.gm_uid = m2.cck_uid

//////////////////////////////////////////////////////////////////////////////////////////////////////////
张文庭
(
    select
        cck_uid,
        real_name,
        phone
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = 20181204 and phone in ('13858671485','15908256105','13148892091','‭13596925140‬','18769363686','13792155622','18297775362','15822066866','18687967890','13111299826','15207018218','13208167956','13814289455','13072675929','15002093214','13593086991','15636615688','13837827713','15831090891','15515329284','18393387968','18346820044','13644717984','17337786982','18237766946','13524555346','18035911892','15253596569','19990731114','13463163940','18338267633','18830878219','15870842246','15083306916','15881768041','13938719298','13708964002','15148816189','15881707720','13478747183','13953530321','13287092652','15240505283','15080521176','15936444056','18360703511','13462652177','18772993232','15138626372','13668199293','18900540050','13604212759','13523115215','13291980227','13762041084','13730169797','18695524384','15003749965','15564722287','18253471426','15245634685','13358998801','13131226622','15039212163','13039616325','13457517520','13069867429','13730471239','13731029909','13097290511','18970359518','13792207744','18353493370','13583957528','18754956702','15266611916','13944185496','13465499860','13525440446','13833635694','18100326597','15133665630','15304083696','13575027867','13689826549','15981644253','13542499894','18888350958','15953720439','15820466619','18260315830','17615209180','15046939190','13859466032','15533419988','15269873494','13489361428','15037075158','18307029128','18188455322','18702046195','18665011382','18638959881','15670921983','15035379152','15801684088','13717626272','17513098759','15866944548','15176289243','15378190046','15182026551','13833237157','15904233520','17605961407','15373195531','15112568963','13731268291','13641183791','13928438119','15267024171','13203370086','13930635025','18403583120','15318515921','13816041452','13940447238','15963071141','17738539693','15582407230','13936911609','15253596587','18624373950','13683511722','13781197731','13015362299','15178259551','15713053281','15530938338','17608707037','17535414947','15838167889','13315910525','15032451673','13663418843','17712882110','15713770716','18791058996','13262196135','13613451045','15037764071','15010551206','13503265823','15092768027','15088683998','13673416163','15836022737','13833663600','15039058023','13525170745','18601037108','13738435449','13663198696','13738129365','13934427639','15637375417','13700341345','15076945142','15836022737','15738099718','13550688241','18636494348','13400185360','15260986217','15840842610','15866976783','15836022737','13832632584','13256489080','15836019307','18156633796','18754766444','15934178736','18533263531','15836022737','15589302489','‭18296288601‬','13958655701','18772261650','13153574622','15090452710','13935840297','15070260788','17084825162','13133153968','15993030900','18801442778','15565229712','15092715314','13664529246','18990844245','15256082068','13782510432','15169697879','18093639920','18756660602','15035606341','15840417845','13420563818','18048015853','13964170978','18660673826','13133283828','15833441229','15836538830','15070211597','13489261679','13661924917','15878662843','13573324724','13032880761','19977230527','13965558558','15093463893','13645368064','17547434060','15117560853','13969798557','13325495498','15305993390','15848430252','18437317083','13805474716','13082051255','17854943491','13513191558','13959896672','15936860198','15076889330','13088935117','15383707873','13298277808','18740374179','18980211997','13681437479','15100960201','15030623993','13592517522','15094709970','15394267899','17761398369','15614013673','15936851625','13998658631','15254431387','15231649407','15226034470','18698633458','13507432348','18048715410','18620725752','13801209403','15803126128','15615335410','18334917592','15941862436','13850953150','18335791121','13383669066','18817275557','13598660172','18382958425','18545140862','18759960677','13839858453','15254431877','15002449078','15035190635','13817174406','13935663654','13063732737','13241441108','15233892904','18568706671','18765831777','13821816551','13937230557','17316566720','13844730392','15350844443','13209138712','13001806177','15115462907','15825998701','15036603298','13317020995','15020913499','15586393334','13223123740','15830726137','13603484014','15350853228','18697388936','13881718290','13424559228','13273855563','13190136415','13395991103','13467002373','15837422557','18768303092','13835838151','13130819178','13976360205','15937800148','15928879274','14783810994','13845746008','18233037249','13930670627','13755244557','18915792879','13831070221','15822999150','13696929958','13785681456','13091272233','15343163275','13603265816','15059008785','15536184561','18655198635','18773300578')
) n1
left join 
(
    select
        cck_uid,
        if(type=0,'VIP',if(type=1,'总监','总经理')) as type
    from 
        cc_ods_fs_wk_cct_layer_info
    where 
        platform=14
) n2
on n1.cck_uid = n2.cck_uid
left join
(--对t1表进行条件限制评估，加平台=14前为164042，加之后为68386，再加status=1限制，还剩43624，再加is_del = 0还是43624，说明都删除条件影响不大
    select
        t1.invite_uid as invite_uid,
        sum(t2.pay_price/100) as pay_price
    from
    ( 
        select
            distinct
            cck_uid,
            invite_uid
        from 
            origin_common.cc_ods_fs_wk_cct_layer_info
        where 
            from_unixtime(create_time,'yyyyMMdd HH:mm:ss') >= '${begin_date}' 
        and 
            from_unixtime(create_time,'yyyyMMdd HH:mm:ss') <= '${end_date}' 
        and 
            platform = 14 
        and 
            status = 1 
    ) t1
    inner join 
    (--对t2表进行条件限制评估，pay_price=39900，有164042， 加上status = 1仍然是164042，加上pay_status=1还剩158682，说明审核通过不影响，支付状态条件略有影响。
        select
            distinct
            cck_uid,
            pay_price
        from 
            origin_common.cc_ods_dwxk_fs_wk_business_info
        where 
            ds = 20181204
        and 
            from_unixtime(create_time,'yyyyMMdd HH:mm:ss') >= '${begin_date}' 
        and 
            from_unixtime(create_time,'yyyyMMdd HH:mm:ss') <= '${end_date}' 
        and 
            pay_price in (39900,49900)
        and 
            pay_status = 1
    ) t2
    on  t1.cck_uid = t2.cck_uid
    group by 
        t1.invite_uid
) t4 
on t1.cck_uid=t4.invite_uid


select
    a0.cck_uid,
    sum(a0.item_price/100) as item_price
from
(
    select
        cck_uid,
        third_tradeno,
        item_price--支付金额
    from 
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where 
        ds >= '${stat_date}' 
    and 
        ds <= '${end_date}'
    and 
        cck_uid in (1299934,1299956,1299961,1299962,1299959,1299965,1299971,1299968,1299969,1299966,1299973,1299980,1299976,1299975,1299977,1299974,1299979,1299992,1299991,1299982,1299983,1299990,1299988,1299984,1299985,1299997,1300002,1299995,1300001,1299999,1299993,1300005,1300004,1300008,1300011,1300006,1300007,1300018,1300014,1300013,1300019,1300015,1300027,1300026,1300021,1300023,1300022,1300032,1300035,1300036,1300030,1300034,1300037,1300038,1300029,1300028,1300031,1300040,1300045,1300046,1300047,1300043,1300052,1300054,1300055,1300053,1300048,1300056,1300050,1300064,1300065,1300062,1300067,1300060,1300078,1300079,1300076,1300077,1300071,1300075,1300074,1300073,1300082,1300083,1300365,1300366,1300371,1300376,1300367,1300382,1300389,1300384,1300388,1300391,1300392,1300586,1300731,1421277,1421285,1421279,1421280,1421283,1421288,1421282,1421287,1421286,1421284,1421278,1421294,1421296,1421300,1421292,1421291,1421290,1421293,1421295,1421302,1421304,1421301,1421303,1421309,1421307,1421310,1421312,1421316,1421319,1421317,1421320,1421318,1421321,1421327,1421330,1421335,1421337,1421328,1421332,1421339,1421338,1421343,1421340,1421341,1421342,1421585,1421586,1421587,1435091,1435146,1582532,1585085,1585092,1585087,1585095,1585090,1585093,1585091,1585100,1585099,1585109,1585097,1585102,1585106,1585101,1585098,1585117,1585110,1585118,1585119,1585112,1585114,1585111,1585116,1585124,1585127,1585126,1585122,1585120,1585121,1585123,1585129,1585136,1585131,1585130,1585133,1585135,1585250,1585252,1585253,1861968,1861971,1861979,1861975,1861978,1861980,1861982,1861981,1861976,1861977,1861988,1861985,1861987,1861989,1861986,1861992,1861991,1861984,1861994,1861999,1861997,1861995,1861996,1862001,1862006,1862012,1862002,1862005,1862007,1862011,1862009,1862053,1862056,1421311,1300069,1299963,1300063,1618156,1300081,1526838,1300381,1862003,1435092,1300373,1300061,1300012,1435088,1300732,1421313,1585108,1862004,1585113,1300072,1861983,1300070,1421298,1300017,1585134,1421281,1300370,1300024,1300080,1300066,1300372,1421314,1862054,1300041,1862014,1300000,1585088,1300025,1585132,1861972,1585094,1861973,1299970,1585125,1585103,1300057,1421276,1421305,1299967,1300059,1862010,1421584,1299986,1300044,1300085,1421331,1300368,1300385,1421582,1300058,1299964,1299972,1861990,1300378,1300016,1300009,1300049,1300374,1300068,1862013,1299978,1300051,1421333,1300020,1861993,1585089,1300003,1585083,1299994,1421299,1421275,1585105,1862055,1299996,1300390,1300383,1421336,1300039,1862000,1862008,1585254,1421297,1585128,1299987,1421289,1299989,1299998,1585086,1861998,1585251,1585255,1585084,1300606 )
) as a0
inner join
(
    select 
        distinct 
        order_sn
    from  
        origin_common.cc_ods_log_gwapp_order_track_hourly
    where 
        ds >= '${stat_date}' 
    and 
        ds <= '${end_date}'
    and 
        source='cctui'
) as a1
on a0.third_tradeno = a1.order_sn
group by a0.cck_uid


select
    n2.cck_uid as cck_uid
    count(n1.user_id) as num
from
(
    select
        user_id
    from 
        origin_common.cc_ods_log_cctapp_click_hourly
    where 
        ds >= 20181101 
    and 
        ds <= 20181130
    and 
        module = 'detail_material' 
    and 
        zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC','link_Circle','link_friends','link_copy','small_routine')
) n1
left join 
(
    select
        distinct
        cck_uid,
        cct_uid
    from 
        origin_common.cc_ods_fs_tui_relation
)n2
on n1.user_id = n2.cct_uid
where    
    cck_uid in (1299934,1299956,1299961,1299962,1299959,1299965,1299971,1299968,1299969,1299966,1299973,1299980,1299976,1299975,1299977,1299974,1299979,1299992,1299991,1299982,1299983,1299990,1299988,1299984,1299985,1299997,1300002,1299995,1300001,1299999,1299993,1300005,1300004,1300008,1300011,1300006,1300007,1300018,1300014,1300013,1300019,1300015,1300027,1300026,1300021,1300023,1300022,1300032,1300035,1300036,1300030,1300034,1300037,1300038,1300029,1300028,1300031,1300040,1300045,1300046,1300047,1300043,1300052,1300054,1300055,1300053,1300048,1300056,1300050,1300064,1300065,1300062,1300067,1300060,1300078,1300079,1300076,1300077,1300071,1300075,1300074,1300073,1300082,1300083,1300365,1300366,1300371,1300376,1300367,1300382,1300389,1300384,1300388,1300391,1300392,1300586,1300731,1421277,1421285,1421279,1421280,1421283,1421288,1421282,1421287,1421286,1421284,1421278,1421294,1421296,1421300,1421292,1421291,1421290,1421293,1421295,1421302,1421304,1421301,1421303,1421309,1421307,1421310,1421312,1421316,1421319,1421317,1421320,1421318,1421321,1421327,1421330,1421335,1421337,1421328,1421332,1421339,1421338,1421343,1421340,1421341,1421342,1421585,1421586,1421587,1435091,1435146,1582532,1585085,1585092,1585087,1585095,1585090,1585093,1585091,1585100,1585099,1585109,1585097,1585102,1585106,1585101,1585098,1585117,1585110,1585118,1585119,1585112,1585114,1585111,1585116,1585124,1585127,1585126,1585122,1585120,1585121,1585123,1585129,1585136,1585131,1585130,1585133,1585135,1585250,1585252,1585253,1861968,1861971,1861979,1861975,1861978,1861980,1861982,1861981,1861976,1861977,1861988,1861985,1861987,1861989,1861986,1861992,1861991,1861984,1861994,1861999,1861997,1861995,1861996,1862001,1862006,1862012,1862002,1862005,1862007,1862011,1862009,1862053,1862056,1421311,1300069,1299963,1300063,1618156,1300081,1526838,1300381,1862003,1435092,1300373,1300061,1300012,1435088,1300732,1421313,1585108,1862004,1585113,1300072,1861983,1300070,1421298,1300017,1585134,1421281,1300370,1300024,1300080,1300066,1300372,1421314,1862054,1300041,1862014,1300000,1585088,1300025,1585132,1861972,1585094,1861973,1299970,1585125,1585103,1300057,1421276,1421305,1299967,1300059,1862010,1421584,1299986,1300044,1300085,1421331,1300368,1300385,1421582,1300058,1299964,1299972,1861990,1300378,1300016,1300009,1300049,1300374,1300068,1862013,1299978,1300051,1421333,1300020,1861993,1585089,1300003,1585083,1299994,1421299,1421275,1585105,1862055,1299996,1300390,1300383,1421336,1300039,1862000,1862008,1585254,1421297,1585128,1299987,1421289,1299989,1299998,1585086,1861998,1585251,1585255,1585084,1300606 )

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 
select
    m1.gm_uid as gm_uid,
    m2.real_name,
    m2.phone,
    m1.new_vip_num as new_vip_num,
    m1.buy_vip_num as buy_vip_num,
    m1.vip_load_app_num as vip_load_app_num,
    m1.vip_share_num as vip_share_num 
from  
(
    select
        t1.gm_uid as gm_uid,
        count(t1.cck_uid) as new_vip_num,
        count(t2.cck_uid) as buy_vip_num,
        count(t3.cck_uid) as vip_load_app_num,
        count(t4.cck_uid) as vip_share_num 
    from
    (
        select
            gm_uid,
            cck_uid as cck_uid 
        from
            origin_common.cc_ods_fs_wk_cct_layer_info
        where 
            platform = 14
        and 
            create_time >=1541260800
        and 
            create_time <=1542211200
    ) t1
    left join 
    (
        select
            distinct
            s1.cck_uid as cck_uid
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
        inner join
            origin_common.cc_ods_dwxk_fs_wk_cck_user s2
        on 
            s1.cck_uid=s2.cck_uid
        where 
            s1.ds >= 20181104
        and
            s1.ds >= 20181115
        and 
            s2.platform =14 
        and 
            s2.ds = 20181115 
    ) t2
    on t1.cck_uid = t2.cck_uid
    left join 
    (
        select
            n1.cct_uid,
            n2.cck_uid as cck_uid
        from 
        (
            select 
                distinct 
                cct_uid
            from 
                origin_common.cc_ods_log_gwapp_pv_hourly  
            where 
                ds >= 20181104 
            and
                ds <= 20181115
            and 
                module='https://app-h5.daweixinke.com/chuchutui/index.html' 
            and 
                cct_uid is not null 
            and 
                app_partner_id = 14
        )n1
        left join 
        (
            select
                distinct
                cck_uid,
                cct_uid
            from 
                origin_common.cc_ods_fs_tui_relation
        )n2
        on n1.cct_uid = n2.cct_uid
    ) t3
    on t1.cck_uid = t3.cck_uid
    left join 
    (
        select
            n1.user_id,
            n2.cck_uid as cck_uid
        from
        (
            select
                distinct
                user_id
            from 
                origin_common.cc_ods_log_cctapp_click_hourly
            where 
                ds >= 20181104 
            and
                ds <= 20181115
            and
                module = 'detail_material' 
            and 
                zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC','link_Circle','link_friends','link_copy','small_routine')
        ) n1
        left join 
        (
            select
                distinct
                cck_uid,
                cct_uid
            from 
                origin_common.cc_ods_fs_tui_relation
        )n2
        on n1.user_id = n2.cct_uid
    ) t4
    on t1.cck_uid = t4.cck_uid
    group by 
            t1.gm_uid
) m1
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
        ds = 20181115
) m2
on m1.gm_uid = m2.cck_uid
/////////////////////////////////////////////////////////////////////////////////////////////   
select
    m1.gm_uid as gm_uid,
    m2.real_name,
    m2.phone,
    m1.team_vip_num as team_vip_num,
    m1.buy_vip_num as buy_vip_num,
    m1.sales_volume as sales_volume,
    m1.vip_load_app_num as vip_load_app_num,
    m1.vip_share_num as vip_share_num 
from  
(
    select
        t1.gm_uid as gm_uid,
        count(t1.cck_uid) as team_vip_num,
        count(t2.cck_uid) as buy_vip_num,
        sum(t2.pay_fee)   as sales_volume,
        count(t3.cck_uid) as vip_load_app_num,
        count(t4.cck_uid) as vip_share_num 
    from
    (
        select
            distinct
            gm_uid,
            cck_uid as cck_uid 
        from
            origin_common.cc_ods_fs_wk_cct_layer_info
        where 
            platform = 14
        and 
            create_time <= 1541606400
        union all
        select
            distinct
            gm_uid,
            gm_uid as cck_uid 
        from
            origin_common.cc_ods_fs_wk_cct_layer_info
        where 
            platform = 14
    ) t1
    left join 
    (
        select
            s1.cck_uid as cck_uid,
            sum(s1.item_price/100) as pay_fee
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
        inner join
            origin_common.cc_ods_dwxk_fs_wk_cck_user s2
        on 
            s1.cck_uid=s2.cck_uid
        where 
            s1.ds >= 20181101
        and 
            s1.ds <= 20181108
        and
            s2.platform =14 
        and 
            s2.ds = 20181115
        group by 
            s1.cck_uid 
    ) t2
    on t1.cck_uid = t2.cck_uid
    left join 
    (
        select
            n1.cct_uid,
            n2.cck_uid as cck_uid
        from 
        (
            select 
                distinct 
                cct_uid
            from 
                origin_common.cc_ods_log_gwapp_pv_hourly  
            where 
                ds >= 20181101 
            and
                ds <= 20181108 
            and 
                module='https://app-h5.daweixinke.com/chuchutui/index.html' 
            and 
                cct_uid is not null 
            and 
                app_partner_id = 14
        ) n1
        left join 
        (
            select
                distinct
                cck_uid,
                cct_uid
            from 
                origin_common.cc_ods_fs_tui_relation
        ) n2
        on n1.cct_uid = n2.cct_uid
    ) t3
    on t1.cck_uid = t3.cck_uid
    left join 
    (
        select
            n1.user_id,
            n2.cck_uid as cck_uid
        from
        (
            select
                distinct
                user_id
            from 
                origin_common.cc_ods_log_cctapp_click_hourly
            where 
                ds >= 20181101 
            and
                ds <= 20181108 
            and 
                module = 'detail_material' 
            and 
                zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC','link_Circle','link_friends','link_copy','small_routine')
        ) n1
        left join 
        (
            select
                distinct
                cck_uid,
                cct_uid
            from 
                origin_common.cc_ods_fs_tui_relation
        ) n2
        on n1.user_id = n2.cct_uid
    ) t4
    on t1.cck_uid = t4.cck_uid
    group by 
            t1.gm_uid
) m1
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
        ds = 20181115
) m2
on m1.gm_uid = m2.cck_uid
///////////////////////////////////////////////////////////////////////////////////////////////////////////
张梨娜 某日某商品 销量最快到达5件的楚客 到达5件的时间 最终销量和最终支付金额 楚客信息 
select
    t1.cck_uid as cck_uid,
    t3.cct_uid as cct_uid,
    t2.real_name as real_name,
    t2.phone as phone,
    t1.create_time as create_time,
    t1.total_sales_num as total_sales_num,
    t1.total_fee as total_fee
from
(
    select
        distinct
        n1.cck_uid,
        from_unixtime(n1.create_time,'yyyyMMdd HH:mm:ss') as create_time,
        n1.total_sales_num,
        n1.total_fee
    from
    (
        select
            t1.cck_uid as cck_uid,
            min(t1.create_time) as create_time,
            max(t1.sales_num) as total_sales_num,
            max(t1.pay_fee) as total_fee
        from
        (
            select
                s1.cck_uid,
                s1.sale_num,
                s1.item_price,
                s1.create_time,
                sum(s1.sale_num) over (partition by s1.cck_uid order by s1.create_time) as sales_num,
                sum(s1.item_price/100) over (partition by s1.cck_uid order by s1.create_time) as pay_fee
            from 
                origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
            inner join
                origin_common.cc_ods_dwxk_fs_wk_cck_user s2
            on 
                s1.cck_uid=s2.cck_uid
            where 
                s1.ds = 20181105
            and 
                s1.product_id = 1100185323991
            and 
                s2.platform =14 
            and 
                s2.ds = 20181105 
        ) t1
        where 
            t1.sales_num >=5
        group by 
            t1.cck_uid
    ) n1
    order by 
        create_time
    limit 200
) t1
left join 
(
    select
        distinct
        cck_uid,
        real_name,
        phone
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info_realtime
    where 
        ds = 20181106
) t2
on t1.cck_uid = t2.cck_uid
left join 
(
    select
        distinct
        cck_uid,
        cct_uid
    from 
        origin_common.cc_ods_fs_tui_relation
) t3 
on t1.cck_uid = t3.cck_uid
/////////////////////////////////////////////////
涛哥 亚当dau 代码
select
    '{date_sign}' as date_sign,
    case
      when s.cck_vip_status = 0 and s.cck_vip_level = 0 then 0
      when s.cck_vip_status = 0 and s.cck_vip_level = 1 then 2
    else 1 end as source,
    s.dau
from
(
    select
        m1.cck_vip_status,
        m1.cck_vip_level,
        count(distinct m1.cct_uid) as dau
    from
    (
        select
            t1.cct_uid,
            t2.cck_vip_status,
            if(t2.cck_vip_status = 1, -1, t2.cck_vip_level) as cck_vip_level
        from
        (
            select 
                cct_uid 
            from 
                origin_common.cc_ods_log_gwapp_pv_hourly  
            where 
                ds = '{bizdate}' 
            and 
                app_partner_id=14  
            and 
                module  = 'https://app-h5.daweixinke.com/chuchutui/index.html'
        ) t1
        join
        (
            select 
                cct_uid, 
                cck_vip_status, 
                cck_vip_level 
            from origin_common.cc_ods_fs_tui_relation
        ) t2
        on
           t1.cct_uid = t2.cct_uid
    ) m1
    group by
        m1.cck_vip_status, m1.cck_vip_level
) s

////////////////////////////////////////////////////////////////////////////////////////////
应夏毅  双十一 实时 商品类目的产出
select
    t3.product_cname1_new      as product_cname1,
    count(distinct t1.third_tradeno)    as order_count, --订单数
    sum(t1.sale_num)           as sales_num,  --销量
    sum(t1.item_price/100)     as item_price,  --支付金额
    sum(t1.cck_commission/100) as cck_commission --佣金
from
(
    select
        distinct
        S1.product_id as product_id,
        s1.product_sku_id,
        s1.third_tradeno as third_tradeno,
        s1.sale_num as sale_num,
        s1.cck_commission as cck_commission,
        s1.item_price as item_price
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_realtime s1
    join 
        origin_common.cc_ods_dwxk_fs_wk_cck_user s2
    on 
        s1.cck_uid = s2.cck_uid
    where 
        s1.ds = 20181109
    and
        s1.create_time >=1541692800
    and 
        s2.ds = 20181108
    and
        s2.platform = 14
) t1
inner join 
(
    select
        m1.product_id as product_id,
        (
        case
        when m1.product_cname1 in ('箱包','女装','运动户外','男装','配饰','鞋靴','女士内衣/男士内衣/家居服') then '服饰'
        when m1.product_cname1 in ('家用电器','手机数码') then '家电数码'
        when m1.product_cname1 = '家居百货' then '家居百货'
        when m1.product_cname1 = '美妆个护' then '美妆个护'
        when m1.product_cname1 = '母婴' then '母婴'
        when m1.product_cname1 = '食品' and m1.product_cname2 = '水产肉类/新鲜蔬果/熟食' then '生鲜'
        when m1.product_cname1 = '食品' and m1.product_cname2 in ('零食/坚果/特产','传统滋补营养品','酒水/茶/冲饮','粮油米面/南北干货/调味品','保健食品/膳食营养补充食品') then '食品'
        else '其他' end 
        ) as product_cname1_new
    from
    (

        select
            distinct
            product_id,
            product_cname1,
            product_cname2
        from
            data.cc_dw_fs_products_shops
        where
        shop_id in (18944,19125,18911,18532,18860,18722,18281,18110,18212,18611,18323,18714,18678,18664,18683,18649,18686,18709,18685,18708,18690,18676,18277,18693,18692,18608,18674,18537,18673,18684,18398,18633,18472,18447,9565,18575,18514,18640,18625,18590,18374,18595,18576,18494,18533,18579,18546,18510,18547,18518,18404,12502,18471,18526,14661,2776,18516,18501,18467,18505,18243,18502,18492,18901,19127,18878,19141,14560,17114,16907,10338,17200,18065,15279,18217,17815,16315,13278,18196,17624,17648,18226,17684,18224,17839,17697,18007,16439,17218,8036,17831,17582,10668,17698,17699,17819,17820,17686,14359,17461,17888,954,15801,18117,18197,17726,17947,9601,17692,18255,17884,18309,8839,11760,18002,14515,11677,17428,1374,17957,9241,5529,17944,16717,17896,12929,8032,17500,14975,13352,16819,18241,16814,18304,11184,13896,16530,18285,18142,4128,8106,17705,5138,8089,5666,18091,16510,9151,8481,7479,533,17690,9238,16270,16567,6521,18330,1796,17704,17769,16561,4121,17653,17951,17531,10078,15811,10141,18417,11548,17850,12922,13773,10036,17902,16798,17851,17649,16743,9772,17982,17597,8270,7992,17405,8622,18317,18238,496,9912,13879,17315,17950,2995,18382,17005,17920,10878,17913,18161,18335,16298,9665,17757,18385,18133,11646,14502,266,16785,10639,168,14390,6621,12545,18174,17776,18145,16540,18338,17665,17788,16355,12846,16538,9950,12854,17961,7199,16853,16741,16286,16896,11137,17167,16687,12924,17349,17981,17645,16878,17707,16375,16581,8756,17768,17644,17939,17137,17441,17522,11102,18164,17693,18080,17926,18123,17801,18210,11279,17483,3133,17748,18430,17501,15519,10097,18060,13805,6427,18294,18394,12889,9961,11449,8553,9691,14005,15688,17172,17152,11445,17012,11237,7647,10671,9570,16650,3826,11242,18103,17337,965,17490,18035,11845,182,9342,18937,19171,18963,19250,19268,19277,18288,19284,19190,17356,19137,19347,19331,17931,19370,19211,18112,19411,18907,19412,18158,18475,19391,18917,18250,19344,19561,19585,19560,18769,19475,19507,19497,17702,19434,19633,19636,19700,16531,12964,3533,19732,18719,8339,18927,19697,19908,19766,19570,18682,19496,12704,19330,19759,19767,19884,20233,7780,20201,19426,18179,20243,10064,19698,20294,20260,20325,20350,20003,19230,11982,17439,19600,19313,20342,20166,19595,20335,18415,18432,11336,18495,19210,17485,20393,19574,18873,19209,20431,653,17300,17530,17516,20205,20351,20401,19236,19528,20481,20399,20458,19491,11568,20413,20499,19500,20471,19866,20427,20493,20443,20546,20500,3646,4457,17115,18871,20492,20506,19139,19651,20505,20452,20541,20539,19333,20545,20285,20503,9974,20573,19384,18647,18176,20411,20534,18552,11520,20601,18744,20449,20336,20537,19687,18512,20604,20007,19338,20327,19675,20484,17780,20567,20614,20648,20675,20676,19596,20615,20619,20577,20579,20566,20593,20618,20657,20689,20533,20719,20764,20664,20757,20754,20812,20558,20916,20923,20939,20937,20806,20524,20965,18053,8935,20860,20931,20751,17297,22361,13589,20940,18662,18729,18588,18740,18799,19319,19405,19402,20216,18965,20696)
    ) m1 
) t3
on t1.product_id = t3.product_id
group by t3.product_cname1_new
////////////////////////////////////////////////////////////////
应夏毅 双十一 实时 美妆个护 商品的产出
select
    t1.product_id as product_id,
    t3.product_cname1_new      as product_cname1,
    count(distinct t1.third_tradeno)    as order_count, --订单数
    sum(t1.sale_num)           as sales_num,  --销量
    sum(t1.item_price/100)     as item_price,  --支付金额
    sum(t1.cck_commission/100) as cck_commission --佣金
from
(
    select
        distinct
        S1.product_id as product_id,
        s1.product_sku_id,
        s1.third_tradeno as third_tradeno,
        s1.sale_num as sale_num,
        s1.cck_commission as cck_commission,
        s1.item_price as item_price
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_realtime s1
    join 
        origin_common.cc_ods_dwxk_fs_wk_cck_user s2
    on 
        s1.cck_uid = s2.cck_uid
    where 
        s1.ds = 20181109
    and
        s1.create_time >=1541692800
    and 
        s2.ds = 20181108
    and
        s2.platform = 14
) t1
inner join 
(
    select
        m1.product_id as product_id,
        (
        case
        when m1.product_cname1 in ('箱包','女装','运动户外','男装','配饰','鞋靴','女士内衣/男士内衣/家居服') then '服饰'
        when m1.product_cname1 in ('家用电器','手机数码') then '家电数码'
        when m1.product_cname1 = '家居百货' then '家居百货'
        when m1.product_cname1 = '美妆个护' then '美妆个护'
        when m1.product_cname1 = '母婴' then '母婴'
        when m1.product_cname1 = '食品' and m1.product_cname2 = '水产肉类/新鲜蔬果/熟食' then '生鲜'
        when m1.product_cname1 = '食品' and m1.product_cname2 in ('零食/坚果/特产','传统滋补营养品','酒水/茶/冲饮','粮油米面/南北干货/调味品','保健食品/膳食营养补充食品') then '食品'
        else '其他' end 
        ) as product_cname1_new
    from
    (

        select
            distinct
            product_id,
            product_cname1,
            product_cname2
        from
            data.cc_dw_fs_products_shops
        where
        shop_id in (18944,19125,18911,18532,18860,18722,18281,18110,18212,18611,18323,18714,18678,18664,18683,18649,18686,18709,18685,18708,18690,18676,18277,18693,18692,18608,18674,18537,18673,18684,18398,18633,18472,18447,9565,18575,18514,18640,18625,18590,18374,18595,18576,18494,18533,18579,18546,18510,18547,18518,18404,12502,18471,18526,14661,2776,18516,18501,18467,18505,18243,18502,18492,18901,19127,18878,19141,14560,17114,16907,10338,17200,18065,15279,18217,17815,16315,13278,18196,17624,17648,18226,17684,18224,17839,17697,18007,16439,17218,8036,17831,17582,10668,17698,17699,17819,17820,17686,14359,17461,17888,954,15801,18117,18197,17726,17947,9601,17692,18255,17884,18309,8839,11760,18002,14515,11677,17428,1374,17957,9241,5529,17944,16717,17896,12929,8032,17500,14975,13352,16819,18241,16814,18304,11184,13896,16530,18285,18142,4128,8106,17705,5138,8089,5666,18091,16510,9151,8481,7479,533,17690,9238,16270,16567,6521,18330,1796,17704,17769,16561,4121,17653,17951,17531,10078,15811,10141,18417,11548,17850,12922,13773,10036,17902,16798,17851,17649,16743,9772,17982,17597,8270,7992,17405,8622,18317,18238,496,9912,13879,17315,17950,2995,18382,17005,17920,10878,17913,18161,18335,16298,9665,17757,18385,18133,11646,14502,266,16785,10639,168,14390,6621,12545,18174,17776,18145,16540,18338,17665,17788,16355,12846,16538,9950,12854,17961,7199,16853,16741,16286,16896,11137,17167,16687,12924,17349,17981,17645,16878,17707,16375,16581,8756,17768,17644,17939,17137,17441,17522,11102,18164,17693,18080,17926,18123,17801,18210,11279,17483,3133,17748,18430,17501,15519,10097,18060,13805,6427,18294,18394,12889,9961,11449,8553,9691,14005,15688,17172,17152,11445,17012,11237,7647,10671,9570,16650,3826,11242,18103,17337,965,17490,18035,11845,182,9342,18937,19171,18963,19250,19268,19277,18288,19284,19190,17356,19137,19347,19331,17931,19370,19211,18112,19411,18907,19412,18158,18475,19391,18917,18250,19344,19561,19585,19560,18769,19475,19507,19497,17702,19434,19633,19636,19700,16531,12964,3533,19732,18719,8339,18927,19697,19908,19766,19570,18682,19496,12704,19330,19759,19767,19884,20233,7780,20201,19426,18179,20243,10064,19698,20294,20260,20325,20350,20003,19230,11982,17439,19600,19313,20342,20166,19595,20335,18415,18432,11336,18495,19210,17485,20393,19574,18873,19209,20431,653,17300,17530,17516,20205,20351,20401,19236,19528,20481,20399,20458,19491,11568,20413,20499,19500,20471,19866,20427,20493,20443,20546,20500,3646,4457,17115,18871,20492,20506,19139,19651,20505,20452,20541,20539,19333,20545,20285,20503,9974,20573,19384,18647,18176,20411,20534,18552,11520,20601,18744,20449,20336,20537,19687,18512,20604,20007,19338,20327,19675,20484,17780,20567,20614,20648,20675,20676,19596,20615,20619,20577,20579,20566,20593,20618,20657,20689,20533,20719,20764,20664,20757,20754,20812,20558,20916,20923,20939,20937,20806,20524,20965,18053,8935,20860,20931,20751,17297,22361,13589,20940,18662,18729,18588,18740,18799,19319,19405,19402,20216,18965,20696)
    ) m1 
) t3
on t1.product_id = t3.product_id
where t3.product_cname1_new = '美妆个护'
group by 
    t1.product_id,t3.product_cname1_new
///////////////////////////////////////////////////////////////////////////
张文庭  9日 vip 销售数据 及归属信息
select
    n1.cck_uid        as cck_uid,
    n4.real_name      as cck_name,
    n4.phone          as cck_phone,
    n3.gm_uid         as gm_uid,
    n6.real_name      as gm_name,
    n6.phone          as gm_phone,
    n1.order_count    as order_count, --订单数
    n1.sales_num      as sales_num,  --销量
    n1.item_price     as item_price,  --支付金额
    n1.cck_commission as cck_commission --佣金
from
(
    select
        s1.cck_uid as cck_uid,
        count(distinct s1.third_tradeno)    as order_count, --订单数
        sum(s1.sale_num)           as sales_num,  --销量
        sum(s1.item_price/100)     as item_price,  --支付金额
        sum(s1.cck_commission/100) as cck_commission --佣金
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_realtime s1
    join 
        origin_common.cc_ods_dwxk_fs_wk_cck_user s2
    on 
        s1.cck_uid = s2.cck_uid
    where 
        s1.ds = 20181109
    and
        s1.create_time >=1541692800
    and
        s1.product_id = 110019268200
    and 
        s2.ds = 20181108
    and
        s2.platform = 14
    group by 
        s1.cck_uid
) n1 
left join 
(   
    select
        distinct
        cck_uid,
        gm_uid
    from
        origin_common.cc_ods_fs_wk_cct_layer_info
    union distinct
    select
        distinct
        gm_uid as cck_uid,
        gm_uid
    from
        origin_common.cc_ods_fs_wk_cct_layer_info
) n3
on n1.cck_uid = n3.cck_uid
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
        ds = 20181108
) n4 
on n1.cck_uid = n4.cck_uid
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
        ds = 20181108
) n6
on n3.gm_uid = n6.cck_uid
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
张文庭 每个总经理 团队人数 9日 团队 有分享次数 分享人数
select
    m1.gm_uid as gm_uid,
    m2.real_name,
    m2.phone,
    m1.team_vip_num as team_vip_num,
    m1.share_num as share_num,
    m1.share_vip_num as share_vip_num
from  
(
    select
        t1.gm_uid as gm_uid,
        count(distinct t1.cck_uid) as team_vip_num,
        count(t4.cck_uid) as share_num,
        count(distinct t4.cck_uid) as share_vip_num  
    from
    (
        select
            distinct
            cck_uid,
            gm_uid
        from
            origin_common.cc_ods_fs_wk_cct_layer_info
        where 
            platform = 14
        union all
        select
            distinct
            gm_uid as cck_uid,
            gm_uid
        from
            origin_common.cc_ods_fs_wk_cct_layer_info
        where 
            platform = 14
    ) t1
    left join 
    (
        select
            n1.user_id,
            n2.cck_uid as cck_uid
        from
        (
            select
                user_id
            from 
                origin_common.cc_ods_log_cctapp_click_hourly
            where 
                ds =20181109 
            and 
                module = 'detail_material' 
            and 
                zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC','link_Circle','link_friends','link_copy','small_routine')
            and 
                source in ('cct','cctui')
        ) n1
        left join 
        (
            select
                distinct
                cck_uid,
                cct_uid
            from 
                origin_common.cc_ods_fs_tui_relation
        )n2
        on n1.user_id = n2.cct_uid
    ) t4
    on t1.cck_uid = t4.cck_uid
    group by 
            t1.gm_uid
) m1
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
        ds = 20181108
) m2
on m1.gm_uid = m2.cck_uid
///////////////////////////////////////////////////////////////
廖志刚  大西北产出
select 
    c5.product_id as product_id, 
    c5.order_sn as order_sn,
    c5.item_price as item_price,
    c5.cck_commission as cck_commission
from
(
    select 
        cck_uid,
        product_id,
        third_tradeno as order_sn,
        (item_price/100) as item_price,
        (cck_commission/100) as cck_commission
    from 
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where 
        ds >= 20181109
    and 
        ds <= 20181111
) c5
join
(
    select 
        cck_uid
    from 
        origin_common.cc_ods_dwxk_fs_wk_cck_user
    where 
        ds = 20181111
    and 
        platform = 14 
) c6 
on c5.cck_uid = c6.cck_uid
join
(
    select 
        order_sn
    from 
        origin_common.cc_ods_log_gwapp_order_track_hourly
    where 
        ds >= 20181109
    and 
        ds <= 20181111
    and 
        ad_material_id = 52859
) c7 
on c5.order_sn = c7.order_sn
//////////////////////////////////////////////////////////////////////////
select
    c5.cck_uid,
    c6.real_name,
    c6.phone,
    c5.product_id,
    c5.third_tradeno as order_sn,
    c5.item_price,
    c5.cck_commission
from 
(
    select 
        cck_uid,
        product_id,
        third_tradeno,
        (item_price/100) as item_price,
        (cck_commission/100) as cck_commission
    from 
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where 
        ds >= 20181120
    and 
        ds <= 20181126
    and 
        third_tradeno in ('181123211641rcl476837','181123210728pXn326837','181123210502meH456837','181123210502C6E356837','181123100815khq326837','181121192227C3w320341')
) c5
left join 
(
    select
        cck_uid,
        real_name,
        phone
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = 20181127
) c6
on c5.cck_uid =c6.cck_uid
/////////////////////////////////////////////////////////////////////
廖志刚 大西北afp 页面pv uv 页面产出 页面分享人数
select

from
(
    SELECT
        ds,
        count(user_id) as pv,
        count(distinct user_id) as uv
    from 
        cc_ods_log_cctapp_click_hourly
    WHERE
        ds >= 20181109 
    and 
        ds <= 20181111
    and 
        ad_material_id = 52859
    and 
        module = 'detail_app'
    and 
        source in ('cct','cctui')
) a1
left join 
(
    select 
        c1.ds,
        count(*) as page_share_cnt,
        count(distinct c1.user_id) as page_share_user_cnt
    from
    (
        select 
            ds,
            hash_value,
            app_flag,
            user_id
        from 
            cc_ods_log_gwapp_click_hourly
        where 
            ds >= '${bizdate}'
            and
            ds <= '${bizdatehh}'
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
            cc_ods_fs_gwapp_hash_track_hourly
    ) c2 
    on c1.hash_value = c2.hash_value
    where split(c2.track, ':_:')[1]=52859
    group by  
        c1.ds
) a2
on a1.ds = a2.ds 
left join 
(
    select 
        c5.ds as ds,
        count(c5.order_sn)      as order_count, --订单数
        sum(c5.sale_num)        as sales_num,  --销量
        sum(c5.item_price)      as item_price,  --支付金额
        sum(c5.cck_commission)  as cck_commission --佣金
    from
    (
        select 
            ds,
            cck_uid,
            product_id,
            third_tradeno as order_sn,
            (item_price/100) as item_price,
            sale_num as sale_num,
            (cck_commission/100) as cck_commission
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where 
            ds >= 20181109
        and 
            ds <= 20181111
    ) c5
    join
    (
        select 
            cck_uid
        from 
            origin_common.cc_ods_dwxk_fs_wk_cck_user
        where 
            ds = 20181111
        and 
            platform = 14 
    ) c6 
    on c5.cck_uid = c6.cck_uid
    join
    (
        select 
            order_sn
        from 
            origin_common.cc_ods_log_gwapp_order_track_hourly
        where 
            ds >= 20181109
        and 
            ds <= 20181111
        and 
            ad_material_id = 52859
    ) c7 
    on c5.order_sn = c7.order_sn
    group by c5.ds
) a3

///////////////////////////////////////////////////////////////////
张文庭 某段时间 vip 邀请人数统计 
select
    a1.invite_uid as cck_uid,
    a2.real_name as real_name,
    a2.phone as phone,
    a3.type as type,
    from_unixtime(a3.create_time,'yyyyMMdd HH:mm:ss') as create_time,
    a6.leader_uid,
    a4.gm_uid as gm_uid,
    a5.real_name as gm_name,
    a1.invite_num
from
(
    select
        invite_uid,
        count(cck_uid) as invite_num
    from 
        origin_common.cc_ods_fs_wk_cct_layer_info
    where 
        from_unixtime(create_time,'yyyyMMdd HH:mm:ss') >= '${begin_date}' 
    and 
        from_unixtime(create_time,'yyyyMMdd HH:mm:ss') <= '${end_date}' 
    and 
        platform = 14 
    and 
        status = 1 
    group by 
        invite_uid
) a1
left join
(
    select
        cck_uid,
        real_name,
        phone
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = 20181111
) a2
on a1.invite_uid = a2.cck_uid
left join 
(
    select
        cck_uid,
        type,
        create_time
    from
        origin_common.cc_ods_fs_wk_cct_layer_info
    where 
        platform = 14
) a3
on a1.invite_uid = a3.cck_uid
left join
(
    select
        distinct
        cck_uid,
        leader_uid
    from
        origin_common.cc_ods_fs_wk_cct_layer_info
    where 
        platform = 14
) a6
on a1.invite_uid = a6.cck_uid
left join
(
    select
        distinct
        cck_uid,
        gm_uid
    from
        origin_common.cc_ods_fs_wk_cct_layer_info
    where 
        platform = 14
) a4
on a1.invite_uid = a4.cck_uid
left join
(
    select
        cck_uid,
        real_name,
        phone
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = 20181111
) a5
on a4.gm_uid = a5.cck_uid
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
崔丹  近两月成交商品 的 商品信息
select
    a1.product_id,
    a2.product_title,
    a2.product_cname1,
    a2.product_cname2,
    a2.product_cname3,
    a2.shop_id,
    a2.shop_title,
    a1.order_count,
    a1.sales_num,
    a1.item_price,
    a1.cck_commission
from
(
    select
        s1.product_id as product_id,
        count(distinct s1.third_tradeno)    as order_count, --订单数
        sum(s1.sale_num)           as sales_num,  --销量
        sum(s1.item_price/100)     as item_price,  --支付金额
        sum(s1.cck_commission/100) as cck_commission --佣金
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
    join 
        origin_common.cc_ods_dwxk_fs_wk_cck_user s2
    on 
        s1.cck_uid = s2.cck_uid
    where 
        s1.ds >= 20180912
    and 
        s1.ds <= 20181112
    and 
        s2.ds = 20180912
    and
        s2.platform = 14
    group by 
        s1.product_id
) a1
left join 
(
    select
        product_id,
        product_title,
        product_cname1,
        product_cname2,
        product_cname3,
        shop_id,
        shop_title
    from
        data.cc_dw_fs_products_shops
) a2
on a1.product_id = a2.product_id
////////////////////////////////////////////////////////////////////////////////////////////////////////
翔哥 某日某商品 服务经理维度销售数据 
select
    a1.gm_uid,
    a2.real_name,
    a2.phone,
    a1.pay_count,
    a1.fee
from
(
    select
        t2.gm_uid as gm_uid,
        sum(t1.pay_count) as pay_count,
        sum(t1.fee)  as fee
    from
    (
        select
            s1.cck_uid as cck_uid,
            count(distinct s1.third_tradeno) as pay_count,
            sum(s1.item_price/100) as fee
        from
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
        join
            origin_common.cc_ods_dwxk_fs_wk_cck_user s2
        on 
            s1.cck_uid = s2.cck_uid
        where
            s1.ds = 20181109
        and 
            s1.product_id = 110019268200
        and
            s2.platform = 14
        and
            s2.ds = 20181113
        group by
            s1.cck_uid
    ) t1
    left join 
    (
        select
            distinct
            cck_uid,
            gm_uid
        from
            origin_common.cc_ods_fs_wk_cct_layer_info
        union all
        select
            distinct
            gm_uid as cck_uid,
            gm_uid
        from
            origin_common.cc_ods_fs_wk_cct_layer_info
    ) t2
    on t1.cck_uid = t2.cck_uid
    group by t2.gm_uid
) a1 
left join 
(
    select
        cck_uid,
        real_name,
        phone
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = 20181113
) a2 
on a1.gm_uid = a2.cck_uid

/////////////////////////////////////////////////////////////////////////////////////
廖宁 某日大盘分时销售数据
select
    t1.hour                          as hour,--小时
    count(distinct t1.third_tradeno) as order_count,
    sum(t1.sale_num)                 as sales_num,
    sum(t1.item_price/100)           as item_price--支付金额
from
(
    select
        from_unixtime(s1.create_time,'yyyyMMdd HH')  as hour,
        s1.third_tradeno as third_tradeno,
        S1.sale_num      as sale_num,
        s1.item_price    as item_price
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
    join
        origin_common.cc_ods_dwxk_fs_wk_cck_user s2
    on 
        s1.cck_uid=s2.cck_uid
    where
        s1.ds >= 20181109
    and
        s1.ds <= 20181111
    and
        s2.platform = 14
    and
        s2.ds = 20181113
) t1
group by 
    t1.hour
/////////////////////////////////////////////////////////////////////////////////////////
廖宁 30商品 某日 分时销售数据
select
    t1.hour                          as hour,--小时
    count(distinct t1.third_tradeno) as order_count,
    sum(t1.sale_num)                 as sales_num,
    sum(t1.item_price/100)           as item_price--支付金额
from
(
    select
        from_unixtime(s1.create_time,'yyyyMMdd HH')  as hour,
        s1.third_tradeno as third_tradeno,
        S1.sale_num      as sale_num,
        s1.item_price    as item_price
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
    join
        origin_common.cc_ods_dwxk_fs_wk_cck_user s2
    on 
        s1.cck_uid=s2.cck_uid
    where
        s1.ds = 20181111
    and 
        s1.product_id in (110019268200,1100209232,1100185323334,1100224632,11001254561,1100185324081,1100224521,110019405533,1100185323984,110018164617,1100185324278,110019405542,1100185324282,110019405900,110019268246,110012964921,11002020181,11001851661,1100185322424,11002047111,10010671208,1100185323699,11002052457,11001976633,11002039240,1100185322977,1100185324013,1100185322974,10014515206,110020499126,1100185324226)
    and
        s2.platform = 14
    and
        s2.ds = 20181113
) t1
group by 
    t1.hour
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
廖宁 30商品 新老楚客占比
select
    sum(if(m1.create_time >= 1541001600,1,0)) as new_vip_num,
    sum(if(m1.create_time <  1541001600,1,0)) as old_vip_num
from
(
     select
        t1.cck_uid as cck_uid,
        t2.create_time as create_time 
    from
    (
        select
            distinct
            s1.cck_uid
        from
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
        join
            origin_common.cc_ods_dwxk_fs_wk_cck_user s2
        on 
            s1.cck_uid=s2.cck_uid
        where
            s1.ds = 20181110
        and 
            s1.product_id in (110019268200,1100209232,1100185323334,1100224632,11001254561,1100185324081,1100224521,110019405533,1100185323984,110018164617,1100185324278,110019405542,1100185324282,110019405900,110019268246,110012964921,11002020181,11001851661,1100185322424,11002047111,10010671208,1100185323699,11002052457,11001976633,11002039240,1100185322977,1100185324013,1100185322974,10014515206,110020499126,1100185324226)
        and
            s2.platform = 14
        and
            s2.ds = 20181113
    ) t1
    left join 
    (
        select
            distinct
            cck_uid,
            create_time
        from
            origin_common.cc_ods_fs_wk_cct_layer_info
    ) t2
    on t1.cck_uid = t2.cck_uid
) m1
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
汤晓晖 类目 毛利额 gmv 毛利率
select
    h1.ds as ds,
    h1.ad_type_new as ad_type_new,
    h1.product_cname1_new as product_cname1_new,
    sum(h1.pb_price) as origin_price,
    sum(h1.commission) as commission,
    sum(h1.product_coupon) as product_coupon,
    sum(h1.item_price) as item_price,
    sum(h1.gross_profit) as gross_profit,
    sum(h1.gross_profit)/(sum(h1.item_price)+sum(h1.product_coupon)) as gross_profit_rate
from
(
    select
        m1.ds as ds,
        (
        case
        when m1.ad_type in ('special','seckill-tab-hot.productList') then '爆款'
        when m1.ad_type = 'cct-past-product.productList' then '往期爆款'
        when m1.ad_type like 'seckill-tab%' and m1.ad_type != 'seckill-tab-hot.productList' then '秒杀'
        else '其他' end
        ) as ad_type_new,
        (
        case
        when m1.product_cname1 in ('箱包','女装','运动户外','男装','配饰','鞋靴','女士内衣/男士内衣/家居服') then '服饰'
        when m1.product_cname1 in ('家用电器','手机数码') then '家电数码'
        when m1.product_cname1 = '家居百货' then '家居百货'
        when m1.product_cname1 = '美妆个护' then '美妆个护'
        when m1.product_cname1 = '母婴' then '母婴'
        when m1.product_cname1 = '食品' and m1.product_cname2 = '水产肉类/新鲜蔬果/熟食' then '生鲜'
        when m1.product_cname1 = '食品' and m1.product_cname2 in ('零食/坚果/特产','传统滋补营养品','酒水/茶/冲饮','粮油米面/南北干货/调味品','保健食品/膳食营养补充食品') then '食品'
        else '其他' end 
        ) as product_cname1_new,
        m1.pb_price as pb_price,
        m1.commission as commission,
        m1.item_price as item_price,
        m1.product_coupon as product_coupon,
        m1.gross_profit
    from
    (
        select
            n1.ds as ds,
            n1.ad_type as ad_type,
            n1.product_cname1 as product_cname1,
            n1.product_cname2 as product_cname2,
            n1.pb_price as pb_price,
            n1.commission as commission,
            n1.item_price as item_price,
            n1.product_coupon as product_coupon,--楚币
            (n1.item_price+n1.product_coupon-n1.pb_price-0.8992*n1.commission) as gross_profit--毛利额
        from
        ( ---能连上供货价的自营的商品毛利额计算
            select
                t1.ds as ds,
                t2.ad_type as ad_type,
                t3.product_cname1 as product_cname1,
                t3.product_cname2 as product_cname2,
                t3.shop_id as shop_id,
                t5.pb_price as pb_price,
                t1.commission as commission,
                t1.item_price as item_price,
                if(t1.item_price=0,t4.used_money,(t1.item_price*t4.used_money/t4.order_price)) as product_coupon
            from
            (
                select
                    s1.ds as ds,
                    s1.product_id as product_id,
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
                    order_sn,
                    ad_type
                from  
                    origin_common.cc_ods_log_gwapp_order_track_hourly
                where 
                    ds >= '${begin_date}'
                and 
                    ds <= '${end_date}'
            ) t2
            on t1.third_tradeno = t2.order_sn
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
                    n1.ds as ds,
                    n1.third_tradeno as third_tradeno,
                    n1.order_price as order_price,
                    n2.used_money as used_money
                from
                (
                    select
                        s1.ds as ds,
                        s1.third_tradeno as third_tradeno,
                        sum(s1.item_price/100) as order_price
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
                    group by 
                        s1.ds,s1.third_tradeno
                ) n1
                left join 
                (
                    select
                        ds,
                        order_sn,
                        used_money
                    from 
                        origin_common.cc_order_coupon_paytime
                    where 
                        ds >= '${begin_date}'
                    and 
                        ds <= '${end_date}'
                    and 
                        shop_id = 0
                ) n2
                on n1.third_tradeno = n2.order_sn and n1.ds = n2.ds
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
            where t5.order_sn is not null
        ) n1
        union all
        select
            n2.ds as ds,
            n2.ad_type as ad_type,
            n2.product_cname1 as product_cname1,
            n2.product_cname2 as product_cname2,
            n2.pb_price as pb_price,
            n2.commission as commission,
            n2.item_price as item_price,
            n2.product_coupon as product_coupon,
            (n2.item_price+n2.product_coupon-n2.pb_price-0.8992*n2.commission) as gross_profit
        from
        (---连不上供货价的但也算是自营的商品毛利额计算
            select
                t1.ds as ds,
                t2.ad_type as ad_type,
                t3.product_cname1 as product_cname1,
                t3.product_cname2 as product_cname2,
                t3.shop_id as shop_id,
                t5.pb_price as pb_price,
                t1.commission as commission,
                t1.item_price as item_price,
                if(t1.item_price=0,t4.used_money,(t1.item_price*t4.used_money/t4.order_price)) as product_coupon 
            from
            (
                select
                    s1.ds as ds,
                    s1.product_id as product_id,
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
                    s1.ds >= '${begin_date}'
                and 
                    s1.ds <= '${end_date}'
                and 
                    s2.ds = '${end_date}'
                and
                    s2.platform = 14
            ) t1
            left join 
            (
                select
                    order_sn,
                    ad_type
                from  
                    origin_common.cc_ods_log_gwapp_order_track_hourly
                where 
                    ds >= '${begin_date}'
                and 
                    ds <= '${end_date}'
            ) t2
            on t1.third_tradeno = t2.order_sn
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
                    n1.ds as ds,
                    n1.third_tradeno as third_tradeno,
                    n1.order_price as order_price,
                    n2.used_money as used_money
                from
                (
                    select
                        s1.ds as ds,
                        s1.third_tradeno as third_tradeno,
                        sum(s1.item_price/100) as order_price
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
                    group by 
                        s1.ds,s1.third_tradeno
                ) n1
                left join 
                (
                    select
                        ds,
                        order_sn,
                        used_money
                    from 
                        origin_common.cc_order_coupon_paytime
                    where 
                        ds >= '${begin_date}'
                    and 
                        ds <= '${end_date}'
                    and 
                        shop_id = 0
                ) n2
                on n1.third_tradeno = n2.order_sn and n1.ds = n2.ds
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
            where t5.order_sn is null and t3.shop_id in (18164,18335,17801,18532,19141,19268,19405,19347,20471,20770)
        ) n2
        union all
        select
            n3.ds as ds,
            n3.ad_type as ad_type,
            n3.product_cname1 as product_cname1,
            n3.product_cname2 as product_cname2,
            n3.pb_price as pb_price,
            n3.commission as commission,
            n3.item_price as item_price,
            n3.product_coupon as product_coupon,
            (n3.commission*0.1008) as gross_profit
        from
        (--连不上供货价的 pop的商品毛利额计算
            select
                t1.ds as ds,
                t2.ad_type as ad_type,
                t3.product_cname1 as product_cname1,
                t3.product_cname2 as product_cname2,
                t3.shop_id as shop_id,
                t5.pb_price as pb_price,
                t1.commission as commission,
                t1.item_price as item_price,
                if(t1.item_price=0,t4.used_money,(t1.item_price*t4.used_money/t4.order_price)) as product_coupon
            from
            (
                select
                    s1.ds as ds,
                    s1.product_id as product_id,
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
                    s1.ds >= '${begin_date}'
                and 
                    s1.ds <= '${end_date}'
                and 
                    s2.ds = '${end_date}'
                and
                    s2.platform = 14
            ) t1
            left join 
            (
                select
                    order_sn,
                    ad_type
                from  
                    origin_common.cc_ods_log_gwapp_order_track_hourly
                where 
                    ds >= '${begin_date}'
                and 
                    ds <= '${end_date}'
            ) t2
            on t1.third_tradeno = t2.order_sn
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
                    n1.ds as ds,
                    n1.third_tradeno as third_tradeno,
                    n1.order_price as order_price,
                    n2.used_money as used_money
                from
                (
                    select
                        s1.ds as ds,
                        s1.third_tradeno as third_tradeno,
                        sum(s1.item_price/100) as order_price
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
                    group by 
                        s1.ds,s1.third_tradeno
                ) n1
                left join 
                (
                    select
                        ds,
                        order_sn,
                        used_money
                    from 
                        origin_common.cc_order_coupon_paytime
                    where 
                        ds >= '${begin_date}'
                    and 
                        ds <= '${end_date}'
                    and 
                        shop_id = 0
                ) n2
                on n1.third_tradeno = n2.order_sn and n1.ds = n2.ds
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
            where t5.order_sn is null and t3.shop_id not in (18164,18335,17801,18532,19141,19268,19405,19347,20471,20770)
        ) n3
    ) m1
) h1
group by h1.ds,h1.ad_type_new,h1.product_cname1_new

/////////////////////////////////////////////////////////
select
    t1.pm_pid,
    t1.pm_title,
    t2.pb_stock,--商品库存
    t2.pb_price,--供货价
    t2.pb_batch,
    t2.pb_status,
    t2.pb_desc
from
(
    select
        pm_mpid,
        pm_pid,
        pm_title
    from 
        origin_common.cc_ods_fs_op_products_map 
    where 
        pm_sid in (17801,18164,18335,18532,19141,19268,19347,19405)
) t1
inner join 
(
    select 
        pb_mpid,
        pb_stock,
        pb_price,
        pb_batch,
        pb_status,
        pb_desc
    from 
        origin_common.cc_ods_fs_op_product_batches 
) t2
on t1.pm_mpid = t2.pb_mpid
/////////////////////////////////////////////////////////////////
select
    t1.pm_pid,
    t1.pm_title,
    t2.pb_stock,
    t2.pb_price,
    t2.pb_batch,
    t2.pb_status,
    t2.pb_desc
from
(
    select
        pm_pid,
        pm_mpid,
        pm_title
    from op_products_map
    where 
        pm_pid in ( )
    and 
        pm_mpid != 0
) t1
inner join
(
    select 
        pb_mpid,
        pb_stock,
        pb_price,
        pb_batch,
        pb_status,
        pb_desc
    from op_product_batches
) t2
on t1.pm_mpid=t2.pb_mpid
////////////////////////////////////////////////////////////////////////////////////////////////

select
    c1.product_id   as product_id,--商品id
    c1.max_ad_price as max_ad_price,
    c1.min_ad_price as min_ad_price,
    c1.max_cck_rate as max_cck_rate,
    c1.min_cck_rate as min_cck_rate,
    c2.order_count  as order_count,
    c2.sales_num    as sales_num,
    c2.pay_fee      as pay_fee,
    c2.refund_count as refund_count
from
(
    select
        a1.app_item_id        as product_id,--商品id
        max(a2.ad_price/100)  as max_ad_price,--券前价格 
        min(a2.ad_price/100)  as min_ad_price,
        max(a2.cck_rate/1000) as max_cck_rate,---楚客佣金率 
        min(a2.cck_rate/1000) as min_cck_rate
    from
    (
        select 
            item_id, 
            app_item_id--商品id
        from 
            cc_ods_dwxk_fs_wk_items
        where 
            app_item_id in ()
    ) a1
    left join 
    (
        select
            item_id, 
            ad_price,--券前价格 
            cck_rate,---楚客佣金率 
            cck_price---楚客佣金额  
        from 
            cc_ods_fs_dwxk_ad_items_daily
    ) a2 
    on a1.item_id = a2.item_id
    group by 
        a1.app_item_id
) c1
left join 
(
   select
        a1.product_id,
        count(distinct a1.third_tradeno) as order_count, --订单数
        sum(a1.sale_num)                 as sales_num,  --销量
        sum(a1.item_price/100)           as pay_fee, --支付金额
        count(a2.order_sn) as refund_count
    from
    (
        select
            s1.product_id,
            s1.sale_num,
            s1.item_price,
            s1.third_tradeno
        from
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
        join
            origin_common.cc_ods_dwxk_fs_wk_cck_user s2
        on 
            s1.cck_uid = s2.cck_uid
        where
            s1.ds >= 20180914
        and
            s1.ds <= 20181114
        and
            s2.platform = 14
        and
            s2.ds = 20181114
    )a1
    left join
    (
        select 
            distinct
            t1.order_sn
        from
            origin_common.cc_ods_fs_refund_order t1
        inner join
            origin_common.cc_order_user_delivery_time t2
        on
            t1.order_sn = t2.order_sn
        where
            from_unixtime(t1.create_time,'yyyyMMdd') >= 20180914 
        and
            from_unixtime(t1.create_time,'yyyyMMdd') <= 20181114
        and
            t1.status = 1
    )a2
    on a1.third_tradeno=a2.order_sn
    group by
        a1.product_id
) c2
on c1.product_id =c2.product_id
//////////////////////////////////////////////////////////////////////////////////////////
张梨娜 
select
    n1.cck_uid        as cck_uid,
    n4.real_name      as cck_name,
    n4.phone          as cck_phone,
    n1.sales_num      as sales_num,  --销量
    n1.item_price     as item_price,  --支付金额
    n1.cck_commission as cck_commission --佣金
from
(
    select
        s1.cck_uid as cck_uid,
        sum(s1.sale_num)           as sales_num,  --销量
        sum(s1.item_price/100)     as item_price,  --支付金额
        sum(s1.cck_commission/100) as cck_commission --佣金
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_realtime s1
    join 
        origin_common.cc_ods_dwxk_fs_wk_cck_user s2
    on 
        s1.cck_uid = s2.cck_uid
    where 
        s1.ds = 20181122
    and
        s1.product_id = 110019347805
    and
        s1.create_time >= 1542852000
    and
        s1.create_time <= 1542855600
    and 
        s2.ds = 20181121
    and
        s2.platform = 14
    group by 
        s1.cck_uid
) n1 
left join 
(
    select        
        cck_uid,
        real_name,
        phone
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = 20181121
) n4 
on n1.cck_uid = n4.cck_uid
order by n1.sales_num desc 
limit 20
//////////////////////////////////////////////////////////
崔丹 大西北 销量top300商品 
select
    m1.product_id,
    m1.order_count    as order_count, --订单数
    m1.sales_num           as sales_num,  --销量
    m1.item_price     as item_price,  --支付金额
    m1.cck_commission as cck_commission, --佣金
    m2.product_title,--商品名称
    m2.product_cname1,--商品一级类目
    m2.product_cname2,--商品二级类目
    m2.product_cname3,--商品三级类目
    m2.shop_id,
    m2.shop_title
from 
(
    select
        t1.product_id,
        count(distinct t1.order_sn)    as order_count, --订单数
        sum(t1.sale_num)           as sales_num,  --销量
        sum(t1.item_price/100)     as item_price,  --支付金额
        sum(t1.cck_commission/100) as cck_commission --佣金
    from 
    (
        select
            product_id       as product_id,
            sale_num         as sale_num,
            third_tradeno    as order_sn,
            item_price       as item_price,
            cck_commission
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime 
        where
            ds >='${stat_date}'
        and
            ds <= '${end_date}'
    ) t1
    inner join 
    (
        select
            order_sn,
            area_id
        from
            origin_common.cc_order_user_pay_time
        where
            ds >='${stat_date}'
        and
            ds <= '${end_date}'        
    ) t2
    on t1.order_sn = t2.order_sn
    inner join 
    (
        select
            area_id,
            province_name
        from 
            origin_common.cc_area_city_province
        where province_id in (107,126,127,128,129,130,131,202,204,205,206) 
    ) t3
    on t2.area_id = t3.area_id
    group by t1.product_id
    order by sales_num desc 
    limit 300
) m1 
left join
(
    select
        product_id,--商品id
        product_title,--商品名称
        product_cname1,--商品一级类目
        product_cname2,--商品二级类目
        product_cname3,--商品三级类目
        shop_id,
        shop_title
    from data.cc_dw_fs_products_shops
) m2
on m1.product_id = m2.product_id

/////////////////////////////////////////////////////////////
    select
        t1.product_id,
        t1.order_sn, --订单数
        t1.sale_num,  --销量
        (t1.item_price/100),  --支付金额
        (t1.cck_commission/100), --佣金
        t3.province_name
    from 
    (
        select
            product_id       as product_id,
            sale_num         as sale_num,
            third_tradeno    as order_sn,
            item_price       as item_price,
            cck_commission
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime 
        where
            ds >='${stat_date}'
        and
            ds <= '${end_date}'
        and  
            product_id = 110019405798
    ) t1
    inner join 
    (
        select
            order_sn,
            area_id
        from
            origin_common.cc_order_user_pay_time
        where
            ds >='${stat_date}'
        and
            ds <= '${end_date}'        
    ) t2
    on t1.order_sn = t2.order_sn
    inner join 
    (
        select
            area_id,
            province_name
        from 
            origin_common.cc_area_city_province
    ) t3
    on t2.area_id = t3.area_id
/////////////////////////////////////////////////////////////////////////////////////////////
廖志刚 批发专区 订单量 订单量，销售额，商品，推广数据
/////////////////////////////////////////////////////////////////////////////////////////////

select
    n1.ds            as ds,
    n1.product_id    as product_id,
    n1.product_sku_id as product_sku_id,
    n2.product_title,
    n2.product_cname1,
    n2.product_cname2,
    n2.shop_id,
    n1.third_tradeno as third_tradeno,
    n1.pay_fee as pay_fee,
    n1.sale_num,
    n3.delivery_time,
    n3.shipping_sn
from
(
    select
        ds            as ds,
        product_id    as product_id,
        product_sku_id,
        third_tradeno as third_tradeno,
        (item_price/100) as pay_fee,--支付金额
        sale_num
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime 
    where
        ds>=20181101
    and
        ds<=20181130
)  n1
inner join
(
    select
        product_id,
        product_title,
        product_cname1,
        product_cname2,
        shop_id
    from 
        data.cc_dw_fs_products_shops
    where 
        shop_id = 18164
) n2
on n1.product_id = n2.product_id
left join 
(
    select
        order_sn,--订单号
        delivery_time,
        shipping_sn
    from 
        cc_order_user_delivery_time
    where
        ds>=20181101
    and
        ds<=20181130
) n3
 on n1.third_tradeno = n3.order_sn
/////////////////////////////////////////////////////////////////////////
30日有销量店铺
select
    distinct
    t2.shop_id,--店铺id
    t2.shop_title,--店铺名称
    (case   
    when t2.shop_id in (19903,20305,20322) then '百诺优品'
    when t2.shop_id = 18470 then '冰冰购'
    when t2.shop_id in (18532,19141,19268,19347,20471) then '代发'
    when t2.shop_id in (18635,18240) then '极便利'
    when t2.shop_id in (17791,18731) then '京东'
    when t2.shop_id in (18704,18636) then '每日优鲜'
    when t2.shop_id in (18730,18723,18542,17636,18482,19089,19667,20203,20314,20343,20065,20548,20738,19517) then '其他'
    when t2.shop_id in (18662,18729,18588,18740,18799,19319,19405,19402,20216,18965,20696) then '生鲜'
    when t2.shop_id in (18706,18586,18569,18262,19392,18606,15426,18314,19534,2873,19708,2369,9872,19871,19756,19755,19709,16851,20179,17691,20242,456,3559,13930,15907,20513,20652,20653,20725,20789) then '小团子'
    when t2.shop_id = 18455 then '严选'
    when t2.shop_id in (18838,19239,19505,19504,19486,19470,19404,19527,19521,19525,19542,19613,19609,19599,19580,19664,19701,19699,19683,19682,19678,19765,19742,19722,19753,20016,19906,19907,20063,20064,20178,20168,20236,20237,20202,20188,4086,20697,20737,19627,20748,18327,12902,11974,12334,15670,15912,14715,5649,16898,15729,2752,12375,4599,13706,15395,12461,19654,16293,4024,20353,17929,104,3037,19170,14948,1793,19207,4999,16137,3885,16671,18791,17210,5987,14956,1341,15499,1555,18381,16194,5107,16133,8670,2254,18253,3803,17773,13698,17576,14832,18565,739,9349,7693,14720,15044,13638,7200,4318,12033,12766,17639,13363,16305,15853,6163,11500,9806,4539,20818,17845,12523,13559,13991,1412,14823,15129,1655,17157,17397,1802,18057,1831,18812,18814,1937,7572,9621,20784) then '一亩田'
    when t2.shop_id in (18335,18164,17801) then '自营'
    when t2.shop_id in (19339,19468,19298,18491,19611,19435,18765,19870,20142,20332,18574,20392,20423,20543,20600,20770) then '总监店铺'
    else 'pop' end) as shop_type--店铺类型
from
(
    select
        distinct
        s1.product_id
    from
    (
        select
            product_id,
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
)t1
left join
(
    select
        product_id,--商品id
        product_cname1,
        product_cname2,
        shop_id,--店铺id
        shop_title--店铺名称
    from 
        data.cc_dw_fs_products_shops
)t2
on t1.product_id=t2.product_id
////////////////////////////////////////////
大微信客的所有店铺（线上3306商城库）
select
    n2.shop_id,
    n2.num
from
(
    select
        cid as shop_id
    from 
        cc_white_manger 
    where 
        type =1 and flag = 6 and status = 1
) n1 
left join 
(
    select
        shop_id,
        count(product_id) as num
    from 
        cc_product
    where
        from_unixtime(ctime,'yyyyMMdd') >= 20181130
    and
        from_unixtime(ctime,'yyyyMMdd') <= 20181205
    group by 
        shop_id 
) n2
on n1.shop_id = n2.shop_id
////////////////////////////////////////////////////////////////////////////////////////////
select
    n2.shop_id,
    n2.num
from
(
    select
        id as shop_id
    from 
        cc_shop
    where 
        ds =20181205
    and 
        status = 1
) n1
inner join 
(
    SELECT
        shop_id,
        count(product_id) as num
    from 
        cc_product
    where
        ds =20181205
    and 
        from_unixtime(ctime,'yyyyMMdd') >= 20181130
    and
        from_unixtime(ctime,'yyyyMMdd') <= 20181205
    GROUP BY 
        shop_id 
) n2
on n1.shop_id =n2.shop_id 
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
或者 cc_ods_op_shops 或者 cc_shop 这俩差不多，基本没啥区别
/////////////////////////////////////////////////////////////////////////////////////////////////
东哥 大微信客的启用的店铺 一段时间内新创建的商品信息及推广与否
select
    n1.shop_id,
    n2.shop_title,
    n1.product_id,
    n2.product_title,
    n2.product_cname1,
    n2.product_cname2,
    n2.product_cname3,
    n1.ctime,
    n3.app_item_id
from 
(
    select
        shop_id,
        product_id,
        from_unixtime(ctime,'yyyyMMdd HH:mm:dd') as ctime
    from 
        origin_common.cc_product
    where
        ds =20181206
    and 
        from_unixtime(ctime,'yyyyMMdd') >= '${begin_date}'
    and
        from_unixtime(ctime,'yyyyMMdd') <= '${end_date}'
    and 
        shop_id in (122,9342,182,11845,18035,17490,965,17337,3826,17115,16650,9570,10671,7647,3646,11237,17012,11445,17516,17152,17172,15688,14005,9691,8553,17845,11449,9961,12889,18394,18294,6427,13805,10097,15519,17501,17748,18168,17483,11279,18210,18123,17926,17489,18080,17693,18164,17522,17441,18240,17137,17768,8756,16581,16375,17707,16878,17645,17981,17349,12924,16687,17167,11137,16286,16741,16853,7199,12854,9950,12846,16355,18338,16540,18145,17776,18314,12964,18174,12545,6621,17560,14390,11607,8266,168,10639,16785,266,13589,14502,17691,11646,18133,3803,18385,10064,17757,9665,18161,10878,17920,17005,18382,2995,17950,17315,17563,9912,18238,18317,8622,17405,8270,17597,17982,17851,17902,10036,13773,12922,17850,11548,18417,10141,9304,15811,10078,17531,17951,17653,4121,16561,17769,17704,1796,18330,6521,16567,16270,9238,17690,17702,533,7479,8481,9151,16510,18091,2369,5666,8089,5138,17705,8106,4128,18142,18285,16530,13896,11184,18304,16819,13352,14975,17500,8032,13930,14438,17896,17485,16717,17944,5529,9241,17957,1374,17428,11677,14515,17791,11760,8839,18309,17884,18255,17692,9601,17947,17726,8935,18197,18117,15801,18250,17888,17461,14359,17686,17819,17699,17803,17698,10668,17582,18057,8036,17218,16439,18007,17697,17839,18224,17684,18432,18226,17624,13278,16315,18053,17815,18071,18217,15279,18065,17200,10338,16907,17114,14560,18491,18482,18455,18470,8990,18492,18243,11520,18467,18501,18516,2776,14661,18526,18471,12502,18518,18547,18510,18546,18569,18574,18579,18533,18542,18494,18576,18595,18479,18586,18606,18374,18588,18590,18636,18625,18551,18575,9565,18472,18633,18662,18398,18684,18673,18674,18608,18692,18693,18676,18723,18686,18649,18683,18729,18730,18714,18323,18611,18682,18108,18744,18871,18838,18281,18765,18740,18722,18860,17636,18706,18532,11568,18911,18927,18799,18719,15426,18878,19127,18937,19171,16671,19239,19268,19277,18288,19284,19190,17356,18262,19347,19211,18112,19370,19392,19405,17931,19426,16851,19412,18907,19404,19470,19486,19504,19505,19391,19521,19527,19542,19525,18791,19561,19580,19599,19609,19613,19534,19497,19507,19664,19089,2873,19678,19682,19683,19699,19701,19402,19434,19708,19700,19636,19621,16531,19765,19742,19722,19753,20016,19906,19907,20063,20064,19709,19755,19756,19871,19872,3533,8339,20179,20203,20168,20178,19570,19766,19908,19697,12704,19330,19675,19767,20242,20188,20202,20237,20236,456,3559,7780,20233,19884,16310,20201,20314,20343,20243,20350,20325,20260,20294,19698,19313,19600,15907,20392,20335,19595,20423,18873,19209,17300,20401,20205,20399,20481,20499,20413,20513,20065,20471,20493,20427,20548,20443,20500,20546,19517,20492,20505,19651,20506,9974,20545,20539,20541,20573,20411,18965,20652,20653,20534,18552,20336,18512,20604,4086,20697,20696,20725,20738,20737,19596,20676,20648,20614,19627,20748,20770,18327,12902,11974,12334,15670,15912,14715,5649,16898,15729,2752,12375,4599,13706,15395,12461,19654,16293,4024,20353,17929,104,3037,19170,1793,19207,4999,16137,3885,17210,5987,14956,1341,15499,1555,18381,16194,5107,16133,8670,2254,17773,13698,17576,14832,18565,20719,20689,20789,15853,16305,13363,17639,12766,12033,4318,7200,13638,15044,14720,7693,9349,739,20818,4539,9806,11500,6163,17157,1937,13991,9621,12523,15129,13559,1412,1802,14823,7572,1831,1655,14948,18812,17397,20754,20784,20842,20812,20923,20916,20558,20937,20939,20806,20524,20965,20931,20860,20751,17297,22361,22474,20940,22605,22452,22463,21703,22728,22747,22760,20777,20495,22819,22799,20688,22751,22451,13098,22800,22917,14686,23024,23137,22853,22881,22607,23179,22458,23294,23246,20554,23033,19641,20667,22955,23622,23633,23384,23634,23708,22699,23721,16273,19371,23643,23794,23801) 
) n1
left join 
(
    select
        distinct
        product_id,
        product_title,
        product_cname1,
        product_cname2,
        product_cname3,
        shop_title
    from
        data.cc_dw_fs_products_shops 
) n2
on n1.product_id =n2.product_id 
left join 
(
    select
        distinct
        t2.app_item_id
    from 
    (
        select
            distinct
            item_id
        from 
            origin_common.cc_ods_fs_dwxk_ad_items_daily
        where
            audit_status = 1
        and
            status>0
        and
            from_unixtime(end_time,'yyyyMMdd')>='${end_date}'
    ) t1
    inner join
    (
        select 
            item_id, 
            app_item_id
        from 
            origin_common.cc_ods_dwxk_fs_wk_items
    )t2
     on t1.item_id = t2.item_id
) n3
on n1.product_id = n3.app_item_id
/////////////////////////////////////////////////////////////////////////////////////////////////
线上日期范围 小于时间得比hive 时间大一天 总数才对的上
select
    product_id,
    is_dwxk
from 
    cc_product
where
    ctime>=1543420800--1130
and
    ctime<=1543939200--1206
and 
    shop_id in (23633,23622,23294,23246,23179,23137,23033,23024,22955,22917,22881,22853,22819,22800,22799,22760,22751,22747,22728,22607,22605,22474,22463,22458,22452,22451,22361,21703,20965,20940,20939,20937,20931,20923,20916,20860,20842,20818,20812,20806,20789,20784,20777,20770,20754,20751,20748,20738,20737,20725,20719,20697,20696,20689,20688,20676,20667,20653,20652,20648,20614,20604,20573,20558,20554,20548,20546,20545,20541,20539,20534,20524,20513,20506,20505,20500,20499,20495,20493,20492,20481,20471,20443,20427,20423,20413,20411,20401,20399,20392,20353,20350,20343,20336,20335,20325,20314,20294,20260,20243,20242,20237,20236,20233,20205,20203,20202,20201,20188,20179,20178,20168,20065,20064,20063,20016,19908,19907,19906,19884,19872,19871,19767,19766,19765,19756,19755,19753,19742,19722,19709,19708,19701,19700,19699,19698,19697,19683,19682,19678,19675,19664,19654,19651,19641,19636,19627,19621,19613,19609,19600,19599,19596,19595,19580,19570,19561,19542,19534,19527,19525,19521,19517,19507,19505,19504,19497,19486,19470,19434,19426,19412,19405,19404,19402,19392,19391,19370,19347,19330,19313,19284,19277,19268,19239,19211,19209,19207,19190,19171,19170,19127,19089,18965,18937,18927,18911,18907,18878,18873,18871,18860,18838,18812,18799,18791,18765,18744,18740,18730,18729,18723,18722,18719,18714,18706,18693,18692,18686,18684,18683,18682,18676,18674,18673,18662,18649,18636,18633,18625,18611,18608,18606,18595,18590,18588,18586,18579,18576,18575,18574,18569,18565,18552,18551,18547,18546,18542,18533,18532,18526,18518,18516,18512,18510,18501,18494,18492,18491,18482,18475,18472,18471,18470,18467,18455,18432,18417,18398,18394,18385,18382,18381,18374,18338,18330,18327,18323,18317,18314,18309,18304,18294,18288,18285,18262,18255,18253,18250,18243,18240,18238,18226,18224,18217,18210,18197,18174,18168,18164,18161,18145,18142,18133,18123,18117,18112,18108,18091,18080,18071,18065,18057,18053,18035,18007,17982,17981,17957,17951,17950,17947,17944,17931,17929,17926,17920,17902,17896,17888,17884,17851,17850,17845,17839,17819,17815,17803,17791,17788,17776,17773,17769,17768,17757,17748,17726,17707,17705,17704,17702,17699,17698,17697,17693,17692,17691,17690,17686,17684,17653,17645,17639,17636,17624,17597,17582,17576,17563,17560,17531,17522,17516,17501,17500,17490,17485,17483,17461,17441,17428,17405,17397,17356,17349,17337,17315,17300,17297,17218,17210,17200,17172,17167,17157,17152,17137,17114,17012,17005,16907,16898,16878,16853,16851,16819,16785,16741,16717,16687,16671,16650,16581,16567,16561,16540,16531,16530,16510,16439,16375,16355,16315,16310,16305,16293,16286,16270,16194,16137,16133,15912,15907,15853,15811,15801,15729,15688,15670,15519,15499,15426,15395,15279,15129,15044,14975,14956,14948,14832,14823,14720,14715,14686,14661,14560,14515,14502,14438,14390,14359,14005,13991,13930,13896,13805,13773,13706,13698,13638,13589,13559,13363,13352,13278,13098,12964,12924,12922,12902,12889,12854,12846,12766,12704,12545,12523,12502,12461,12375,12334,12033,11974,11845,11760,11677,11646,11607,11568,11548,11520,11500,11449,11445,11279,11237,11184,11137,10878,10671,10668,10639,10338,10141,10097,10078,10064,10036,9974,9961,9950,9912,9806,9691,9665,9621,9601,9570,9565,9349,9342,9304,9241,9238,9151,8990,8935,8839,8756,8670,8622,8553,8481,8339,8270,8266,8106,8089,8036,8032,7780,7693,7647,7572,7479,7200,7199,6621,6521,6427,6163,5987,5666,5649,5529,5138,5107,4999,4599,4539,4318,4128,4121,4086,4024,3885,3826,3803,3646,3559,3533,3037,2995,2873,2776,2752,2369,2254,1937,1831,1802,1796,1793,1655,1555,1412,1374,1341,965,739,533,456,266,182,168,122,104)
///////////////////////////////////////////////////////////
领取两个优惠券的用户信息
select
    n1.user_id,
    n1.template_id,
    n1.draw_time,
    n1.coupon_sn,
    n1.cck_uid,
    n1.real_name,
    n1.phone,
    n2.order_sn
from 
(
    select
        t1.user_id,
        t1.template_id,
        t1.draw_time,
        t1.coupon_sn,
        t2.cck_uid,
        t3.real_name,
        t3.phone
    from 
    (
        select
            user_id,
            template_id,
            draw_time,
            coupon_sn
        from 
            origin_common.cc_coupon_user 
        where 
            ds >=20181201
        and 
            ds <=20181209
        and 
            template_id in (16974449,16974311)
    ) t1 
    left join 
    (
        select
            cct_uid,
            cck_uid
        from 
            origin_common.cc_ods_fs_tui_relation 
    ) t2
    on t1.user_id = t2.cct_uid
    left join 
    (
        select        
            cck_uid,
            real_name,
            phone
        from 
            origin_common.cc_ods_dwxk_fs_wk_business_info
        where 
            ds = 20181209
    ) t3
    on t2.cck_uid=t3.cck_uid
) n1
left join 
(
    select
        t1.order_sn,
        t2.user_id,
        t2.pay_time
    from 
    (
        select
            s1.order_sn
        from
            origin_common.cc_order_products_user_pay_time s1
        where 
            s1.ds >= '${state_date}'
        and 
            s1.ds <= '${end_date}'
        and 
            s1.product_id in (110018542152,110018542158)  
    ) t1
    left join 
    (
        select
            order_sn,
            user_id,
            pay_time
        from 
            origin_common.cc_order_user_pay_time
        where 
            ds >= '${state_date}'
        and 
            ds <= '${end_date}'
    ) t2
    on t1.order_sn = t2.order_sn
) n2
on n1.user_id = n2.user_id and n1.draw_time =n2.pay_time
////////////////////////////////////////////////////////////////////
施旭鸿 
select
    n1.cct_uid,
    n2.timestamp
from 
(
    select 
        cct_uid,
        min(timestamp) as timestamp
    from 
        origin_common.cc_ods_log_gwapp_pv_hourly  
    where 
        ds = 20180601
    and 
        module='https://app-h5.daweixinke.com/chuchutui/index.html' 
    and 
        cct_uid is not null 
    and 
        app_partner_id = 14
)n1
inner join 
(
    select 
        cct_uid,
        min(timestamp) as timestamp
    from 
        origin_common.cc_ods_log_gwapp_pv_hourly  
    where 
        ds <= 20180601 
    and 
        module='https://app-h5.daweixinke.com/chuchutui/index.html' 
    and 
        cct_uid is not null 
    and 
        app_partner_id = 14
)n2
on n1.cct_uid = n2.cct_uid and  n1.timestamp = n2.timestamp
////////////////////////////////////////////////////////////////////////////
施旭鸿 
select 
    count(p1.cct_uid) as newuser,
    count(if(p1.user_type=2,p1.cct_uid,null)) as newuser_c,
    count(if(p1.user_type=1,p1.cct_uid,null)) as newuser_j,
    count(if(p1.user_type=0,p1.cct_uid,null)) as newusaer_v,
    count(p2.cct_uid) as loginapp
from 
(
    select 
        t1.cck_uid as cck_uid,
        t2.cct_uid as cct_uid,
        0 as user_type
    from
    (
        select 
            cck_uid
        from 
            cc_ods_fs_wk_cct_layer_info
        where 
            platform=14
        and 
            is_del=0
        and 
            from_unixtime(create_time,'yyyymmdd')='${bizdate}'
    ) t1
    inner join
    (
        select 
            cck_uid,
            cct_uid
        from 
            cc_ods_dwxk_fs_wk_cck_user
        where 
            ds='${bizdate}'
        and 
            cct_uid>0
    ) t2 
    on t1.cck_uid=t2.cck_uid
    union all 
    select 
        distinct 
        t1.cck_uid as cck_uid,
        t1.cct_uid as cct_uid,
        1 as user_type
    from
    (
        select
            cck_uid,
            cct_uid,
            guider_uid
        from 
            cc_ods_fs_tui_relation
        where 
            cck_vip_level=1
        and 
            cck_vip_status=0
        and 
            from_unixtime(mtime,'yyyyMMdd')='${bizdate}'
    ) t1
    inner join
    (
        select 
            cck_uid
        from 
            cc_ods_dwxk_fs_wk_cck_user
        where 
            ds='${bizdate}'
        and 
            platform=14
    ) t2 
    on t1.guider_uid=t2.cck_uid
    union all 
--->新增普通用户
    select 
        distinct 
        t1.cck_uid as cck_uid,
        t1.cct_uid as cct_uid,
        2 as user_type
    from
    (
        select 
            cck_uid,
            cct_uid,
            guider_uid
        from 
            cc_ods_fs_tui_relation
        where 
            cck_vip_level=0
        and 
            cck_vip_status=0
        and 
            from_unixtime(mtime,'yyyyMMdd')='${bizdate}'
    ) t1
    inner join
    (
        select 
            cck_uid
        from 
            cc_ods_dwxk_fs_wk_cck_user
        where 
            ds='${bizdate}'
        and 
            platform=14
    ) t2 
    on t1.guider_uid=t2.cck_uid
) p1
left join
(   
    select 
        distinct 
        cct_uid
    from 
        origin_common.cc_ods_log_gwapp_pv_hourly
    where 
        ds = '${bizdate}'
    and 
        app_partner_id=14
    and
        module = 'https://app-h5.daweixinke.com/chuchutui/index.html'
 ) p2 
on p1.cct_uid=p2.cct_uid 
/////////////////////////////////////////////////////////////////
周鹿
select
    n1.user_id,
    n2.cck_uid,
    n3.pay_cnt, --销售单数
    n3.sales_num,
    n3.total_fee, --销售额
    n3.cck_commission --预估佣金
from
(
    select
        distinct
        m1.user_id
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
) n1
left join 
(
    select 
        cck_uid,
        cct_uid,
        guider_uid
    from 
        cc_ods_fs_tui_relation
) n2
on n1.user_id = n2.cct_uid
left join 
(
    select 
        cck_uid, --楚客id
        count(distinct third_tradeno) as pay_cnt, --销售单数
        sum(sale_num) as sales_num,
        sum(item_price/100) as total_fee, --销售额
        sum(cck_commission/100) as cck_commission --预估佣金
    from 
        cc_ods_dwxk_wk_sales_deal_ctime
    where 
        ds = '${stat_date}' 
    and 
        product_id = '${product_id}'
    group by 
        cck_uid
) n3
on n2.cck_uid = n3.cck_uid
/////////////////////////////////////////////////////////////////////////
select
    n1.date,
    n2.product_id,
    n3.product_title,--商品名称
    n3.product_cname1,--商品一级类目
    n3.product_cname2,--商品二级类目
    n3.product_cname3--商品三级类目
from
(
    select
        from_unixtime(begin_time,'yyyyMMdd') as date,
        ad_material_id,
        begin_time,
        end_time
    from
        origin_common.cc_ods_fs_cck_xb_policies_hourly
    where
        from_unixtime(begin_time,'yyyyMMdd') >= '${begin_date}'
    and
        from_unixtime(begin_time,'yyyyMMdd') <= '${end_date}'
) as n1
inner join
(
    select
        ad_material_id,
        product_id,
        operator
    from
        origin_common.cc_ods_fs_cck_ad_material_products_hourly
    where
        ad_material_id >0
) as n2
on n1.ad_material_id = n2.ad_material_id
inner join 
(
    select
        product_id,--商品id
        product_title,--商品名称
        product_cname1,--商品一级类目
        product_cname2,--商品二级类目
        product_cname3--商品三级类目
    from data.cc_dw_fs_products_shops
    where shop_id in (18704,18636)
) as n3
on n2.product_id=n3.product_id

///////////////////////////////////////////////////////////////////
select
    n1.shop_id,
    n2.shop_title,
    n1.product_id,
    n2.product_cname1,
    n2.product_cname2,
    n2.product_cname3,
    n1.ctime
from 
(
    select
        shop_id,
        product_id,
        from_unixtime(ctime,'yyyyMMdd HH:mm:dd') as ctime
    from 
        origin_common.cc_product
    where
        ds =20181211
    and 
        from_unixtime(ctime,'yyyyMMdd') >= 20181130
    and
        from_unixtime(ctime,'yyyyMMdd') <= 20181206
    and 
        shop_id in (18704,18636)
) n1
left join 
(
    select
        distinct
        product_id,
        product_cname1,
        product_cname2,
        product_cname3,
        shop_title
    from
        data.cc_dw_fs_products_shops 
) n2
on n1.product_id =n2.product_id 
///////////////////////////////////////////////////////////////////////////////////////
廖志刚
select
    t1.real_name,
    t1.cck_uid,
    t1.phone
from 
(
    select        
        real_name,
        cck_uid,
        phone
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = 20181211
    and 
        real_name in ('肖琴','王晓军','滕首东','曹冬梅','王芳','唐敏','饶明忠','李佳','袁昌莲','李金国','李洁平','李玉忠','陈小如','杨秋香','李卫明','芦永福','王秀青','邱天南','杨大明','戴安丽','方桂萍','邢桂香','曾妍','曾小雪','胡凤','胡砾尹','吴大宝','韩定安','薛晓玲','黄艳','叶传贵','魏建文','高易红','张素丽','孙礼萍','程明','李红','史亚红','管晋','郭倩君','张建疆','殷海云','康旭红','李祖强','陈水清','刘颖','杨敏','张馥麒','郭继云','魏祖莹','李世忠','吴思逸','管元发','李萍','杨世春','杨世灯','张冬妹','刘学伟','黄醒光','张凤琴','赖霞云','黄宁胜','徐玉芳','王璇','张蕾','王国英','杨桂红','吴弢','黄升','沈建平','陈学华','高娃','朱志萍','陈金亦','张国添','齐希云')
) t1
inner join 
(
    select 
        cck_uid
    from cc_ods_fs_wk_cct_layer_info
    where 
        type = 1
) t2
on t1.cck_uid=t2.cck_uid
///////////////////////////////////////////////////////////////////////////////////////////////
李光远 
select
    n1.product_id,
    n1.product_title,
    n1.product_cname1,
    n1.shop_id,
    n1.shop_title,
    n3.ad_price as ad_price,---券前价
    n3.cck_rate as cck_rate,---楚客佣金率
    n3.cck_price as cck_price,---楚客佣金额
from
(
    select
        product_id,
        product_title,
        product_cname1,
        shop_id,
        shop_title
    from
        data.cc_dw_fs_products_shops
    where 
        product_cname1 = '美妆个护'
    and 
        shop_id in (19211,18112,19370,19412,18907,18250,18927,19561,19507,17702,19434,19700,19636,12964,16531,3533,18719,8339,19570,19766,19908,19697,18682,12704,19330,19767,7780,20233,19884,20201,19426,20243,10064,19698,20294,20260,20325,20350,19600,20335,19595,18432,17485,18873,19209,17300,20401,20205,17516,20399,20481,20499,20413,11568,20493,20427,20443,20500,20546,3646,20492,20505,19651,20506,9974,20545,20539,20541,20573,20411,20534,18552,11520,18744,20336,18512,20604,19338,19675,19596,20676,20648,20614,20719,20689,20754,20812,20923,20916,20558,20937,20939,20806,20524,20965,18053,8935,20931,20860,20751,22605,22361,13589,20940,22452,22463,18071,22728,22747,22760,20777,22819,22799,17563,20688,22751,22451,13098,22800,22917,14686,23024,23137,18911,18860,18722,18611,18323,18714,18683,18649,18686,18676,18693,18692,18608,18674,18673,18684,18398,18633,18472,9565,18575,18625,18590,18374,18595,18576,18494,18533,18579,18546,18510,18547,18518,12502,18471,18526,14661,2776,18516,18501,18467,18243,18492,19127,18878,14560,17114,16907,10338,17200,18065,15279,18217,17815,16315,13278,17624,18226,17684,18224,17839,17697,18007,16439,17218,8036,17582,10668,17698,17699,17819,17686,14359,17461,17888,15801,18117,18197,17726,17947,9601,17692,18255,17884,18309,8839,11760,14515,11677,17428,1374,17957,9241,5529,17944,16717,17896,8032,17500,14975,13352,16819,18304,11184,13896,16530,18285,18142,4128,8106,17705,5138,8089,5666,18091,16510,9151,8481,7479,533,17690,9238,16270,16567,6521,18330,1796,17704,17769,16561,4121,17653,17951,17531,10078,15811,10141,18417,11548,17850,12922,13773,10036,17902,17851,17982,17597,8270,17405,8622,18317,18238,9912,17315,17950,2995,18382,17005,17920,10878,18161,9665,17757,18385,18133,11646,14502,266,16785,10639,168,14390,6621,12545,18174,17776,18145,16540,18338,17788,16355,12846,9950,12854,7199,16853,16741,16286,11137,17167,16687,12924,17349,17981,17645,16878,17707,16375,16581,8756,17768,17644,17137,17441,17522,17693,18080,17926,18123,18210,11279,17483,17748,17501,15519,10097,13805,6427,18294,18394,12889,9961,11449,8553,9691,14005,15688,17172,17152,11445,17012,11237,7647,10671,9570,16650,3826,17337,965,17490,18035,11845,182,9342,18937,19171,19277,18288,19284,19190,17356,17931,20471,18532,19268,19347,18164,18491,18765,18574,20392,20423,20770,22853,22881,22607,18551,23179,17803,18871,19641,20667,22955,9304,23633,17489)
) n1 
inner join
(
    select
        distinct
        s2.product_id
    from
    (
        select
            distinct 
            item_id
        from 
            cc_ods_fs_dwxk_ad_items_daily 
        where 
            from_unixtime(start_time,'yyyyMMdd')>=20180601
        and 
            from_unixtime(start_time,'yyyyMMdd')<=20180901
    ) s1
    inner join
    (
        select
            item_id,
            product_id
        from 
            cc_ods_dwxk_fs_wk_items
    ) s2
    on s1.item_id = s2.item_id 
) n2 
on n1.product_id = n2.product_id
left join 
(
    select
        s2.app_item_id as product_id,
        (s1.ad_price/100) as ad_price,
        (s1.cck_rate/1000) as cck_rate,---楚客佣金率
        (s1.cck_price/100) as cck_price---楚客佣金额
    from
        cc_ods_dwxk_fs_wk_ad_items s1
    inner join
        cc_ods_dwxk_fs_wk_items s2
    on 
        s1.item_id =s2.item_id
    where
        s1.audit_status=1
    and
        s1.status=1
) n3
on n1.product_id = n3.product_id
///////////////////////////////////////////////////////////////////
商品30日复购数据
select
    n1.product_id,
    n2.product_title,
    n2.product_cname1,
    n2.shop_id,
    n2.shop_title,
    n1.total_buyer_num,
    (n1.one_time_buyer_num/n1.total_buyer_num) as one_buyer_rate,
    (n1.two_times_buyer_num/n1.total_buyer_num) as two_buyer_rate,
    (n1.three_times_buyer_num/n1.total_buyer_num) as three_buyer_rate,
    (n1.four_times_buyer_num/n1.total_buyer_num) as four_buyer_rate,
    (n1.five_times_buyer_num/n1.total_buyer_num) as five_buyer_rate,
    (n1.six_times_buyer_num/n1.total_buyer_num) as six_buyer_rate,
    (n1.seven_times_buyer_num/n1.total_buyer_num) as seven_buyer_rate,
    (n1.more_than_seven_times_buyer_num/n1.total_buyer_num) as more_than_seven_buyer_rate
from
(
    select
        a1.product_id         as product_id,
        count(a1.uid)         as total_buyer_num,
        sum(if(a1.num=1,1,0)) as one_time_buyer_num,
        sum(if(a1.num=2,1,0)) as two_times_buyer_num,
        sum(if(a1.num=3,1,0)) as three_times_buyer_num,
        sum(if(a1.num=4,1,0)) as four_times_buyer_num,
        sum(if(a1.num=5,1,0)) as five_times_buyer_num,
        sum(if(a1.num=6,1,0)) as six_times_buyer_num,
        sum(if(a1.num=7,1,0)) as seven_times_buyer_num,
        sum(if(a1.num>7,1,0)) as more_than_seven_times_buyer_num
    from
    (
        select
            s1.product_id,
            s1.uid,
            count(s1.create_time) as num 
        from
        (
            select
                product_id,
                cck_uid,
                uid,
                create_time
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
        group by 
            s1.product_id,s1.uid
    ) a1
    group by a1.product_id
) n1
left join 
(
    select
        product_id,
        product_title,
        product_cname1,
        shop_id,
        shop_title
    from
        data.cc_dw_fs_products_shops
) n2
on n1.product_id = n2.product_id
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
周鹿 某日 某商品前500单
select
    s1.cck_uid,
    s3.phone,
    s3.real_name,
    s1.sale_num,
    (s1.item_price/100) as item_price,
    s1.create_time
from
(
    select
        cck_uid,
        sale_num,
        item_price,
        create_time
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where
        ds = 20181228
    and
        product_id = 1100185322564
    and
        create_time>=1545998400
    and 
        create_time<=1546012800
    order by 
        create_time
    limit 1000
) s1
inner join
(
    select
        cck_uid
    from    
        origin_common.cc_ods_dwxk_fs_wk_cck_user 
    where
        ds = 20181228
    and
        platform = 14
) s2
on s1.cck_uid=s2.cck_uid
left join
(
    select        
        cck_uid,
        real_name,
        phone
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = 20181228
) s3
on s1.cck_uid = s3.cck_uid
///////////////////////////////////////////////////////////////////////////////////////
select
    s1.cck_uid,
    s1.third_tradeno,
    s1.create_time
from
(
    select
        cck_uid,
        third_tradeno,
        create_time
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_realtime
    where
        ds = 20181214
    and
        product_id = 1100185322564
) s1
inner join
(
    select
        cck_uid
    from    
        origin_common.cc_ods_dwxk_fs_wk_cck_user 
    where
        ds = 20181214
    and
        platform = 14
) s2
on s1.cck_uid=s2.cck_uid
inner join 
(
    select 
        distinct
        order_sn
    from
        origin_common.cc_ods_fs_refund_order 
    where
        from_unixtime(create_time,'yyyyMMdd') >= 20181214
    and
        from_unixtime(create_time,'yyyyMMdd') <= 20181221
    and
        status = 1
) s3
on s1.third_tradeno = s3.order_sn
/////////////////////////////////////////////////////////////////////////////////
东哥 11月份 奖励数据之 退款数据
select
    s1.product_id,
    sum(s1.cck_commission/100) as cck_commission,
    sum(s1.item_price/100) as pay_fee
from
(
    select
        product_id,
        cck_commission,
        item_price,
        cck_uid
    from 
        wk_sales_deal 
    where
        create_time>
    and
        create_time<
    and
        status=3
) s1
inner join 
(
    select
        distinct
        cck_uid
    from 
        wk_cck_user
    where 
        platform = 14
) s2 
on s1.cck_uid = s2.cck_uid
group by
    s1.product_id
//////////////////////////////////////////////////////////////////////
东哥 11，12 月份 奖励数据 注意 再次做一月份的奖励数据时，应该吧statsu=3的所有订单拉出来，查每个订单的实际退款金额，
select
    s1.product_id,
    sum(s1.item_price/100) as pay_fee,
    sum(s1.cck_commission/100) as cck_commission
from
(
    select
        product_id,
        cck_commission,
        item_price,
        cck_uid
    from 
        wk_sales_deal 
    where
        create_time>=1546272000
    and
        create_time<1548950400
    and 
        status in (1,2)
) s1
inner join 
(
    select
        distinct
        cck_uid
    from 
        wk_cck_user
    where 
        platform = 14
) s2 
on s1.cck_uid = s2.cck_uid
group by
    s1.product_id
//////////////////////////////////////////////////////////////////////
东哥 20190101月份 奖励数据,用hive的代码跑了，配合下面的代码合用，把status=3的实际情况还原到每个类目去
select
    n2.product_id,--商品id
    n2.product_cname1,
    n2.product_cname2,
    n1.cck_commission,
    n1.pay_fee
from
(
    select
        t1.product_id as product_id,
        sum(t1.cck_commission/100) as cck_commission,
        sum(t1.item_price/100) as pay_fee
    from
    (
        select
            s1.cck_uid,
            s1.third_tradeno,
            s1.item_price,
            s1.cck_commission,
            s1.product_id,
            s1.product_sku_id
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
        join
            origin_common.cc_ods_dwxk_fs_wk_cck_user s2
        on 
            s1.cck_uid=s2.cck_uid    
        where
            s1.ds>=20190101
            and
            s1.ds<=20190131
            and
            s2.ds=20190131
            and
            s2.platform=14
    )t1
    left join
    (    
        select
            cck_uid,
            sale_num,
            create_time,
            third_tradeno,
            product_sku_id
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_realtime
        where
            ds>=20190101
            and 
            create_time>=1546272000 
            and 
            status=3 
    )t2
    on t1.third_tradeno=t2.third_tradeno and t1.product_sku_id=t2.product_sku_id
    where t2.third_tradeno is null
    group by t1.product_id  
)n1
left join 
(
    select
        distinct
        product_id,--商品id
        product_cname1,
        product_cname2
    from 
        data.cc_dw_fs_products_shops
)n2
on n1.product_id=n2.product_id
//////////////////////////////////////////////////////////////////////
排查status=3的所有订单拉出来，支付金额，查每个订单每个商品的实际退款金额，
这里与status=3的实际支付金额相比还是会多算，
但是与下面的的写法相比，支付金额会少10万，但退款金额却多算了。可能因为靠refun_sn关联商品sku_id时，多返回了，所以把退款金额多算了。
因此又说明refund_sn 不是唯一的。而且之前问过 技术部+周宏他 也说整单退就是一对多的关系，确实有重复的。
select
    t1.product_id,
    t4.product_cname1,
    t4.product_cname2,
    t1.third_tradeno,
    (t1.item_price/100) as item_price,
    (t1.cck_commission/100) as cck_commission,
    t3.success_price,
    t3.status
from
(
    select
        s1.cck_uid,
        s1.third_tradeno,
        s1.item_price,
        s1.cck_commission,
        s1.product_id,
        s1.product_sku_id
    from 
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
    join
        origin_common.cc_ods_dwxk_fs_wk_cck_user s2
    on 
        s1.cck_uid=s2.cck_uid    
    where
        s1.ds>=20190101
        and
        s1.ds<=20190131
        and
        s2.ds=20190131
        and
        s2.platform=14
)t1
inner join
(    
    select
        cck_uid,
        sale_num,
        create_time,
        third_tradeno,
        product_sku_id
    from 
        origin_common.cc_ods_dwxk_wk_sales_deal_realtime
    where
        ds>=20190101
        and 
        create_time>=1546272000 
        and 
        status=3 
)t2
on t1.third_tradeno=t2.third_tradeno and t1.product_sku_id=t2.product_sku_id
left join
(
    select
        m1.order_sn,
        m2.sku_id,
        m1.status,
        m1.success_price
    from 
    (
        select
            t1.refund_sn,
            t1.order_sn,
            t1.success_price,
            t1.status
        from
            origin_common.cc_ods_fs_refund_order t1
        where
            from_unixtime(t1.create_time,'yyyyMMdd') >= 20190101
    ) m1
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
            ds = 20190221 
    )m2
    on m1.refund_sn=m2.refund_sn
)t3
on t1.third_tradeno=t3.order_sn and t1.product_sku_id=t3.sku_id
left join 
(
    select
        distinct
        product_id,--商品id
        product_cname1,
        product_cname2
    from 
        data.cc_dw_fs_products_shops
)t4
on t1.product_id=t4.product_id
//////////////////////////////////////////////////////////////////////
排查status=3的所有订单拉出来，支付金额，查每个订单的实际退款金额，这里会因为退款表的order_sn重复而多算
select
    n1.third_tradeno,
    n1.item_price,
    n1.cck_commission,
    n2.status,
    n2.success_price
from
(
    select
        t1.third_tradeno,
        sum(t1.item_price/100) as item_price,
        sum(t1.cck_commission/100) as cck_commission
    from 
    (
        select
            s1.cck_uid,
            s1.third_tradeno,
            s1.item_price,
            s1.cck_commission,
            s1.product_id,
            s1.product_sku_id
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
        join
            origin_common.cc_ods_dwxk_fs_wk_cck_user s2
        on 
            s1.cck_uid=s2.cck_uid    
        where
            s1.ds>=20190101
            and
            s1.ds<=20190131
            and
            s2.ds=20190131
            and
            s2.platform=14
    )t1
    inner join
    (    
        select
            cck_uid,
            sale_num,
            create_time,
            third_tradeno,
            product_sku_id
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_realtime
        where
            ds>=20190101
            and 
            create_time>=1546272000 
            and 
            status=3 
    )t2
    on t1.third_tradeno=t2.third_tradeno and t1.product_sku_id=t2.product_sku_id
    group by
        t1.third_tradeno
)n1
left join
(
    select
        t1.order_sn,
        t1.success_price,
        t1.status as status
    from
        origin_common.cc_ods_fs_refund_order t1
    where
        from_unixtime(t1.create_time,'yyyyMMdd') >= 20190101
)n2
on n1.third_tradeno=n2.order_sn
//////////////////////////////////////////////////////////////////////////////////////
一段时间 成交商品 
select
    t1.product_id     as product_id,
    t3.product_title  as product_title,
    t3.product_cname1 as product_cname1,
    t3.product_cname2 as product_cname2,
    t3.shop_id        as shop_id,--店铺id
    t3.shop_title     as shop_title,--店铺名称
    t1.order_count    as order_count,
    t1.sales_num      as sales_num,
    t1.cck_commission as cck_commission,
    t1.item_price     as item_price, 
    t4.pv             as pv,
    t4.ipv_uv         as ipv_uv
from 
(
    select
        n1.product_id as product_id,
        count(distinct n1.order_sn) as order_count,
        sum(n1.sale_num) as sales_num,
        sum(n1.cck_commission) as cck_commission,
        sum(n1.item_price) as item_price 
    from
    (
        select
            s1.product_id     as product_id,
            s1.product_sku_id as product_sku_id,
            s1.third_tradeno  as order_sn,
            s1.sale_num       as sale_num,
            (s1.cck_commission/100) as cck_commission,
            (s1.item_price/100) as item_price
        from
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
        inner join 
            origin_common.cc_ods_dwxk_fs_wk_cck_user s2
        on 
            s1.cck_uid = s2.cck_uid
        where 
            s1.ds >= '${begin_date_30d}'
        and 
            s1.ds <= '${end_date}'
        and 
            s2.ds = '${end_date}'
        and
            s2.platform = 14
    )n1
    inner join 
    (
        select
            order_sn
        from  
            origin_common.cc_ods_log_gwapp_order_track_hourly
        where 
            ds >= '${begin_date_30d}'
        and 
            ds <= '${end_date}'
        and 
            ad_type in ('search','searchS','shareSearch')
    ) n2
    on n1.order_sn = n2.order_sn
    group by n1.product_id
)t1 
left join
(
    select
        distinct
        product_id,--商品id
        product_title,
        product_cname1,
        product_cname2,
        shop_id,--店铺id
        shop_title--店铺名称
    from 
        data.cc_dw_fs_products_shops
)t3
on t1.product_id=t3.product_id
left join
(
    select
        n1.product_id,
        sum(n1.pv) as pv,
        sum(n1.ipv_uv) as ipv_uv
    from 
    (
        select
            ds,
            product_id,
            count(user_id) as pv,
            count(distinct user_id) as ipv_uv
        from
            origin_common.cc_ods_log_cctui_product_coupon_detail_hourly
        where
            ds >= '${begin_date_30d}'
        and
            ds <= '${end_date}'
        and
            detail_type = 'item'
        group by
            ds,product_id
    ) n1 
    group by n1.product_id
)t4
on t1.product_id=t4.product_id

///////////////////////////////////////////////
pv 浏览次数，uv浏览人数，
select 
    s1.product_id as product_id, 
    sum(if(s1.ds='${bizdate}',1,0)) as ipv,
    count(1) as ipv_30
from
(
    select
        ds, 
        product_id
    from 
        origin_common.cc_ods_log_cctui_product_coupon_detail_hourly
    where 
        ds>='${bizdate-29}' 
    and 
        ds <= '${bizdate}' 
    and 
        detail_type='item'
    union all
    select
        ds, 
        product_id
    from 
        origin_common.cc_ods_log_gwapp_product_detail_hourly 
    where 
        ds>='${bizdate-29}' 
    and 
        ds <= '${bizdate}'
) s1
group by s1.product_id





