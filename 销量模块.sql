select
    '${bizdate}' as date,
    all2.product_id,
    all2.ad_type,
    all2.buy_type,
    pro_cnt,
    open_cnt,
    pay_fee,
    cck_commission,
    pay_count,
    all1.product_name,
    c1, 
    product_cname1,
    ad_price,
    discount_fee,
    cck_rate,
    all1.ad_id,
    ad_material_id,
    author,
    begin_date,
    end_date,
    shop_id,
    shop_name,
    cck_commission/pro_cnt as pro_roi,
    pro_material_cnt,
    pqrcode_cnt,
    save_material_cnt,
    pro_user_cnt
from
(
    select
        coalesce(p2.product_id,p3.product_id) as product_id,
        coalesce(p2.ad_type,p3.ad_type) as ad_type,
        case
        when coalesce(p2.ad_type,p3.ad_type) in ('cct-today-top-new.productlist','cct-huge-discount-list.productlist','cct-new-people-buy.productlist','commodity-popular.productlist') then split(coalesce(p2.ad_type,p3.ad_type),'\\\\.')[0]
        when coalesce(p2.ad_type,p3.ad_type) like 'cct-huge-discount-%.bannerlist' then 'cct-huge-discount'
        when coalesce(p2.ad_type,p3.ad_type) like 'cct-home-selected-%.bannerlist' then 'cct-home-selected'
        when coalesce(p2.ad_type,p3.ad_type) like 'seckill-tab%' then coalesce(p2.ad_type,p3.ad_type)
        else 'other' end as ad_key,
        case
        when coalesce(p2.ad_type,p3.ad_type) in ('cct-today-top-new.productlist','cct-huge-discount-list.productlist','cct-new-people-buy.productlist','commodity-popular.productlist') then split(coalesce(p2.ad_type,p3.ad_type),'\\\\.')[1]
        when coalesce(p2.ad_type,p3.ad_type) like 'cct-huge-discount-%.bannerlist' then 'bannerlist'
        when coalesce(p2.ad_type,p3.ad_type) like 'cct-home-selected-%.bannerlist' then 'bannerlist'
        when coalesce(p2.ad_type,p3.ad_type) like 'seckill-tab%' then 'productlist'
        else 'other' end as zone_2,
        coalesce(p2.pro_cnt,0) as  pro_cnt,
        coalesce(p2.open_cnt,0) as  open_cnt,
        coalesce(p2.pro_material_cnt,0) as  pro_material_cnt,
        coalesce(p2.pqrcode_cnt,0) as  pqrcode_cnt,
        coalesce(p2.save_material_cnt,0) as  save_material_cnt,
        coalesce(p2.pro_user_cnt,0) as  pro_user_cnt,
        coalesce(p3.buy_type,'0') as  buy_type,
        coalesce(pay_fee,0) as  pay_fee,
        coalesce(cck_commission,0) as  cck_commission,
        coalesce(pay_count,0) as  pay_count
    from
    (
        select
          b1.product_id as product_id,--商品id
          max(a1.ad_type) as ad_type,--推广类型
          sum(if(a1.module in ('detail','detail_app') and a1.zone='spread',1,0)) as pro_cnt,--推广次数
          sum(if(a1.module in ('detail','detail_app') and a1.zone='enter',1,0)) as open_cnt,--商品详情页打开次数=ipv
          sum(if(a1.module in ('detail_material') and a1.zone='promotion',1,0)) as pro_material_cnt,--
          sum(if(a1.module in ('detail_material') and a1.zone='save',1,0)) as save_material_cnt,
          sum(if(a1.module in ('detail_material') and a1.zone='pqrcode',1,0)) as pqrcode_cnt,
          count(distinct a1.user_id ) as pro_user_cnt
        from
        (
            select
                s1.ad_id as ad_id,--推广id
                s0.app_item_id as product_id--商品id
            from
            (
                select
                    item_id,
                    app_item_id
                from origin_common.cc_ods_dwxk_fs_wk_items
            ) s0
            join
            (
                select
                    ad_id,
                    item_id
                from origin_common.cc_ods_fs_dwxk_ad_items_daily
                where start_time < '${gmtdate_ts}' and end_time >= '${bizdate_ts}'
            ) s1
            on s0.item_id = s1.item_id
        ) b1
        join
        (
            select
                ad_id,--推广id
                ad_type,--推广类型
                module,--模块
                zone,
                user_id--用户id
            from
            (
                select
                    ad_id,--推广id 
                    module,--模块
                    zone,
                    if(module in ('detail','detail_app') and zone='spread', user_id, 0) as user_id,
                    case
                    when ad_type in ('cct-today-top-new.productlist','cct-huge-discount-list.productlist','cct-new-people-buy.productlist','commodity-popular.productlist') then ad_type
                    when ad_type like 'cct-huge-discount-%.bannerlist' then 'cct-huge-discount-bannerlist'
                    when ad_type like 'cct-home-selected-%.bannerlist' then 'cct-home-selected-bannerlist'
                    when ad_type like 'seckill-tab%' then split(ad_type,'\\\\.')[0]
                    else 'other' end as ad_type--推广类型
                from
                    origin_common.cc_ods_log_cctapp_click_hourly
                where
                    ds='${bizdate}' 
                and  
                    module in ('detail','detail_app','detail_material') 
                and  
                    zone in ('spread','enter','promotion','save','pqrcode') 
                and 
                    length(ad_id)>0 
                and 
                    source in ('cct', 'cctui')
            ) t1
            where ad_type!= 'other'
        ) a1
        on b1.ad_id = a1.ad_id
        group by b1.product_id
    ) p2
    full outer join
    (
        select
            t1.product_id as product_id,
            ad_type,
            case
            when source='ccj_cct' then 'promotion_buy'
            when source='cctui' and coalesce(cck_vip_status,0)=1 then 'vip_inner_buy'
            when source='cctui' and coalesce(cck_vip_status,0)=0 then 'no_vip_inner_buy'
            else 'other_buy' end as buy_type,
            sum(item_price) as pay_fee,
            sum(cck_commission) as cck_commission,
            count(*) as pay_count
        from
        (
            select
                third_tradeno,
                cck_uid,
                uid, 
                product_id,
                item_price,
                cck_commission
            from
                origin_common.cc_ods_dwxk_wk_sales_deal_ctime 
            where ds='${bizdate}'
        )t1
        left join
        (
            select
                cct_uid, 
                cck_vip_status
            from
                origin_common.cc_ods_fs_tui_relation
        )b1
        on t1.uid=b1.cct_uid
        join
        (
            select
                cck_uid
            from
                origin_common.cc_ods_dwxk_fs_wk_cck_user
            where
                ds='${bizdate}' and platform=14
        )t0
        on t1.cck_uid=t0.cck_uid
        join
        (
            select
                order_sn,
                source
            from
                origin_common.cc_ods_log_gwapp_order_track_hourly
            where
                ds >= '${bizdate-3}' and ds<='${bizdate}'
        )t4
        on t1.third_tradeno=t4.order_sn
        join
        (
            select
                order_sn,
                product_id,
                max(track_id) as track_id
            from
                origin_common.cc_ods_log_new_order_hourly
            where
                ds >= '${bizdate-3}' and ds<='${bizdate}' and length(track_id)>0
            group by order_sn, product_id
        )t2
        on t1.third_tradeno = t2.order_sn and t1.product_id=t2.product_id
        join
        (
            select
                hash_value,
                case
                when split(track, ':_:')[0] in ('cct-today-top-new.productlist','cct-huge-discount-list.productlist','cct-new-people-buy.productlist','commodity-popular.productlist') then split(track, ':_:')[0]
                when split(track, ':_:')[0] like 'cct-huge-discount-%.bannerlist' then 'cct-huge-discount-bannerlist'
                when split(track, ':_:')[0] like 'cct-home-selected-%.bannerlist' then 'cct-home-selected-bannerlist'
                when split(track, ':_:')[0] like 'seckill-tab%' then split(split(track, ':_:')[0],'\\\\.')[0]
                else 'other' end as ad_type
            from
                origin_common.cc_ods_fs_gwapp_hash_track_hourly
        )t3
        on t2.track_id=t3.hash_value
        group by  t1.product_id, ad_type,
        case
        when source='ccj_cct' then 'promotion_buy'
        when source='cctui' and coalesce(cck_vip_status,0)=1 then 'vip_inner_buy'
        when source='cctui' and coalesce(cck_vip_status,0)=0 then 'no_vip_inner_buy'
        else 'other_buy' end
        having ad_type != 'other'
    )p3
    on p2.product_id =p3.product_id and p2.ad_type=p3.ad_type
) all2
left join
(
    select
        t1.product_id as product_id,
        max(shop_id) as shop_id,
        max(cn_name) as shop_name,
        max(ad_id) as ad_id,
        max(product_name) as product_name,
        max(c1) as c1,
        max(product_cname1) as product_cname1,
        max(ad_price) as ad_price,
        max(discount_fee) as discount_fee,
        max(cck_rate) as cck_rate
    from
    (
        select
            product_id,
            ad_id,
            product_name,
            c1,
            product_cname1,
            ad_price,
            discount_fee,
            cck_rate
        from
            data.cc_dm_ad_product_relation_attr
        where
            ds = '${bizdate}'
    )t1
    join
    (
        select
            shop_id,
            product_id
        from
            origin_common.cc_ods_fs_product
    )t2
    on t1.product_id = t2.product_id
    join
    (
        select
            id,
            cn_name
        from
            origin_common.cc_ods_fs_shop
    )t3
    on t2.shop_id = t3.id
    group by t1.product_id
) all1
on all1.product_id = all2.product_id
left join
(
    select
        product_id,
        case
        when ad_key = 'cct-huge-discount-list' then 'cct-huge-discount-list'
        when ad_key like 'cct-huge-discount-%' then 'cct-huge-discount'
        when ad_key like 'cct-home-selected-%' then 'cct-home-selected'
        when ad_key like 'seckill-tab%' then ad_key
        else  ad_key end as ad_key,
        zone,
        max(t1.ad_material_id) as ad_material_id,
        max(operator) as author,
        max(begin_date) as begin_date,
        max(end_date) as end_date
    from
    (
        select
            ad_material_id, 
            ad_key,
            zone,
            from_unixtime(begin_time,'yyyymmdd') as begin_date,
            from_unixtime(end_time,'yyyymmdd') as end_date
        from
            origin_common.cc_ods_fs_cck_xb_policies_hourly
        where
            begin_time < '${gmtdate_ts}' and  end_time >= '${bizdate_ts}'
    ) t1
    join
    (
        select
            popularize_id,
            product_id,
            operator,
            ad_material_id
        from
            origin_common.cc_ods_fs_cck_ad_material_products_hourly
        where 
            ad_material_id>0
    ) t2
    on t1.ad_material_id=t2.ad_material_id
    group by  product_id,zone,
    case
    when ad_key = 'cct-huge-discount-list' then 'cct-huge-discount-list'
    when ad_key like 'cct-huge-discount-%' then 'cct-huge-discount'
    when ad_key like 'cct-home-selected-%' then 'cct-home-selected'
    when ad_key like 'seckill-tab%' then ad_key
    else ad_key end
) all3
on all1.product_id=all3.product_id and all2.ad_key=all3.ad_key and all2.zone_2=all3.zone
////////////////////////////////////////////////////////////////////////////////////////////
select
    '${bizdate}' as date,
    product_id,
    ad_id,
    product_name,
    c1,
    product_cname1,
    c2,
    product_cname2,
    ad_price,
    discount_fee,
    cck_price,
    cck_rate,
    status,
    shop_id
from
(
    select
        '${bizdate}' as date,
        app_item_id as product_id,
        t1.ad_id as ad_id,
        ad_name as product_name,
        c1,
        product_cname1,
        c2,
        product_cname2,
        ad_price,
        ad_price - money as discount_fee,
        cck_price,
        cck_rate,
        status,
        shop_id,
        row_number() over (partition by app_item_id order by start_time desc) as start_rank
    from
    (
        select
            app_id,
            shop_id,
            item_id,
            app_item_id,
            status
        from
            origin_common.cc_ods_dwxk_fs_wk_items
    ) t0
    join
    (
        select
            ad_id,
            item_id,
            ad_price,
            cck_price,
            cck_rate,
            ad_name,
            start_time,
            end_time,
            c1,
            c2
        from
            origin_common.cc_ods_fs_dwxk_ad_items_daily
        where
            start_time < '${gmtdate_ts}' and end_time >= '${bizdate_ts}'
    ) t1
    on t0.item_id = t1.item_id
    join
    (
        select
            ad_id, 
            money
        from
            origin_common.cc_ods_dwxk_fs_wk_ad_coupon
    ) t2
    on t1.ad_id = t2.ad_id
    left join
    (
        select
            distinct 
            product_c1, 
            product_c2, 
            product_cname1, 
            product_cname2
        from  
            data.cc_dw_fs_products_shops
    ) t3
    on t1.c1 = t3.product_c1 and t1.c2 = t3.product_c2
) all0
where start_rank = 1
