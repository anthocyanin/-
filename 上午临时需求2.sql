一段时间 搜索成交商品信息 
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
    t4.ipv_uv         as ipv_uv，
    t6.buyer_num as buyer_num,
    t6.again_buyer_num as again_buyer_num
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
left join
(
    select
        a1.product_id,
        count(a1.uid) as buyer_num,
        sum(if(a1.num>=2,1,0)) as again_buyer_num
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
)t6
on t1.product_id=t6.product_id
//////////////////////////////////////////////////////////////////////////
一段时间 爆款成交商品信息 
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
    t4.ipv_uv         as ipv_uv，
    t6.buyer_num as buyer_num,
    t6.again_buyer_num as again_buyer_num
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
            ad_type in ('special','seckill-tab-hot.productList','seckill-tab-hot.productList*pay_list') then '爆款'
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
left join
(
    select
        a1.product_id,
        count(a1.uid) as buyer_num,
        sum(if(a1.num>=2,1,0)) as again_buyer_num
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
)t6
on t1.product_id=t6.product_id
//////////////////////////////////////////////////////////////////////////
一段时间 往期成交商品信息 
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
    t4.ipv_uv         as ipv_uv，
    t6.buyer_num as buyer_num,
    t6.again_buyer_num as again_buyer_num
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
            ad_type in ('special','seckill-tab-hot.productList','seckill-tab-hot.productList*pay_list') then '爆款'
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
left join
(
    select
        a1.product_id,
        count(a1.uid) as buyer_num,
        sum(if(a1.num>=2,1,0)) as again_buyer_num
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
)t6
on t1.product_id=t6.product_id
////////////////////////////////////////////////////////////////////
select
    h1.cck_uid as cck_uid,
    from_unixtime(min(h1.create_time)) as create_time
from
(
    select
        t1.cck_uid as cck_uid,
        t1.sale_num as sale_num,
        t1.create_time as create_time,
        sum(t1.sale_num) over(partition by t1.cck_uid order by t1.create_time) as total_sale_num
    from
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
            ds=20181128
            and
            create_time>=1543406400
            and
            create_time<1543420800
            and 
            (status between 1 and 2)
            and 
            product_id=1100185323903
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
            ds>=20181128
            and 
            create_time>=1543406400 
            and 
            status=3 
            and 
            product_id=1100185323903
    )t2
    on t1.third_tradeno=t2.third_tradeno and t1.product_sku_id=t2.product_sku_id
    where t2.third_tradeno is null
)h1
join
(
    select
        cck_uid
    from
        origin_common.cc_ods_dwxk_fs_wk_cck_user
    where
        ds=20181128
        and
        platform=14
)h2
on h1.cck_uid =h2.cck_uid
where h1.total_sale_num>=3
group by h1.cck_uid
//////////////////////////////////////////////////////////////////////////
select
    s1.cck_uid as cck_uid,
    s1.product_id     as product_id,
    s1.product_sku_id as product_sku_id,
    s1.third_tradeno  as order_sn,
    s1.sale_num       as sale_num,
    (s1.cck_commission/100) as cck_commission,
    (s1.item_price/100) as item_price
from
    origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
where 
    s1.ds >= '${begin_date}'
and 
    s1.ds <= '${end_date}'
and 
    s1.cck_uid = 1512849
//////////////////////////////////////////////////////////
颜值 日期 服务经理 推广 销售数据
select
    p1.gm_uid,
    p2.team_num,
    (
    case   
    when p1.gm_uid in (574693,997954,678663,351503,446253,1068772,684796,946772,1088215,581919,376987,263970,353276,353349,612648,614766,589882,769441,330186,919648,540504,614766,376987,541262) then '一战区'
    when p1.gm_uid in (710973,1018891,620072,1117720,537784,453987,360184,255478,419227,459877,1102429,346058,939962,575222,350755,240474,240561,242804,520269,577979) then '二战区'
    when p1.gm_uid in (1199321,1199168,1197475,1202494,1210498,1200648,1204007,1240629,1199210,1209314,1199978,1199305,1199214,1199749,1201288,1205821,1201128,1240633,1199956,1199621,1199365,1204049,1241239,1200483,1252167,1204461,1199985,1199515,1199635,1257637,1227974,1200608,1419045,1242477,1201979,1224204,1200412,1240632,1241385) then '三战区'
    when p1.gm_uid in (721563,760397,1025605,1034136,709705,1175203,1031405,930631,901929,1035670,1034247,720760,815637,744474,736736,1151558,1016400,747406,1001493) then '四战区'
    when p1.gm_uid in (240087,325059,245919,243168,293459,402913,950800,958082,974410,976562,1012885,1022549,1317551,507451,532027,240461,285968,289477,443123,475906,493355,344364,422650,547890,718749,1199257,1245487,278330) then '五战区'
    when p1.gm_uid in (1307543,1342969,1368362,1318636,1406113,1307523,493355,877172) then '七战区'
    when p1.gm_uid in (328580,289477,569834,455813,985271,980086,471845,989050,1133012,816449,472959,399316,239746,940677,1586109,787686,1199257,1245487,1068651,1216371,692018,497607,1113907,1004655,573196,1156866,942532,931097,664703,1225213,790020,831619,551805,416797,415062,501015,1157510,278849,283867,944769,1312806,493355,961545,1140146,240863,261756,1022418,443123,863183,375083,280411,995759,334274,949822,430297,227880) then '九战区'
    else '其他' end
    ) as type,
    p1.ds,
    p1.shared_cck_num as shared_cck_num,
    p1.share_num as share_num,
    p1.click_user_num as click_user_num,
    p1.click_num as click_num,
    p1.order_count   as order_count, --订单数
    p1.sales_num      as sales_num,  --销量
    p1.item_price    as item_price,  --支付金额
    p1.cck_commission as cck_commission --佣金
from
(
    select
        m1.gm_uid,
        m1.ds,
        count(m1.shared_cck) as shared_cck_num,
        sum(m1.share_num) as share_num,
        sum(m2.click_user_num) as click_user_num,
        sum(m2.click_num) as click_num,
        sum(m3.order_count)    as order_count, --订单数
        sum(m3.sales_num)      as sales_num,  --销量
        sum(m3.item_price)     as item_price,  --支付金额
        sum(m3.cck_commission) as cck_commission --佣金
    from 
    (
        select
            t2.ds,
            t1.gm_uid,
            t2.cck_uid as shared_cck,
            t2.share_num
        from 
        (
            select
                distinct
                gm_uid,
                cck_uid
            from 
                cc_ods_fs_wk_cct_layer_info
            where 
                gm_uid in (574693,997954,678663,351503,446253,1068772,684796,946772,1088215,581919,376987,263970,353276,353349,612648,614766,589882,769441,330186,919648,540504,614766,376987,541262,710973,1018891,620072,1117720,537784,453987,360184,255478,419227,459877,1102429,346058,939962,575222,350755,240474,240561,242804,520269,577979,328580,289477,569834,455813,985271,980086,471845,989050,1133012,816449,472959,399316,239746,940677,1586109,787686,1199257,1245487,1068651,1216371,692018,497607,1113907,1004655,573196,1156866,942532,931097,664703,1225213,790020,831619,551805,416797,415062,501015,1157510,278849,283867,944769,1312806,493355,961545,1140146,240863,261756,1022418,443123,863183,375083,280411,995759,334274,949822,430297,227880,1199321,1199168,1197475,1202494,1210498,1200648,1204007,1240629,1199210,1209314,1199978,1199305,1199214,1199749,1201288,1205821,1201128,1240633,1199956,1199621,1199365,1204049,1241239,1200483,1252167,1204461,1199985,1199515,1199635,1257637,1227974,1200608,1419045,1242477,1201979,1224204,1200412,1240632,1241385,721563,760397,1025605,1034136,709705,1175203,1031405,930631,901929,1035670,1034247,720760,815637,744474,736736,1151558,1016400,747406,1001493,240087,325059,245919,243168,293459,402913,950800,958082,974410,976562,1012885,1022549,1317551,507451,532027,240461,285968,289477,443123,475906,493355,344364,422650,547890,718749,1199257,1245487,278330,1307543,1342969,1368362,1318636,1406113,1307523,493355,877172)
            union all
            select
                distinct
                gm_uid,
                gm_uid as cck_uid
            from 
                cc_ods_fs_wk_cct_layer_info
            where 
                gm_uid in (574693,997954,678663,351503,446253,1068772,684796,946772,1088215,581919,376987,263970,353276,353349,612648,614766,589882,769441,330186,919648,540504,614766,376987,541262,710973,1018891,620072,1117720,537784,453987,360184,255478,419227,459877,1102429,346058,939962,575222,350755,240474,240561,242804,520269,577979,328580,289477,569834,455813,985271,980086,471845,989050,1133012,816449,472959,399316,239746,940677,1586109,787686,1199257,1245487,1068651,1216371,692018,497607,1113907,1004655,573196,1156866,942532,931097,664703,1225213,790020,831619,551805,416797,415062,501015,1157510,278849,283867,944769,1312806,493355,961545,1140146,240863,261756,1022418,443123,863183,375083,280411,995759,334274,949822,430297,227880,1199321,1199168,1197475,1202494,1210498,1200648,1204007,1240629,1199210,1209314,1199978,1199305,1199214,1199749,1201288,1205821,1201128,1240633,1199956,1199621,1199365,1204049,1241239,1200483,1252167,1204461,1199985,1199515,1199635,1257637,1227974,1200608,1419045,1242477,1201979,1224204,1200412,1240632,1241385,721563,760397,1025605,1034136,709705,1175203,1031405,930631,901929,1035670,1034247,720760,815637,744474,736736,1151558,1016400,747406,1001493,240087,325059,245919,243168,293459,402913,950800,958082,974410,976562,1012885,1022549,1317551,507451,532027,240461,285968,289477,443123,475906,493355,344364,422650,547890,718749,1199257,1245487,278330,1307543,1342969,1368362,1318636,1406113,1307523,493355,877172)
        ) t1
        inner join 
        (
            select
                n1.ds,
                n2.cck_uid,
                count(n1.user_id) as share_num
            from
            (
                select
                    ds,
                    user_id
                from 
                    origin_common.cc_ods_log_cctapp_click_hourly
                where 
                    ds >= '${begin_date}' 
                and 
                    ds <= '${end_date}'
                and 
                    module = 'detail_material' 
                and 
                    zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC','link_Circle','link_friends','link_copy','small_routine')
            ) n1
            left join 
            (
                select
                    cck_uid,
                    cct_uid
                from 
                    origin_common.cc_ods_fs_tui_relation
            ) n2
            on n1.user_id=n2.cct_uid
            group by 
                n1.ds,n2.cck_uid
        ) t2
        on t1.cck_uid = t2.cck_uid
    ) m1
    left join 
    (
        select
            ds,
            cck_uid,
            count(user_id) as click_num,
            count(distinct user_id) as click_user_num
        from
            origin_common.cc_ods_log_cctui_product_coupon_detail_hourly 
        where
            ds >= '${begin_date}' 
        and 
            ds <= '${end_date}'
        and
            detail_type='item'
        and
            is_in_app = 0
        group by
            ds,cck_uid
    ) m2
    on m1.ds=m2.ds and m1.shared_cck=m2.cck_uid
    left join 
    (
        select
            a1.ds,
            a1.cck_uid,
            count(distinct a1.third_tradeno)    as order_count, --订单数
            sum(a1.sale_num)           as sales_num,  --销量
            sum(a1.item_price/100)     as item_price,  --支付金额
            sum(a1.cck_commission/100) as cck_commission --佣金
        from
        (
            select
                s1.ds,
                s1.cck_uid,
                s1.third_tradeno,
                s1.sale_num,
                s1.item_price,
                s1.cck_commission
            from
                origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
            inner join 
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
                s2.platform=14
        ) a1
        left join
        (
            select 
                distinct 
                order_sn
            from
                origin_common.cc_ods_log_gwapp_order_track_hourly
            where
                ds >= '${begin_date}'
            and
                ds <= '${end_date}'
            and
                source='cctui'
        ) a2
        on 
            a1.third_tradeno = a2.order_sn
        where
            a2.order_sn is null
        group by 
            a1.ds,a1.cck_uid
    ) m3
    on m1.ds=m3.ds and m1.shared_cck=m3.cck_uid
    group by 
        m1.ds,m1.gm_uid
) p1
left join 
(
    select
        t1.gm_uid,
        count(t1.cck_uid) as team_num
    from 
    (
        select
            distinct
            gm_uid,
            cck_uid
        from 
            cc_ods_fs_wk_cct_layer_info
        where 
            gm_uid in (574693,997954,678663,351503,446253,1068772,684796,946772,1088215,581919,376987,263970,353276,353349,612648,614766,589882,769441,330186,919648,540504,614766,376987,541262,710973,1018891,620072,1117720,537784,453987,360184,255478,419227,459877,1102429,346058,939962,575222,350755,240474,240561,242804,520269,577979,328580,289477,569834,455813,985271,980086,471845,989050,1133012,816449,472959,399316,239746,940677,1586109,787686,1199257,1245487,1068651,1216371,692018,497607,1113907,1004655,573196,1156866,942532,931097,664703,1225213,790020,831619,551805,416797,415062,501015,1157510,278849,283867,944769,1312806,493355,961545,1140146,240863,261756,1022418,443123,863183,375083,280411,995759,334274,949822,430297,227880,1199321,1199168,1197475,1202494,1210498,1200648,1204007,1240629,1199210,1209314,1199978,1199305,1199214,1199749,1201288,1205821,1201128,1240633,1199956,1199621,1199365,1204049,1241239,1200483,1252167,1204461,1199985,1199515,1199635,1257637,1227974,1200608,1419045,1242477,1201979,1224204,1200412,1240632,1241385,721563,760397,1025605,1034136,709705,1175203,1031405,930631,901929,1035670,1034247,720760,815637,744474,736736,1151558,1016400,747406,1001493,240087,325059,245919,243168,293459,402913,950800,958082,974410,976562,1012885,1022549,1317551,507451,532027,240461,285968,289477,443123,475906,493355,344364,422650,547890,718749,1199257,1245487,278330,1307543,1342969,1368362,1318636,1406113,1307523,493355,877172)
        union all
        select
            distinct
            gm_uid,
            gm_uid as cck_uid
        from 
            cc_ods_fs_wk_cct_layer_info
        where 
            gm_uid in (574693,997954,678663,351503,446253,1068772,684796,946772,1088215,581919,376987,263970,353276,353349,612648,614766,589882,769441,330186,919648,540504,614766,376987,541262,710973,1018891,620072,1117720,537784,453987,360184,255478,419227,459877,1102429,346058,939962,575222,350755,240474,240561,242804,520269,577979,328580,289477,569834,455813,985271,980086,471845,989050,1133012,816449,472959,399316,239746,940677,1586109,787686,1199257,1245487,1068651,1216371,692018,497607,1113907,1004655,573196,1156866,942532,931097,664703,1225213,790020,831619,551805,416797,415062,501015,1157510,278849,283867,944769,1312806,493355,961545,1140146,240863,261756,1022418,443123,863183,375083,280411,995759,334274,949822,430297,227880,1199321,1199168,1197475,1202494,1210498,1200648,1204007,1240629,1199210,1209314,1199978,1199305,1199214,1199749,1201288,1205821,1201128,1240633,1199956,1199621,1199365,1204049,1241239,1200483,1252167,1204461,1199985,1199515,1199635,1257637,1227974,1200608,1419045,1242477,1201979,1224204,1200412,1240632,1241385,721563,760397,1025605,1034136,709705,1175203,1031405,930631,901929,1035670,1034247,720760,815637,744474,736736,1151558,1016400,747406,1001493,240087,325059,245919,243168,293459,402913,950800,958082,974410,976562,1012885,1022549,1317551,507451,532027,240461,285968,289477,443123,475906,493355,344364,422650,547890,718749,1199257,1245487,278330,1307543,1342969,1368362,1318636,1406113,1307523,493355,877172)
    ) t1
    group by 
        t1.gm_uid
) p2 
on p1.gm_uid=p2.gm_uid
////////////////////////////////////////////////////////////////
cc_ad_material_products_create_time 
cc_ods_fs_dwxk_ad_brand
cc_ods_fs_ad_material_products
cc_product_sku 查商品库存表
////////////////////////////////////////////////////////////////
cc_ods_fs_dwxk_refund_order     0 条数据
cc_ods_fs_dwxk_refund_products  0 条数据

cc_ods_fs_refund_order           12331394 条数据
cc_ods_fs_refund_products        12845869 条数据

cc_refund_order    ds=20181224   12331394 条数据
cc_refund_products ds=20181224   12845869 条数据

/////////////////////////////////////////////////////////////////
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
        distinct
        s1.order_sn,
        s3.product_id,
        s3.sku_id
    from 
    (
        select
            n1.order_sn
        from 
        (
            select 
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
            and 
                t1.refund_reason != 31
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
            order_sn,
            product_id,
            sku_id
        from
            origin_common.cc_refund_products 
        where
            ds = '${end_date}' 
    ) s3
    on s1.order_sn=s3.order_sn
) a2
on a1.third_tradeno=a2.order_sn and a1.product_id=a2.product_id and a1.product_sku_id=a2.sku_id
group by 
    a1.product_id
/////////////////////////////////////////////////////////////////

select
    a1.product_id,
    count(a1.third_tradeno) as pay_count_30d,--30日订单数
    count(a2.order_sn) as refund_count_30d--30日发货后退款数
from
(
    select
        s1.product_id,
        s1.item_price,
        s1.cck_commission,
        s1.third_tradeno,
        s1.create_time
    from
    (
        select
            product_id,
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
        distinct
        s1.order_sn
    from 
    (
        select
            n1.order_sn
        from 
        (
            select 
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
            and 
                t1.refund_reason != 31
        ) n1
        where n1.status=1
    )s1
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
    )s2
    on s1.order_sn=s2.order_sn
) a2
on a1.third_tradeno=a2.order_sn
group by 
    a1.product_id
////////////////////////////////////////////////////
select 
*
from
    origin_common.cc_items_rates 
where
    ds = 20181215
and
    id in (64811947,64811948,64811949,64811950,64811951,64811952)
////////////////////////////////////////////////////

select
    a1.product_id,
    sum(a1.item_price/100) as pay_fee,
    sum(a1.cck_commission) as cck_commission,
    count(distinct a1.third_tradeno) as order_num
from
(
    select
        s1.product_id,
        s1.item_price,
        s1.cck_commission,
        s1.third_tradeno,
        s1.create_time
    from
    (
        select
            product_id,
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
        distinct
        order_sn,
        rate_id,
        star_num,
        product_id
    from
        origin_common.cc_rate_star
    where
        ds >= '${begin_date_30d}'
    and 
        ds <= '${end_date}' 
    and
        rate_id != 0
    and
        order_sn!='170213194354LFo017564wk'
) a3
on a1.third_tradeno=a3.order_sn and a1.product_id = a3.product_id
group by 
    a1.product_id
//////////////////////////////////////////////////////////////////////////////
下单表限制楚楚推的写法
select
    '${bizdate}'                              as date,
    count(distinct g1.shop_id)                as saled_shop_cnt,
    count(distinct g1.user_id)                as pay_user_cnt,
    cast(sum(g1.total_fee) as decimal(20,2))  as total_fee
from
(
    select
        order_sn,
        user_id,
        shop_id,
        total_fee
    from 
        origin_common.cc_order_user_pay_time
    where 
        ds='${bizdate}'
) g1
inner join
(
    select
        distinct 
        order_sn as order_sn,
        split(order_trace,':')[1] as cck_uid
    from 
        origin_common.cc_order_products_user_add_time
    where 
        ds='${bizdate}' 
    and 
        order_trace!=''
) g2
on g1.order_sn=g2.order_sn
inner join
(
    select
        cck_uid
    from 
        origin_common.cc_ods_dwxk_fs_wk_cck_user
    where 
        ds='${bizdate}' 
    and 
        platform=14
) g3
on g2.cck_uid=g3.cck_uid
//////////////////////////////////////////////////////////////////
徐冲供应商系统所有商品供货价(实际结果是会有600多个为null)
select
    t1.pm_sid as shop_id,
    t1.pm_pid as product_id,
    t1.pm_title as product_title,
    t3.product_cname1,
    t3.product_cname2,
    t3.product_cname3,
    t2.pb_stock as product_stock,
    t2.pb_price as product_price,
    t2.pb_batch as product_batch,
    t2.pb_status as product_status,
    from_unixtime(t2.pb_ctime, 'yyyyMMdd HH:mm:ss') as product_ctime
from
(
    select
        pm_sid,--店铺ID
        pm_pid,--商品ID
        pm_title,--商品名称
        pm_mpid
    from 
        cc_ods_fs_op_products_map
    where 
        pm_sid in (19347,18532,18164,18335,17801,19268,19141)
)t1
left join
(
    select 
        pb_mpid,
        pb_stock,
        pb_price,
        pb_batch,
        pb_status,
        pb_ctime
    from 
        cc_ods_fs_op_product_batches
    where 
        pb_mpid>0
)t2
on t1.pm_mpid=t2.pb_mpid
left join 
(
    select
        product_id,--商品id
        product_cname1,
        product_cname2,
        product_cname3
    from 
        data.cc_dw_fs_products_shops
) t3
on t1.pm_pid=t3.product_id

//////////////////////////////////////////////////////////////////
上官形形
select
    ds,
    uid,
    sum(item_price/100) as pay_fee,
    count(distinct third_tradeno) as order_count,
    sum(cck_commission/100) as cck_commission
from
    origin_common.cc_ods_dwxk_wk_sales_deal_ctime
where
    ds >= 20181227
and
    ds <= 20181229
group by 
    ds,uid

////////////////////////////////////////////////////////////////////////////////////////////////
######VIP id、姓名、手机号、订单数、支付金额、销量、最后一个订单创建时间（MySQL）
考考数据需求
select
    distinct
    t1.cck_uid,
    t3.real_name,
    t2.phone_number,
    t1.pay_count,
    t1.product_num,
    t1.fee
from
(
    select
        cck_uid,
        count(distinct third_tradeno) as pay_count,
        sum(item_price/100) as fee,
        sum(sale_num) as product_num
    from
        wk_sales_deal
    where
            create_time>=1548172800
            and
            create_time<1548259200
            and
            product_id in (1100185325472)
            and
            status in(1,2)
        group by
            cck_uid
)t1
left join
(
    select
        cck_uid,
        phone_number
    from
        wk_cck_user
)t2
on t1.cck_uid =t2.cck_uid
left join
(
    select
        cck_uid,
        real_name
    from
        wk_business_info
    union all
    select
        cck_uid,
        real_name
    from
        wk_region_user
)t3
on t1.cck_uid=t3.cck_uid
order by t1.sale_num desc
////////////////////////////////////////////////////////////////////////////////////////////////
考考订单明细
select
    t1.cck_uid,
    t3.real_name,
    t2.phone_number,
    t1.third_tradeno,
    t1.sale_num,
    t1.item_price
from
(
    select
        cck_uid,
        third_tradeno,
        item_price,
        sale_num as sale_num
    from
        wk_sales_deal
    where
        create_time>=1548172800
    and
        create_time<1548259200
    and
        product_id in (1100185325472)
    and
        status in(1,2)
)t1
left join
(
    select
        cck_uid,
        phone_number
    from
        wk_cck_user
)t2
on t1.cck_uid =t2.cck_uid
left join
(
    select
        cck_uid,
        real_name
    from
        wk_business_info
    union all
    select
        cck_uid,
        real_name
    from
        wk_region_user
)t3
on t1.cck_uid=t3.cck_uid
////////////////////////////////////////////////////////////////////////////////////////////////



////////////////////////////////////////////////////////////////////////////////////////////////
select
    cck_uid, 
    uid,
    product_id,
    product_sku_id, 
    third_tradeno, 
    item_price,
    cck_commission,
    sale_num,
    status
from
    wk_sales_deal
where
    create_time>=1545926400
and
    create_time<1546012800
and
    product_id = 1100185325472
and
    status in(1,2)
and
    cck_uid=1929875
////////////////////////////////////////////////////////////////////////////////////////////////
楚楚推30日商品成交 退款数据 
--cc_ods_dwxk_wk_sales_deal_ctime 表里一个订单包含4个sku_id,假设此订单退款了，则
--cc_ods_fs_refund_order表里面 记录一次:一个order_sn，一个refund_sn,--但是0221发现订单号相同，refund_sn是不同的啊，也就是会存4次的。
--cc_refund_product表里面 记录四次：四个相同的order_sn，四个refund_sn，四个不同的sku_id.
select
    a1.product_id,
    sum(a1.item_price/100) as item_price,
    count(a1.third_tradeno) as pay_count_30d,--30日订单数
    count(a2.order_sn) as refund_count_30d,--30日退款数
    sum(a2.success_price) as refund_price--30退款金额
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
            ds >='${begin_date_30d}'
        and
            ds <='${end_date}' 
    ) s1
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
    ) s2
    on s1.cck_uid=s2.cck_uid
) a1
left join
(
    select 
        distinct
        s1.refund_sn,
        s1.order_sn,
        (s1.success_price/s3.num) as success_price,
        s3.product_id,
        s3.sku_id
    from 
    (
        select
            n1.refund_sn,
            n1.order_sn,
            n1.success_price
        from 
        (
            select
                t1.refund_sn, 
                t1.order_sn,
                t1.success_price,
                if(t1.status=2,if(t1.success_price=0,0,1),t1.status) as status
            from
                origin_common.cc_ods_fs_refund_order t1
            where
                from_unixtime(t1.create_time,'yyyyMMdd') >= '${begin_date_30d}' 
            and 
                from_unixtime(t1.create_time,'yyyyMMdd') <= '${end_date2}' 
            and 
                t1.status in (1,2)
        ) n1
        where n1.status=1
    ) s1
    left join 
    (
        select
            n1.refund_sn, 
            n1.order_sn,
            n1.product_id,
            n1.sku_id,
            n2.num
        from 
        (
            select
                refund_sn, 
                order_sn,
                product_id,
                sku_id
            from
                origin_common.cc_refund_products 
            where
                ds = '${end_date2}' 
        ) n1
        left join
        (
            select
                refund_sn, 
                count(sku_id) as num
            from
                origin_common.cc_refund_products 
            where
                ds = '${end_date2}' 
            group by 
                refund_sn

        ) n2
        on n1.refund_sn = n2.refund_sn
    ) s3
    on s1.refund_sn=s3.refund_sn
) a2
on a1.third_tradeno=a2.order_sn and a1.product_id=a2.product_id and a1.product_sku_id=a2.sku_id
group by 
    a1.product_id
///////////////////////////////////////////////////////////////////////////////////////
Grace 服饰Q4销售额
select
    s3.product_cname1,
    sum(s1.item_price/100) as pay_fee,
    sum(s1.cck_commission/100) as cck_commission
from
(
    select
        product_id,
        item_price,
        cck_commission,
        cck_uid
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where
        ds >= '${begin_date}'
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
inner join 
(
    select
        distinct
        product_id,
        shop_id,
        product_cname1
    from 
        data.cc_dw_fs_products_shops
    where 
        product_cname1 in ('箱包','女装','运动户外','男装','配饰','鞋靴','女士内衣/男士内衣/家居服')
    union all
    select
        distinct
        product_id,
        shop_id,
        '童装童鞋' as product_cname1
    from 
        data.cc_dw_fs_products_shops
    where 
        product_cname1 = '母婴' and product_cname2 in ('童装/亲子装','婴童鞋/亲子鞋')
)s3
on s1.product_id=s3.product_id
group by 
    s3.product_cname1
////////////////////////////////////////////////////////////////////////
张梨娜 楚楚推服饰所有商品信息
select
    m1.product_id,
    m1.product_title,
    m1.product_cname1,
    m1.product_cname2,
    m1.product_cname3,
    m1.shop_id,
    m1.shop_title,
    m2.max_ad_price,--券前价格 
    m2.min_ad_price--券前价格 
from 
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
    where 
        shop_id in (19211,18112,19370,19412,18907,18250,18927,19561,19507,17702,19434,19700,19636,12964,16531,3533,18719,8339,19570,19766,19908,19697,18682,12704,19330,19767,7780,20233,19884,20201,19426,20243,10064,19698,20294,20260,20325,20350,19600,20335,19595,18432,17485,18873,19209,17300,20401,20205,17516,20399,20481,20499,20413,11568,20493,20427,20443,20500,20546,3646,20492,20505,19651,20506,9974,20545,20539,20541,20573,20411,20534,18552,11520,18744,20336,18512,20604,19338,19675,19596,20676,20648,20614,20719,20689,20754,20812,20923,20916,20558,20937,20939,20806,20524,20965,18053,8935,20931,20860,20751,22605,22361,13589,20940,22452,22463,18071,22728,22747,22760,20777,22819,22799,17563,20688,22751,22451,13098,22800,22917,14686,23024,23137,18911,18860,18722,18611,18323,18714,18683,18649,18686,18676,18693,18692,18608,18674,18673,18684,18398,18633,18472,9565,18575,18625,18590,18374,18595,18576,18494,18533,18579,18546,18510,18547,18518,12502,18471,18526,14661,2776,18516,18501,18467,18243,18492,19127,18878,14560,17114,16907,10338,17200,18065,15279,18217,17815,16315,13278,17624,18226,17684,18224,17839,17697,18007,16439,17218,8036,17582,10668,17698,17699,17819,17686,14359,17461,17888,15801,18117,18197,17726,17947,9601,17692,18255,17884,18309,8839,11760,14515,11677,17428,1374,17957,9241,5529,17944,16717,17896,8032,17500,14975,13352,16819,18304,11184,13896,16530,18285,18142,4128,8106,17705,5138,8089,5666,18091,16510,9151,8481,7479,533,17690,9238,16270,16567,6521,18330,1796,17704,17769,16561,4121,17653,17951,17531,10078,15811,10141,18417,11548,17850,13773,10036,17902,17851,17982,17597,8270,17405,8622,18317,18238,9912,17315,17950,2995,18382,17005,17920,10878,18161,9665,17757,18385,18133,11646,14502,266,16785,10639,168,14390,6621,12545,18174,17776,18145,16540,18338,16355,12846,9950,12854,7199,16853,16741,16286,11137,17167,16687,12924,17349,17981,17645,16878,17707,16375,16581,8756,17768,17644,17137,17441,17522,17693,18080,17926,18123,18210,11279,17483,17748,17501,15519,10097,13805,6427,18294,18394,12889,9961,11449,8553,9691,14005,15688,17172,17152,11445,17012,11237,7647,10671,9570,16650,3826,17337,965,17490,18035,11845,182,9342,18937,19171,19277,18288,19284,17356,17931,20471,18532,19268,19347,18164,18491,18765,18574,20392,20423,20770,22853,22881,22607,18551,23179,17803,18871,19641,20667,22955,9304,23633,17489,23246,23384,23634,23708,22699,17115,18479,23721,23643,23794,23801,23599,23783,23736,23632,23891,23904,23928,23931,15159,23961,19995,18840,23934,23924,23809,20400)
    and
        product_cname1 in ('箱包','女装','运动户外','男装','配饰','鞋靴','女士内衣/男士内衣/家居服')
    union all
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
    where 
        shop_id in (19211,18112,19370,19412,18907,18250,18927,19561,19507,17702,19434,19700,19636,12964,16531,3533,18719,8339,19570,19766,19908,19697,18682,12704,19330,19767,7780,20233,19884,20201,19426,20243,10064,19698,20294,20260,20325,20350,19600,20335,19595,18432,17485,18873,19209,17300,20401,20205,17516,20399,20481,20499,20413,11568,20493,20427,20443,20500,20546,3646,20492,20505,19651,20506,9974,20545,20539,20541,20573,20411,20534,18552,11520,18744,20336,18512,20604,19338,19675,19596,20676,20648,20614,20719,20689,20754,20812,20923,20916,20558,20937,20939,20806,20524,20965,18053,8935,20931,20860,20751,22605,22361,13589,20940,22452,22463,18071,22728,22747,22760,20777,22819,22799,17563,20688,22751,22451,13098,22800,22917,14686,23024,23137,18911,18860,18722,18611,18323,18714,18683,18649,18686,18676,18693,18692,18608,18674,18673,18684,18398,18633,18472,9565,18575,18625,18590,18374,18595,18576,18494,18533,18579,18546,18510,18547,18518,12502,18471,18526,14661,2776,18516,18501,18467,18243,18492,19127,18878,14560,17114,16907,10338,17200,18065,15279,18217,17815,16315,13278,17624,18226,17684,18224,17839,17697,18007,16439,17218,8036,17582,10668,17698,17699,17819,17686,14359,17461,17888,15801,18117,18197,17726,17947,9601,17692,18255,17884,18309,8839,11760,14515,11677,17428,1374,17957,9241,5529,17944,16717,17896,8032,17500,14975,13352,16819,18304,11184,13896,16530,18285,18142,4128,8106,17705,5138,8089,5666,18091,16510,9151,8481,7479,533,17690,9238,16270,16567,6521,18330,1796,17704,17769,16561,4121,17653,17951,17531,10078,15811,10141,18417,11548,17850,13773,10036,17902,17851,17982,17597,8270,17405,8622,18317,18238,9912,17315,17950,2995,18382,17005,17920,10878,18161,9665,17757,18385,18133,11646,14502,266,16785,10639,168,14390,6621,12545,18174,17776,18145,16540,18338,16355,12846,9950,12854,7199,16853,16741,16286,11137,17167,16687,12924,17349,17981,17645,16878,17707,16375,16581,8756,17768,17644,17137,17441,17522,17693,18080,17926,18123,18210,11279,17483,17748,17501,15519,10097,13805,6427,18294,18394,12889,9961,11449,8553,9691,14005,15688,17172,17152,11445,17012,11237,7647,10671,9570,16650,3826,17337,965,17490,18035,11845,182,9342,18937,19171,19277,18288,19284,17356,17931,20471,18532,19268,19347,18164,18491,18765,18574,20392,20423,20770,22853,22881,22607,18551,23179,17803,18871,19641,20667,22955,9304,23633,17489,23246,23384,23634,23708,22699,17115,18479,23721,23643,23794,23801,23599,23783,23736,23632,23891,23904,23928,23931,15159,23961,19995,18840,23934,23924,23809,20400)
    and
        product_cname1 = '母婴' and product_cname2 in ('童装/亲子装','婴童鞋/亲子鞋')
) m1
left join 
(
    select
        a1.app_item_id        as product_id,--商品id
        max(a2.ad_price/100)  as max_ad_price,--券前价格 
        min(a2.ad_price/100)  as min_ad_price
    from
    (
        select 
            item_id, 
            app_item_id--商品id
        from 
            cc_ods_dwxk_fs_wk_items
    ) a1
    left join 
    (
        select
            item_id, 
            ad_price--券前价格  
        from 
            cc_ods_fs_dwxk_ad_items_daily
    ) a2 
    on a1.item_id = a2.item_id
    group by 
        a1.app_item_id
) m2
on m1.product_id=m2.product_id
////////////////////////////////////////////////////////////////////////
张梨娜 楚楚推服饰2018全年销售额
select
    s3.product_id,
    count(distinct s1.third_tradeno) as order_count,
    sum(s1.sale_num) as sales_num,
    sum(s1.item_price/100) as pay_fee,
    sum(s1.cck_commission/100) as cck_commission
from
(
    select
        product_id,
        third_tradeno,
        sale_num,
        item_price,
        cck_commission,
        cck_uid
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where
        ds >= '${begin_date}'
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
inner join 
(
    select
        product_id
    from 
        data.cc_dw_fs_products_shops
    where 
        shop_id in (19211,18112,19370,19412,18907,18250,18927,19561,19507,17702,19434,19700,19636,12964,16531,3533,18719,8339,19570,19766,19908,19697,18682,12704,19330,19767,7780,20233,19884,20201,19426,20243,10064,19698,20294,20260,20325,20350,19600,20335,19595,18432,17485,18873,19209,17300,20401,20205,17516,20399,20481,20499,20413,11568,20493,20427,20443,20500,20546,3646,20492,20505,19651,20506,9974,20545,20539,20541,20573,20411,20534,18552,11520,18744,20336,18512,20604,19338,19675,19596,20676,20648,20614,20719,20689,20754,20812,20923,20916,20558,20937,20939,20806,20524,20965,18053,8935,20931,20860,20751,22605,22361,13589,20940,22452,22463,18071,22728,22747,22760,20777,22819,22799,17563,20688,22751,22451,13098,22800,22917,14686,23024,23137,18911,18860,18722,18611,18323,18714,18683,18649,18686,18676,18693,18692,18608,18674,18673,18684,18398,18633,18472,9565,18575,18625,18590,18374,18595,18576,18494,18533,18579,18546,18510,18547,18518,12502,18471,18526,14661,2776,18516,18501,18467,18243,18492,19127,18878,14560,17114,16907,10338,17200,18065,15279,18217,17815,16315,13278,17624,18226,17684,18224,17839,17697,18007,16439,17218,8036,17582,10668,17698,17699,17819,17686,14359,17461,17888,15801,18117,18197,17726,17947,9601,17692,18255,17884,18309,8839,11760,14515,11677,17428,1374,17957,9241,5529,17944,16717,17896,8032,17500,14975,13352,16819,18304,11184,13896,16530,18285,18142,4128,8106,17705,5138,8089,5666,18091,16510,9151,8481,7479,533,17690,9238,16270,16567,6521,18330,1796,17704,17769,16561,4121,17653,17951,17531,10078,15811,10141,18417,11548,17850,13773,10036,17902,17851,17982,17597,8270,17405,8622,18317,18238,9912,17315,17950,2995,18382,17005,17920,10878,18161,9665,17757,18385,18133,11646,14502,266,16785,10639,168,14390,6621,12545,18174,17776,18145,16540,18338,16355,12846,9950,12854,7199,16853,16741,16286,11137,17167,16687,12924,17349,17981,17645,16878,17707,16375,16581,8756,17768,17644,17137,17441,17522,17693,18080,17926,18123,18210,11279,17483,17748,17501,15519,10097,13805,6427,18294,18394,12889,9961,11449,8553,9691,14005,15688,17172,17152,11445,17012,11237,7647,10671,9570,16650,3826,17337,965,17490,18035,11845,182,9342,18937,19171,19277,18288,19284,17356,17931,20471,18532,19268,19347,18164,18491,18765,18574,20392,20423,20770,22853,22881,22607,18551,23179,17803,18871,19641,20667,22955,9304,23633,17489,23246,23384,23634,23708,22699,17115,18479,23721,23643,23794,23801,23599,23783,23736,23632,23891,23904,23928,23931,15159,23961,19995,18840,23934,23924,23809,20400)
    and
        product_cname1 in ('箱包','女装','运动户外','男装','配饰','鞋靴','女士内衣/男士内衣/家居服')
    union all
    select
        product_id
    from 
        data.cc_dw_fs_products_shops
    where 
        shop_id in (19211,18112,19370,19412,18907,18250,18927,19561,19507,17702,19434,19700,19636,12964,16531,3533,18719,8339,19570,19766,19908,19697,18682,12704,19330,19767,7780,20233,19884,20201,19426,20243,10064,19698,20294,20260,20325,20350,19600,20335,19595,18432,17485,18873,19209,17300,20401,20205,17516,20399,20481,20499,20413,11568,20493,20427,20443,20500,20546,3646,20492,20505,19651,20506,9974,20545,20539,20541,20573,20411,20534,18552,11520,18744,20336,18512,20604,19338,19675,19596,20676,20648,20614,20719,20689,20754,20812,20923,20916,20558,20937,20939,20806,20524,20965,18053,8935,20931,20860,20751,22605,22361,13589,20940,22452,22463,18071,22728,22747,22760,20777,22819,22799,17563,20688,22751,22451,13098,22800,22917,14686,23024,23137,18911,18860,18722,18611,18323,18714,18683,18649,18686,18676,18693,18692,18608,18674,18673,18684,18398,18633,18472,9565,18575,18625,18590,18374,18595,18576,18494,18533,18579,18546,18510,18547,18518,12502,18471,18526,14661,2776,18516,18501,18467,18243,18492,19127,18878,14560,17114,16907,10338,17200,18065,15279,18217,17815,16315,13278,17624,18226,17684,18224,17839,17697,18007,16439,17218,8036,17582,10668,17698,17699,17819,17686,14359,17461,17888,15801,18117,18197,17726,17947,9601,17692,18255,17884,18309,8839,11760,14515,11677,17428,1374,17957,9241,5529,17944,16717,17896,8032,17500,14975,13352,16819,18304,11184,13896,16530,18285,18142,4128,8106,17705,5138,8089,5666,18091,16510,9151,8481,7479,533,17690,9238,16270,16567,6521,18330,1796,17704,17769,16561,4121,17653,17951,17531,10078,15811,10141,18417,11548,17850,13773,10036,17902,17851,17982,17597,8270,17405,8622,18317,18238,9912,17315,17950,2995,18382,17005,17920,10878,18161,9665,17757,18385,18133,11646,14502,266,16785,10639,168,14390,6621,12545,18174,17776,18145,16540,18338,16355,12846,9950,12854,7199,16853,16741,16286,11137,17167,16687,12924,17349,17981,17645,16878,17707,16375,16581,8756,17768,17644,17137,17441,17522,17693,18080,17926,18123,18210,11279,17483,17748,17501,15519,10097,13805,6427,18294,18394,12889,9961,11449,8553,9691,14005,15688,17172,17152,11445,17012,11237,7647,10671,9570,16650,3826,17337,965,17490,18035,11845,182,9342,18937,19171,19277,18288,19284,17356,17931,20471,18532,19268,19347,18164,18491,18765,18574,20392,20423,20770,22853,22881,22607,18551,23179,17803,18871,19641,20667,22955,9304,23633,17489,23246,23384,23634,23708,22699,17115,18479,23721,23643,23794,23801,23599,23783,23736,23632,23891,23904,23928,23931,15159,23961,19995,18840,23934,23924,23809,20400)
    and
        product_cname1 = '母婴' and product_cname2 in ('童装/亲子装','婴童鞋/亲子鞋')
)s3
on s1.product_id=s3.product_id
group by 
    s3.product_id

////////////////////////////////////////////////////////////////////////
崔丹
select
    cck_uid,
    from_unixtime(create_time,'yyyyMMdd HH:mm:ss') as create_time
from 
    origin_common.cc_ods_fs_wk_cct_layer_info
where 
    leader_uid in (4775,7070,29211,36200,36467,38743)
union all
select
    leader_uid as cck_uid,
    from_unixtime(leader_ctime,'yyyyMMdd HH:mm:ss') as create_time
from 
    origin_common.cc_ods_fs_wk_cct_layer_info
where 
    leader_uid in (4775,7070,29211,36200,36467,38743)

////////////////////////////////////////////////////////////////////////////////
楚楚街商家清单 此代码不对
select
    t1.shop_id,
    t2.shop_name,
    t1.create_time,
    t2.company_name,
    t3.apply_reason,
    t3.source
from
(
    select
        app_shop_id as shop_id,
        app_shop_name as shop_name,
        create_time
    from 
        origin_common.cc_ods_fs_ccj_shop
) t1
left join
(
    select
        shop_id,
        company_name
    from 
        origin_common.cc_ods_fs_business_basic
) t2
on t1.shop_id=t2.shop_id
left join 
(
    select
        shop_id,
        apply_reason,
        source
    from
        origin_common.cc_ods_fs_shop_close
    where 
        status = 'close' 
) t3
on t1.shop_id=t3.shop_id
//////////////////////////////////////////////////////////////
应是此代码
select
    id,
    cn_name,
    status,--0开着，1关店
from
(
    select
        id,
        cn_name,
        status,--0开着，1关店
    from 
        cc_shop 
    where platform = 1
) n1
left join 
(
    select
        cid
    from 
        cc_white_manger 
    where 
        type =1 and flag = 6 
) n2
on n1.id = n2.cid
where n2.cid is null 
//////////////////////////////////////////////////////////////
每周在线上新数据
select
    m1.shop_id,
    m2.shop_title,
    m1.online_prd_cnt,
    m1.new_prd_cnt
from 
(
    select
        app_shop_id as shop_id,
        count(distinct ad_id) as online_prd_cnt,
        sum(if(start_time>='${bizdate_ts}' and start_time<'${gmtdate_ts}',1,0)) as new_prd_cnt
    from 
        origin_common.cc_ods_fs_dwxk_ad_items_daily
    where 
        audit_status=1 
    and 
        status>0 
    and 
        start_time<'${gmtdate_ts}' 
    and 
        end_time>='${bizdate_ts}'
    group by 
        app_shop_id
) m1
left join 
(
    select
        distinct
        shop_id,
        shop_title
    from
        data.cc_dw_fs_products_shops
) m2
on m1.shop_id = m2.shop_id
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
每周在线数据
select
    m1.shop_id,
    m2.shop_title,
    m1.app_item_id,
    m2.product_title,
    m2.product_cname1,
    m2.product_cname2,
    m2.product_cname3
from
(
    select
        n1.shop_id,
        n2.app_item_id
    from
    (
        select
            app_shop_id as shop_id,
            ad_id,
            item_id    
        from 
            origin_common.cc_ods_fs_dwxk_ad_items_daily
        where 
            audit_status=1 
        and 
            status>0 
        and 
            start_time<unix_timestamp('${end_date}','yyyyMMdd') 
        and 
            end_time>=unix_timestamp('${begin_date}','yyyyMMdd')
        and 
            app_shop_id != 18636
    ) n1
    left join 
    (
         select 
            item_id, 
            app_item_id--商品id
        from 
            cc_ods_dwxk_fs_wk_items
    ) n2
    on n1.item_id = n2.item_id
) m1
left join 
(
    select
        product_id,
        product_title,
        product_cname1,
        product_cname2,
        product_cname3,
        shop_title
    from
        data.cc_dw_fs_products_shops
) m2
 on m1.app_item_id = m2.product_id
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
每周上新数据
select
    m1.shop_id, 
    m2.shop_title,
    m1.app_item_id,
    m2.product_title,
    m2.product_cname1,
    m2.product_cname2,
    m2.product_cname3
from
(
    select
        n1.shop_id,
        n2.app_item_id
    from
    (
        select
            app_shop_id as shop_id,
            ad_id,
            item_id
        from 
            origin_common.cc_ods_fs_dwxk_ad_items_daily
        where 
            audit_status=1 
        and 
            status>0 
        and 
            start_time>=unix_timestamp('${begin_date}','yyyyMMdd')
        and 
            start_time<=unix_timestamp('${end_date}','yyyyMMdd')
        and 
            app_shop_id != 18636
    ) n1
    inner join 
    (
        select 
            item_id, 
            app_item_id--商品id
        from cc_ods_dwxk_fs_wk_items
    ) n2
    on n1.item_id = n2.item_id
) m1
left join 
(
    select
        product_id,
        product_title,
        product_cname1,
        product_cname2,
        product_cname3,
        shop_title
    from
        data.cc_dw_fs_products_shops
) m2
on m1.app_item_id = m2.product_id
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
select
    s1.product_id as product_id,
    s1.product_sku_id as product_sku_id,
    s1.third_tradeno as third_tradeno,
    (s1.discount_fee/100) as discount_fee, 
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
and 
    s1.product_id = 11002077059
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
张梨娜 
select
    t1.cck_uid as cck_uid,
    t1.uid as uid,
    t1.product_id as product_id,
    t1.third_tradeno as third_tradeno,
    t1.sale_num as sale_num,
    t1.cck_commission,
    t1.item_price,
    t1.create_time,
    t2.order_sn
from
(
    select
        s1.cck_uid as cck_uid,
        s1.uid as uid,
        s1.product_id as product_id,
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
        s1.product_id = 110022607100
    and
        s2.ds= '${state_date}'
    and
        s2.platform = 14
) t1
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
) t2
on t1.third_tradeno=t2.order_sn
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
张梨娜
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


/////////////////////////////////////////////////////////////////////
东哥 店铺数据
select
    t1.shop_id,
    t1.shop_name, 
    t3.shop_cname1,
    t1.company_name, 
    t1.real_name, 
    t1.phone_number, 
    t1.create_time,
    t2.shop_id,
    t2.apply_reason,
    t2.source,
    t2.insert_date
from 
(
    select
        real_name, 
        shop_id,
        shop_name, 
        company_name, 
        phone_number, 
        from_unixtime(create_time,'yyyyMMdd HH:mm:ss') as create_time
    from 
        cc_ods_fs_business_basic
    where
        shop_id in (19144,19356,19125,19304,18281,20795,19367,19308,19658,19659,19673,19661,19672,20257,19617,18976,22571,20772,23805,20465,18985,23250,18993,18991,22989,19231,19155,19464,20176,20437,18288,19410,20859,19013,22779,23335,20317,19604,20093,19446,19903,22816,20136,22562,23552,20109,20877,23251,17022,20848,19276,19213,20938,19910,20091,22547,19373,20858,19121,23995,19065,19462,18971,20057,19189,18906,19491,24057,20669,20874,18984,23968,24002,19913,19028,19381,18532,19618,19005,20849,20255,19340,18880,19109,19688,23988,19006,22678,19242,20097,20253,18963,19500,19318,20422,20844,19047,20655,24014,19887,20638,20444,19399,18994,23600,23820,19385,20101,19327,18849,23315,20004,20852,16567,20261,19044,18436,19448,18972,21210,24026,20518,19216,19110,20925,18164,22122,20528,19437,23657,19122,19493,19288,19112,20248,20592,19650,18966,221,19386,22595,20813,20515,19541,19657,19764,22845,23654,19195,20703,20487,19481,18738,20863,19471,19323,20533,19017,19134,19076,20406,20762,19038,19461,19365,19177,20932,19655,17226,23868,23876,18941,19070,18987,22548,19686,23744,24024,18983,23517,18998,20904,21666,22959,19689,18989,19326,23925,19037,18872,20851,19103,19034,19106,19357,20873,20624,23806,23965,23535,19389,20563,20865,19346,19043,22602,19488,19023,19514,19146,19199,18859,19055,22887,19097,19322,19364,19358,19039,19377,18760,19363,24031,19018,19072,20817,19359,22949,19031,22728,19256,19598,18599,19021,23651,20910,19706,20316,18332,22971,20880,19362,19597,22781,22692,19499,22473,22972,19292,20096,20104,18500,22783,19424,19431,19660,20670,20764,23644,20366,24004,19036,22821,19265,23923,20235,22840,19152,19048,20102,19544,19552,24022,20542,19004,19283,20138,20292,19201,20177,19454,20776,19508,19397,19237,19894,19749,18986,20540,20791,16896,24034,20512,19291,19096,18982,19905,18950,19299,18964,20894,20207,20485,19524,19261,19306,20862,22774,19716,16375,19269,19490,19297,19396,19194,19311,20475,19710,19111,20914,20871,19674,18973,20100,19058,20775,23645,20929,21395,19400,18442,19069,19567,19041,19376,19559,20510,18967,19757,19414,20482,19663,23016,20878,19946,21214,23791,22822,19513,19665,15318,20887,23293,19360,20796,19432,19267,19452,19182,20616,23049,18948,19479,19444,18612,19116,20866,19904,22852,20192,19252,20936,22596,19321,19430,19495,23991,19148,20895,18975,20704,20028,19566,18905,19375,19463,18965,19166,23054,18995,19310,20620,18654,21425,19351,19071,19403,19529,20258,23601,20352,23870,19355,19264,19563,18977,20845,20876,20514,19458,19221,20088,21430,23743,20822,19476,18674,18853,19056,20757,23779,20934,19051,23236,18978,19008,19032,20008,23017,20905,19307,19629,19656,18981,18857,20087,19480,20167,20133,19545,19531,19485,19275,19387,20827,20868,19030,19161,22944,19108,19053,20110,18835,24029,19366,19361,19554,19455,20690,20504,20504,19040,20808,19244,20696,19266,18718,19382,19024,20678,22969,24037,19025,20152,19492,19062,21367,20273,19733,19336,20492,20679,19019,19052,19420,23903,24021,19077,23746,23385,19305,20945,24013,19100,19243,19259,19451,19234,19225,19226,19220,19215,19223,18572,19260,19217,19214,19098,19218,19247,19219,18868,23989,20368,19091,20309,19012,19395,19473,19150,18508,20897,18819,20306,20105,23981,20915,20098,19290,18854,20457,19652,20099,18974,20430,23804,20906,20549,18949,18902,20708,19095,20809,22662,23603,18676,19197,19183,19102,19123,20636,20756,23970,18677,19915,23983,19172,19388,18531,19413,18916,19145,20793,19421,22820,23922,23919,19073,19057,20300,19715,18899,18988,22854,20875,19014,23386,20502,21872,19901,19188,19099,19140,19033,18903,20825,20854,21213,19615,23182,23575,19293,19086,19066,20511,20621,19001,19185,19371,23792,19101,19079,21740,20840,20555,19165,19465,19092,19519,19713,21563,20890,18616,22823,20390,20723,19964,19022,19107,24015,19369,19193,20256,20935,19338,19241,19113,20948,19233,24018,19050,19324,22908,18928,23336,22880,20722,23796,19010,20379,18746,19105,19442,19068,19456,19009,20249,20951,20433,19731,23745,20108,19090,19487,20095,19262,23046,19016,19179,19535,23825,20369,19506,19423,20565,23926,19390,18979,23740,19258,19332,20092,19003,23918,22909,18939,20500,19002,19054,23826,19049,18033,19289,18598,20888,19168,20654,18980,20588,20942,19632,19162,19315,19467,19501,19447,19063,20132,19080,23023,20949,19734,19059,19035,19027,20329,19045,20428,19046,19029,20111,20410,18997,19891,24027,22677,19136,19167,19026,20517,23969,23867,20944,23053,20107,19677,19192,19407,19690,20020,19229,20103,20903,19186,23980,18684,19671,19061,20889,19546,19119,18879,19064,19211,18112,19370,19412,18907,18250,18927,19561,19507,17702,19434,19700,19636,12964,16531,3533,18719,8339,19570,19766,19908,19697,18682,12704,19330,19767,7780,20233,19884,20201,19426,20243,10064,19698,20294,20260,20325,20350,19600,20335,18432,17485,18873,19209,17300,20401,20205,17516,20399,20481,20499,20413,11568,20493,20427,20443,20500,20546,3646,20492,20505,19651,20506,9974,20545,20539,20541,20573,20411,20534,18552,11520,18744,20336,18512,20604,19338,19675,19596,20676,20648,20614,20719,20689,20754,20812,20923,20916,20558,20937,20939,20806,20524,20965,18053,8935,20931,20860,20751,22605,22361,13589,20940,22452,22463,18071,22728,22747,22760,20777,22819,22799,17563,20688,22751,22451,13098,22800,22917,14686,23024,23137,18911,18860,18722,18611,18323,18714,18683,18649,18686,18676,18693,18692,18608,18674,18673,18684,18398,18633,18472,9565,18575,18625,18590,18374,18595,18576,18494,18533,18579,18546,18510,18547,18518,12502,18471,18526,14661,2776,18516,18501,18467,18243,18492,19127,18878,14560,17114,16907,10338,17200,18065,15279,18217,17815,16315,13278,17624,18226,17684,18224,17839,17697,18007,16439,17218,8036,17582,10668,17698,17699,17819,17686,14359,17461,17888,15801,18117,18197,17726,17947,9601,17692,18255,17884,18309,8839,11760,14515,11677,17428,1374,17957,9241,5529,17944,16717,17896,8032,17500,14975,13352,16819,18304,11184,13896,16530,18285,18142,4128,8106,17705,5138,8089,5666,18091,16510,9151,8481,7479,533,17690,9238,16270,16567,6521,18330,1796,17704,17769,16561,4121,17653,17951,17531,10078,15811,10141,18417,11548,17850,13773,10036,17902,17851,17982,17597,8270,17405,8622,18317,18238,9912,17315,17950,2995,18382,17005,17920,10878,18161,9665,17757,18385,18133,11646,14502,266,16785,10639,168,14390,6621,12545,18174,17776,18145,16540,18338,16355,12846,9950,12854,7199,16853,16741,16286,11137,17167,16687,12924,17349,17981,17645,16878,17707,16375,16581,8756,17768,17644,17137,17441,17522,17693,18080,17926,18123,18210,11279,17483,17748,17501,15519,10097,13805,6427,18294,18394,12889,9961,11449,8553,9691,14005,15688,17172,17152,11445,17012,11237,7647,10671,9570,16650,3826,17337,965,17490,18035,11845,182,9342,18937,19171,19277,18288,19284,17356,17931,18470,20471,18532,19268,19347,17791,18636,17636,18482,19089,20203,20314,20343,20065,20548,20738,22474,18240,18723,18542,18730,19402,18965,20696,18662,18729,18588,18740,18799,19319,19405,17691,19392,18606,15426,18314,19534,2873,19708,2369,19709,19755,19756,19871,9872,16851,20179,20242,456,3559,13930,15907,20513,20652,20653,20725,20789,18706,18586,18569,18262,18455,19404,19470,19486,19504,19505,19521,19527,19542,19525,19580,19599,19609,19613,19664,19678,19682,19683,19699,19701,19765,19742,19722,19753,20016,19906,19907,20063,20064,20168,20178,20188,20202,20237,20236,4086,20697,20737,19627,20748,18327,12902,11974,12334,15670,15912,14715,5649,16898,15729,2752,12375,4599,13706,15395,12461,19654,16293,4024,20353,17929,104,3037,19170,14948,1793,19207,4999,16137,3885,16671,18791,17210,5987,14956,1341,15499,1555,18381,16194,5107,16133,8670,2254,3803,17773,13698,17576,14832,18565,15853,16305,13363,17639,12766,12033,4318,7200,13638,15044,14720,7693,9349,739,20818,4539,9806,11500,6163,12523,13559,13991,1412,14823,15129,1655,17157,17397,1802,18057,1831,18812,18814,1937,7572,9621,17845,20784,18838,19239,18164,18491,18765,18574,20392,20423,20770,22853,22881,22607,18551,23179,22458,17803,18168,18871,19641,20667,22955,9304,23633,17489,23246,23384,23634,23708,22699,17115,18479,23721,23643,23794,23801,23599,23783,23736,23632,23891,23904,23928,23931,15159,23961,19995,18840,23934,23924,23809,20400,24017,24028,17903,23966)
) t1
left join 
(
    select
        shop_id,
        apply_reason,
        source,
        from_unixtime(insert_date,'yyyyMMdd HH:mm:ss') as insert_date
    from
        origin_common.cc_ods_fs_shop_close
    where 
        status = 'close' 
) t2
on t1.shop_id=t2.shop_id
left join 
(
    select
        distinct
        shop_id,
        shop_cname1
    from
        data.cc_dw_fs_products_shops 
) t3
on t1.shop_id=t3.shop_id 
////////////////////////////////////////////////////////////
select
    t2.shop_id,
    sum(t1.item_price) as item_price,
    sum(t1.commission) as commission
from
(
    select
        s1.product_id as product_id, 
        (s1.commission/100) as commission,
        (s1.item_price/100) as item_price
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
    inner join 
        origin_common.cc_ods_dwxk_fs_wk_cck_user s2
    on 
        s1.cck_uid = s2.cck_uid
    where 
        s1.ds>='${begin_date}'
    and
        s1.ds<='${end_date}'
    and
        s2.ds= '${end_date}'
    and
        s2.platform = 14
) t1
inner join 
(
    select
        distinct
        shop_id,
        product_id
    from 
        origin_common.cc_product
    where
        ds =20190114
    and
        shop_id in 

) t2
on t1.product_id=t2.product_id
group by 
    t2.shop_id
////////////////////////////////////////////////////////////
sudo ln -s /Users/gonghuidepro/anaconda3/pkgs/django-2.1.5-py36_0/bin/django-admin /usr/local/bin
su ln -s /Users/gonghuidepro/anaconda3/pkgs/django-2.1.5-py36_0/bin/django-admin /usr/local/bin
////////////////////////////////////////////////////////////
郑普祥需求
select
    n1.shop_id,
    n1.shop_title,
    n1.product_id,
    n1.product_title,
    n1.product_cname3,
    n2.ad_id,--推广id
    n2.ad_name,--推广名称
    n2.ad_price,---券前价
    n2.cck_rate,---楚客佣金率
    n2.cck_price,---楚客佣金额
    n2.discount_price,---券后价
    n2.audit_status,---审核状态（0待审核，1审核通过，2驳回）
    n2.status,---商品状态（0下架，1在架）  
    from_unixtime(n2.start_time, 'yyyyMMdd HH:mm:ss') as start_time,
    from_unixtime(n2.end_time, 'yyyyMMdd HH:mm:ss') as end_time
from 
(
    select
        distinct
        shop_id,
        shop_title,
        product_id,
        product_title,
        product_cname3
    from
        data.cc_dw_fs_products_shops 
    where
        product_cname1 = "食品"
    and
        product_cname2 = "水产肉类/新鲜蔬果/熟食"
) n1
left join
(
    select
        h1.shop_id as shop_id,--店铺id
        h2.app_item_id as product_id,--商品id
        h1.ad_id as ad_id,--推广id
        h1.ad_name as ad_name,--推广名称
        (h1.ad_price/100)as ad_price,---券前价
        (h1.cck_rate/1000) as cck_rate,---楚客佣金率
        (h1.cck_price/100) as cck_price,---楚客佣金额
        ((h1.cck_price/100)/(h1.cck_rate/1000)) as discount_price,---券后价
        h1.audit_status as audit_status,---审核状态（0待审核，1审核通过，2驳回）
        h1.status as status,---商品状态（0下架，1在架）
        h1.start_time,
        h1.end_time  
    from
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
            t1.status as status,---商品状态（0下架，1在架）
            t1.start_time,
            t1.end_time
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
                status,---商品状态（0下架，1在架）
                start_time,
                end_time    
            from 
                cc_ods_fs_dwxk_ad_items_daily
        ) t1
        inner join
        (
            select
                max(id) as id, 
                item_id
            from 
                cc_ods_fs_dwxk_ad_items_daily
            group by item_id
        ) t2
        on t1.id=t2.id
    )h1
    inner join
    (
        select 
            item_id, 
            app_item_id--商品id
        from 
            cc_ods_dwxk_fs_wk_items
    )h2
    on h1.item_id=h2.item_id
) n2
on n1.product_id=n2.product_id

////////////////////////////////////////////////////////////////////////////////////
唐东 供应商子店铺类目信息
select
    t1.shop_id,
    t1.shop_name, 
    t1.category1, 
    t2.name,
    t1.category2,
    t3.name
from
(
    select
        shop_id,
        shop_name, 
        category1, 
        category2
    from 
        origin_common.cc_ods_fs_business_basic
    where
        shop_id in (18760,18880,18905,18950,18995,19005,19010,19100,19110,19145,19195,19220,19275,19310,19340,19355,19455,19485,19500,19615,19650,19655,20465,22840,22845,23805,221,18616,18906,18971,18991,19021,19046,19056,19076,19086,19091,19121,19161,19241,19261,19311,19326,19351,19361,19381,19386,19456,19461,19471,19501,19506,20366,20756,23601,18332,18677,18857,18872,18967,18982,18987,19002,19047,19052,19077,19097,19102,19177,19182,19292,19327,19377,19382,19437,19462,19467,19492,19552,23657,23792,24027,18718,18738,18903,19003,19018,19103,19123,19168,19183,19283,19363,19403,19413,19448,19463,20028,20258,20533,20703,22783,18819,18854,18859,18899,18964,18979,18984,18989,19004,19009,19029,19034,19039,19049,19054,19194,19259,19289,19304,19359,19424,19464,19479,19514,19674,19734,19904,20004,20109,20249,20379,22959,24024)
) t1
left join 
(
    select
        s5.last_cid as last_cid,
        s6.name     as name,
        s5.c1       as c1
    from 
        origin_common.cc_category_cascade s5
    join 
        origin_common.cc_ods_fs_category s6
    on 
        s5.c1 = s6.cid
    where 
        s5.ds='${bizdate}'
) t2
on t1.category1 = t2.last_cid
left join
(
    select
        s3.last_cid as last_cid,
        s4.name     as name,
        s3.c2       as c2
    from 
        origin_common.cc_category_cascade s3
    join 
        origin_common.cc_ods_fs_category s4
    on  
        s3.c2 = s4.cid
    where 
        s3.ds='${bizdate}'
) t3
on t1.category2 = t3.last_cid

/////////////////////////////////////////////////////////
select 
    max(p3.ds) as date,
   --->新增用户数
    count(p1.cct_uid) as newuser,
    count(if(p1.user_type=2,p1.cct_uid,null)) as newuser_c,
    count(if(p1.user_type=1,p1.cct_uid,null)) as newuser_j,
    count(if(p1.user_type=0,p1.cct_uid,null)) as newusaer_v,
    --->登录app的新用户数
    count(p2.cct_uid) as loginapp,
    count(if(p1.user_type=2,p2.cct_uid,null)) as loginapp_c,
    count(if(p1.user_type=1,p2.cct_uid,null)) as loginapp_j,
    count(if(p1.user_type=0,p2.cct_uid,null)) as loginapp_v,

from 
(
    --->t1:新增vip用户-493
    select 
        t1.cck_uid as cck_uid,
        t2.cct_uid as cct_uid,
        0 as user_type
    from
    (
        select 
            date_format(from_unixtime(create_time),'yyyyMMdd') as time,
            cck_uid
        from 
            cc_ods_fs_wk_cct_layer_info
        where 
            platform=14
        and 
            is_del=0
        and 
            date_format(from_unixtime(create_time),'yyyyMMdd')='${bizdate}'
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
    --->新增积分用户
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
            date_format(mtime,'yyyyMMdd')='${bizdate}'
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
            date_format(ctime,'yyyyMMdd')='${bizdate}'
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
left outer join
--->p2:登录app用户数
(
    select
        distinct cct_uid
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
/////////////////////////////////////////////////////////////
普通用户为什么会有cck_uid呢？
select
    t1.user_type,
    count(t1.cct_uid) as aa,
    count(t1.cck_uid) as bb
from
(
    select
        n1.cct_uid,
        n1.cck_uid,
        n1.guider_uid,
        n1.user_type
    from 
    (
        select 
            cct_uid,
            cck_uid,
            guider_uid,
            cck_vip_level,
            cck_vip_status,
            (
            case 
            when cck_vip_status=0 and cck_vip_level=0 then "普通用户"
            when cck_vip_status=0 and cck_vip_level=1 then "积分用户"
            else 'vip用户' end
            ) as user_type
        from 
            cc_ods_fs_tui_relation 
    ) n1
    inner join 
    (
        select 
            cck_uid
        from 
            cc_ods_dwxk_fs_wk_cck_user
        where 
            ds=20190130
        and 
            platform=14
    ) n2
    on n1.guider_uid=n2.cck_uid
) t1
group by 
    t1.user_type

/////////////////////////////////////////////////////////////
select 
    cck_uid
from 
    cc_ods_dwxk_fs_wk_cck_user
where 
    ds='${bizdate}'
and 
    platform=14

/////////////////////////////////////////////////////////////
select
    t1.cct_uid,
    t1.cck_uid,
    t1.cck_vip_level,
    t1.cck_vip_status,
    t1.user_type,
    t2.real_name,
    t2.status,
    t2.pay_status,
    t2.pay_sn,
    t2.pay_time,
    t2.pay_price
from
(
    select 
        cct_uid,
        cck_uid,
        cck_vip_level,
        cck_vip_status,
        (
        case 
        when cck_vip_status=0 and cck_vip_level=0 then "普通用户"
        when cck_vip_status=0 and cck_vip_level=1 then "积分用户"
        else 'vip用户' end
        ) as user_type
    from 
        cc_ods_fs_tui_relation 
    where
        cct_uid in ()
) t1
left join
(
    select
        cck_uid,
        real_name,
        status,
        pay_status,
        pay_sn,
        from_unixtime(pay_time, "yyyyMMdd") as pay_time,
        pay_price
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where
        ds=20190130
) t2
on t1.cck_uid=t2.cck_uid
///////////////////////////
施旭鸿 需求 0108-0110 期间登录的新用户 且在 0108-0130 升级的用户

下面的代码则是 0108-0130 升级的用户且在 0108-0110 期间登录的用户
这个逻辑有点瑕疵，就是跑出来的结果可能包含 0108-0110 的老用户，也在 0108-0130期间升级了
且mtime 是否代表升级时间也是个问题。
select
    n1.mtime,
    n1.cct_uid,
    n1.cck_uid,
    n1.guider_uid,
    n1.user_type
from 
(
    select 
        mtime,
        cct_uid,
        cck_uid,
        guider_uid,
        cck_vip_level,
        cck_vip_status,
        (
        case 
        when cck_vip_status=0 and cck_vip_level=0 then "普通用户"
        when cck_vip_status=0 and cck_vip_level=1 then "积分用户"
        else 'vip用户' end
        ) as user_type
    from 
        cc_ods_fs_tui_relation 
    where
        date_format(mtime,'yyyyMMdd')>='${bizdate1}'
    and
        date_format(mtime,'yyyyMMdd')<='${bizdate30}'
) n1
inner join 
(
    select 
        cck_uid
    from 
        cc_ods_dwxk_fs_wk_cck_user
    where 
        ds=20190130
    and 
        platform=14
) n2
on n1.guider_uid=n2.cck_uid
inner join 
(
    select
        distinct 
        cct_uid
    from 
        origin_common.cc_ods_log_gwapp_pv_hourly
    where 
        ds>='${bizdate1}'
    and
        ds<='${bizdate2}'
    and 
        app_partner_id=14
    and 
        module = 'https://app-h5.daweixinke.com/chuchutui/index.html'
) n3 
on n1.cct_uid=n3.cct_uid 

////////////////////////////////////////////////////////
李光远

select
    s1.cck_uid as cck_uid, 
    s1.product_id,
    s1.third_tradeno,
    s1.sale_num,
    (s1.commission/100) as commission,
    (s1.item_price/100) as item_price
from
    origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
where 
    s1.ds=20190107
and
    s1.third_tradeno in (
        '190107093822Pkd324264','190107100141tuz325745','190107100133Wsf322223','190107101521wDB32254','190107100035X0R322210','1901071002492Ym324358','190107100119hym321187','19010710154908c329262','190107100019jDS327499','190107102854vwP322180','190107102124Z1J322180','190107102100kBu322180','190107100304ALB322180','190107103509Yi8325612','190107100143Ctb325612','1901071030590nV325612','190107102224qmH329752','190107102212qQN320985','190107100156C02320482','190107100114NDo323502'
        )
/////////////////////////////////////////////////////////
张梨娜 服饰达人数据
select
    t1.*
from 
(
    select
        s1.cck_uid,
        s4.real_name,
        s4.phone,
        s3.product_cname1,
        s3.product_cname2,
        count(distinct s1.third_tradeno) as order_count,
        sum(s1.sale_num) as sales_num,
        sum(s1.item_price/100) as pay_fee,
        sum(s1.cck_commission/100) as cck_commission
    from
    (
        select
            product_id,
            third_tradeno,
            sale_num,
            item_price,
            cck_commission,
            cck_uid
        from
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where
            ds >= 20181101
        and
            ds <= 20181231
    )s1
    inner join
    (
        select
            cck_uid
        from
            origin_common.cc_ods_dwxk_fs_wk_cck_user 
        where
            ds = 20181231
        and
            platform = 14
    )s2
    on s1.cck_uid=s2.cck_uid
    inner join 
    (
        select
            distinct
            product_id,
            product_cname1,
            product_cname2
        from 
            data.cc_dw_fs_products_shops
        where 
            product_cname1 in ('箱包','女装','运动户外','男装','配饰','鞋靴','女士内衣/男士内衣/家居服')
        union all
        select
            distinct
            product_id,
            product_cname1,
            product_cname2
        from 
            data.cc_dw_fs_products_shops
        where 
            product_cname1 = '母婴' and product_cname2 in ('童装/亲子装','婴童鞋/亲子鞋')
    )s3
    on s1.product_id=s3.product_id
    left join 
    (
        select
            cck_uid,
            real_name,
            phone
        from 
            origin_common.cc_ods_dwxk_fs_wk_business_info
        where 
            ds=20181231
    )s4
    on s1.cck_uid=s4.cck_uid
    group by 
        s1.cck_uid,s4.real_name,s4.phone,s3.product_cname1,s3.product_cname2
) t1
order by t1.pay_fee desc
limit 100

//////////////////////////////////////////////////////////
郑普祥需求 供应商子店铺19690，虚拟ID为792，商品售后
select
    t1.pm_pid,--自营店铺商品ID
    t1.pm_title,--供应商商品名称
    t1.pm_sid,--自营店铺ID
    t3.shop_id, 
    t3.order_sn, 
    t3.refund_sn, 
    t3.refund_reason, 
    t3.description, 
    t3.status, 
    t3.hope_price, 
    t3.success_price, 
    from_unixtime(t3.create_time,'yyyyMMdd') as ctime  
from 
(
    SELECT
        distinct
        pm_pid,--自营店铺商品ID
        pm_title,--供应商商品名称
        pm_sid--自营店铺ID
    FROM
        cc_ods_op_products_map
    WHERE 
        ds=20190312
    and
        pm_vid=260
)t1
inner join 
(
    select
        product_id,
        order_sn
    from 
        cc_ods_fs_refund_products
)t2
on t1.pm_pid=t2.product_id
inner join
(
    select
        shop_id, 
        order_sn, 
        refund_sn, 
        refund_reason, 
        description, 
        status, 
        hope_price, 
        success_price, 
        is_without_shipping, 
        shipping_sn, 
        shipping_company, 
        create_time 
    from 
        cc_ods_fs_refund_order
    where
        create_time>=1551369600
    and
        status=1
)t3
on t2.order_sn=t3.order_sn

//////////////////////////////////////////////////////

select
    distinct
    cck_uid,
    phone_number
from 
    origin_common.cc_ods_dwxk_fs_wk_cck_user
where 
    ds = 20190220
and 
    platform=14
and 
    phone_number = 15972927464

//////////////////////////////////////////////////////
崔丹 王铁铸 团队信息
20190304 又一团队平移
select
    t1.gm_uid,
    t1.leader_uid,
    t3.real_name as leader_name,
    t3.phone as leader_phone,
    t1.cck_uid,
    t1.type,
    t2.real_name as cck_name,
    t2.phone as cck_phone
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
        gm_uid = 459877
) t1
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
        ds = 20190303
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
        ds = 20190303
)t3
on t1.leader_uid=t3.cck_uid
////////////////////////////////////
从滨团队数据2总监之间从属关系1版写法，
select
    t1.leader_uid,
    t3.real_name as leader_name,
    t1.hatch_uid,
    t4.real_name as hatch_name,
    t1.cck_uid,
    t1.type,
    t2.real_name as cck_name
from 
(
    select
        distinct
        leader_uid,
        hatch_uid,
        cck_uid,
        type
    from
        origin_common.cc_ods_fs_wk_cct_layer_info
    where 
        leader_uid in (463618,477064,488286,537334,692014,724196,781880,808174,952584,971706,984142,1016208,1074528,1268028,529895,639705,820427,845745,962885,964427,1048485,1057691,1140953,1170287)
) t1
left join 
(
    select
        distinct
        cck_uid,
        real_name
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = 20190306
) t2
on t1.cck_uid=t2.cck_uid
left join 
(
    select
        distinct
        cck_uid,
        real_name
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = 20190306
)t3
on t1.leader_uid=t3.cck_uid
left join 
(
    select
        distinct
        cck_uid,
        real_name
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = 20190306
)t4
on t1.hatch_uid=t4.cck_uid

////////////////////////////////////////////////////
从滨团队数据2总监之间从属关系2版写法 hive表只有最新的整表记录，3.6号平移后总监的孵化人ID变为0了，
select
    t1.cck_uid,
    t1.type,
    t2.real_name as cck_name,
    t1.hatch_uid,
    t4.real_name as hatch_name,
    t1.leader_uid,
    t3.real_name as leader_name
from 
(
    select
        distinct
        leader_uid,
        hatch_uid,
        cck_uid,
        type
    from
        origin_common.cc_ods_fs_wk_cct_layer_info
    where 
        cck_uid in (463618,477064,488286,537334,692014,724196,781880,808174,952584,971706,984142,1016208,1074528,1268028,529895,639705,820427,845745,962885,964427,1048485,1057691,1140953,1170287)
) t1
left join 
(
    select
        distinct
        cck_uid,
        real_name
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = 20190306
) t2
on t1.cck_uid=t2.cck_uid
left join 
(
    select
        distinct
        cck_uid,
        real_name
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = 20190306
)t3
on t1.leader_uid=t3.cck_uid
left join 
(
    select
        distinct
        cck_uid,
        real_name
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = 20190306
)t4
on t1.hatch_uid=t4.cck_uid
////////////////////////////////////////////////////////
从滨团队数据2总监之间从属关系3版写法，线上表有过去某天的整表记录，能拿到总监的孵化人ID，

SELECT 
    cck_uid,
    hatch_uid,
FROM 
    wk_cct_layer_info_20190304
WHERE 
    cck_uid IN (463618,477064,488286,537334,692014,724196,781880,808174,952584,971706,984142,1016208,1074528,1268028,529895,639705,820427,845745,962885,964427,1048485,1057691,1140953,1170287)
//////////////////////////////////////////////////////////////////
上官形形 
select
    t07.date        as date,
    t07.order_sn    as order_sn,
    t07.template_id as template_id,
    t07.coupon_sn   as coupon_sn,
    t07.used_money  as used_money,
    t08.total_fee   as total_fee,
    t08.pay_fee     as pay_fee
from
(
    select
        ds as date,
        order_sn,
        template_id,
        coupon_sn,
        used_money
    from 
        origin_common.cc_order_coupon_paytime
    where 
        ds>=20190218 
    and 
        ds<=20190219 
    and 
        template_id =17436455
) as t07
left join
(
    select
        ds as date,
        order_sn,
        total_fee,
        pay_fee
    from 
        origin_common.cc_order_user_pay_time
    where 
        ds>=20190218 
    and 
        ds<=20190219 
    and 
        source_channel = 2
) as t08
on t07.date=t08.date and t07.order_sn=t08.order_sn
/////////////////////////////////////////////////////////////////////////
-- 唐东
-- 1100194051142  这个商品是昨天晚上的爆款，芒果，想看一个数据
-- 昨天购买的所有用户中，有多少是活跃用户（近一个月内有登录，成交，纷享或者其他相关数据），
-- 多少是新用户（第一次登录app或者小程序），
-- 多少是唤醒的老用户（近30天内，没有任何数据的用户）
select
    count(t1.uid) as buyer_num,
    count(t2.user_id) as active_num,
    sum(if(t3.user_id is null,1,0)) as new_user_num,
    count(t4.user_id) as wake_num
from
(
    select
        distinct
        uid
    from
        origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where
        ds = '${end_date1}'
    and
        product_id = 1100194051142
)t1
left join 
(
    select 
        distinct
        user_id
    from 
        origin_common.cc_ods_log_cctapp_click_hourly  
    where 
        ds >= '${begin_date1}' 
    and 
        ds <= '${end_date1}'
    and 
        source in ('cct','cctui')
)t2
on t1.uid=t2.user_id
left join 
(
    select 
        distinct
        user_id
    from 
        origin_common.cc_ods_log_cctapp_click_hourly  
    where 
        ds >= '${begin_date2}' 
    and 
        source in ('cct','cctui')
)t3
on t1.uid=t3.user_id
left join 
(
    select
        distinct
        n1.user_id
    from
    (
        select 
            distinct
            user_id
        from 
            origin_common.cc_ods_log_cctapp_click_hourly  
        where 
            ds >= '${begin_date2}' 
        and
            source in ('cct','cctui')
    )n1
    left join 
    (
        select 
            distinct
            user_id
        from 
            origin_common.cc_ods_log_cctapp_click_hourly  
        where 
            ds >= '${begin_date1}'
        and
            ds <= '${end_date1}'
        and
            source in ('cct','cctui') 
    )n2
    on n1.user_id=n2.user_id
    where n2.user_id is null
)t4
on t1.uid=t4.user_id
































