######大微信客小二后台商品最新推广信息
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
    h1.status as status---商品状态（0下架，1在架）  
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
        from cc_ods_fs_dwxk_ad_items_daily
        where app_shop_id=19405
    ) t1
    inner join
    (
        select
            max(id) as id, 
            item_id
        from cc_ods_fs_dwxk_ad_items_daily
        where app_shop_id=19405
        group by item_id
    ) t2
    on t1.id=t2.id
)h1
inner join
(
    select 
        item_id, 
        app_item_id--商品id
    from cc_ods_dwxk_fs_wk_items
    where shop_id=19405
)h2
on h1.item_id=h2.item_id


