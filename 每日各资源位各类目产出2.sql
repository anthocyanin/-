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
        when m1.product_cname1 = '母婴' and m1.product_cname2 in ('童装/亲子装','婴童鞋/亲子鞋') then '服饰'
        when m1.product_cname1 = '母婴' and m1.product_cname2 in ('玩具/模型/动漫/早教/益智','奶粉/辅食/营养品/零食','孕妇装/孕产妇用品/营养','尿片/洗护/喂哺/推车床') then '母婴'
        when m1.product_cname1 in ('家用电器','手机数码') then '家电数码'
        when m1.product_cname1 = '家居百货' then '家居百货'
        when m1.product_cname1 = '美妆个护' then '美妆个护'
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
                s1.ds = '${begin_date}'
            and 
                s2.ds  = '${begin_date}'
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
                ds = '${begin_date}'
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
