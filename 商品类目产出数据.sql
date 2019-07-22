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
                shop_id in ('自营的店铺id')
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
    sum(t7.pay_fee) as pay_fee
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