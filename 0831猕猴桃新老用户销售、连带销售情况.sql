###猕猴桃总支付金额、订单数、销量、购买人数
select
    count(t1.uid),
    sum(t1.item_price/100),
    count(distinct t1.third_tradeno),
    sum(t1.sale_num)
from
(
    select
        cck_uid,
        uid,
        sale_num,
        third_tradeno,
        item_price,
        create_time
    from 
    	origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where 
    	ds >='${start_date}' and ds<='${end_date}'
    	and
    	product_id in(110019405576,110019405615)
)t1
join
(
    select
        cck_uid
    from 
    	origin_common.cc_ods_dwxk_fs_wk_cck_user
    where 
    	ds='${end_date}'
    	and 
    	platform=14
)t2
on t1.cck_uid=t2.cck_uid



###猕猴桃新用户支付金额、订单数、销量、新用户人数
select
    count(t1.uid),
    sum(t1.item_price/100),
    count(distinct t1.third_tradeno),
    sum(t1.sale_num)
from
(
    select
        cck_uid,
        uid,
        sale_num,
        third_tradeno,
        item_price,
        create_time
    from 
    	origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where 
    	ds>='${start_date}' and ds<='${end_date}'
    	and
    	product_id in(110019405576,110019405615)
)t1
join
(
    select
        cck_uid
    from 
    	origin_common.cc_ods_dwxk_fs_wk_cck_user
    where 
    	ds='${end_date}'
    	and 
    	platform=14
)t2
on t1.cck_uid=t2.cck_uid
left join
(
    select
        uid,
        min(create_time) as first_time
    from 
    	origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    group by 
    	uid
) t3
on t1.uid = t3.uid
where t3.first_time = t1.create_time



###猕猴桃连带销售
select
    t2.uid,
    t2.product_id,
    t3.product_title,
    sum(t2.item_price/100),
    count(t2.third_tradeno),
    count(t2.sale_num)
from
(
	select
    	uid
	from 
		origin_common.cc_ods_dwxk_wk_sales_deal_ctime
	where 
		product_id in(110019405576,110019405615)
		and 
		ds>='${start_date}'
		and
		ds<='${end_date}'
)t1
join
(
	select
		uid,
    	product_id,
    	item_price,
    	third_tradeno,
    	sale_num
	from 
		origin_common.cc_ods_dwxk_wk_sales_deal_ctime
	where 
		product_id not in(110019405576,110019405615) 
		and 
		ds>='${start_date}'
		and
		ds<='${end_date}'
)t2
on t1.uid=t2.uid
left join
(
	select
    	distinct 
    	product_id,
    	product_title
	from 
		data.cc_dw_fs_products_shops
)t3
on t2.product_id=t3.product_id
group by 
	t2.uid,t2.product_id,t3.product_title