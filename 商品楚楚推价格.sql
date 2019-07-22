select
	t1.app_item_id,
	t4.product_title,
    t4.product_cname1, 
    t4.product_cname2,
    t4.product_cname3,
    t4.shop_id, 
    t4.shop_title,
    t2.sales_price,
    ((t1.ad_price-t3.money)/100) as cctui_price,
    t5.fee,
    t6.pv
from
(
	select
		s2.app_item_id,
		s1.ad_id,
		s1.ad_name,
		s1.ad_price
	from
		cc_ods_dwxk_fs_wk_ad_items s1
	join
		cc_ods_dwxk_fs_wk_items s2
	on s1.item_id =s2.item_id
	where
		s2.shop_id not in(18164,18335,17801,18628,18635,17791,17891,18253,18455,18470)
		and
		s1.audit_status=1
		and
		s1.status=1

)t1
left join
(
	select
		product_id,
		sales_price
	from
		cc_ods_fs_product
	where
		status=1
)t2
on t1.app_item_id=t2.product_id
left join
(
	select
		ad_id,
		money
	from
		cc_ods_dwxk_fs_wk_ad_coupon
	where
		status =1
)t3
on t1.ad_id=t3.ad_id
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
)t4
on t1.app_item_id=t4.product_id 
left join
(
	select
		product_id,
		sum(item_price/100) as fee
	from
		cc_ods_dwxk_wk_sales_deal_ctime
	where
		ds>=20180514
	group by
		product_id
)t5
on t1.app_item_id=t5.product_id
left join
(
	select
		product_id,
		count(1) as pv
	from
		origin_common.cc_ods_log_cctui_product_coupon_detail_hourly
	where
		ds>= 20180514
		and
		ds<= 20180520
		and
		detail_type='item'
	group by
		product_id
)t6
on t1.app_item_id=t6.product_id
where t5.fee is not null and t4.product_title is not null