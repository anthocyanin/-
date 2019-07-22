select 
	n1.add_time,
	n1.user_id,
	n1.order_sn,
	n1.pay_time,
	n1.order_type,
	n1.delivery_address,
    n1.delivery_name,
	n1.area_id,
	n2.sale_num,
	n2.product_id,
	n3.product_title,
	n3.shop_id,
	n3.shop_title,
	n4.province_name,
	n4.city_name,
	n5.create_time
from
(
	select 
	    distinct   
		add_time,
		user_id,
		order_sn,
		order_type,
		delivery_time,
		area_id,
		pay_time,
		delivery_name,
		delivery_address
	from 
		origin_common.cc_order_user_delivery_time
	where 
		delivery_address like '%工商局%'
	or 
		delivery_address like '%工商行政管理局%'
	or 
		delivery_address like '%公安%'
	or 
		delivery_address like '%法院%'
	or 
		delivery_address like '%消协%'
	or 
		delivery_address like '%消保%'
	or 
		delivery_address like '%传媒%'
	or 
		delivery_address like '%媒体%'
	or 
		delivery_address like '%电视台%'
	or 
		delivery_address like '%电台%'
	or 
		delivery_address like '%报社%'
	or 
		delivery_address like '%食品%'
	or 
		delivery_address like '%药品监督局%'
	or
		delivery_address like '%卫视%'
	or 
		delivery_name  like '%警官%'
	or 
		delivery_name like '%记者%'
	or 
		delivery_name  like '%赵正启%'
	or 
	   delivery_name  like '%吕晋杰%'
) as n1
inner  join
(
	select
		distinct
		s1.product_id,
		s1.third_tradeno,
		s1.sale_num 
	from
		origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
	join
		origin_common.cc_ods_dwxk_fs_wk_cck_user s2
	on s1.cck_uid=s2.cck_uid
	where 
		s2.platform =14
	and 
		s2.ds = 20180911
) as n2
on 
	n1.order_sn = n2.third_tradeno
left join
(
	select 
	    distinct
		product_id,
		product_title,
		shop_id,
		shop_title
	from 
		data.cc_dw_fs_products_shops
) as n3
on 
	n2.product_id = n3.product_id
left join
(
	select 
		area_id, 
		province_name,
		city_name
	from 
		cc_area_city_province
) as n4
on 
	n1.area_id = n4.area_id
left join
(
	select 
		order_sn,
		min(create_time) as create_time
	from 
		origin_common.cc_ods_fs_refund_order
	group by 
		order_sn
) n5
on 
	n1.order_sn = n5.order_sn;











