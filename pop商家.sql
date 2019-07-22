select
	t1.shop_id,
	t2.shop_name,
	t2.c1_name,
	(case
	when t2.shop_id in (18164,18335,17801,18628,18635,19141,19268) then '自营'
	when t2.shop_id = 17791 then '京东'
	when t2.shop_id in(17891,18253) then '天猫'
	when t2.shop_id = 18455 then '严选'
	when t2.shop_id = 18470 then '冰冰购'
	when t2.shop_id in (18636,18704) then '每日优鲜'
	when t2.shop_id in (18641,18642,18643,18644) then '拼多多'
	when t2.shop_id in (17927,18253,17891) then '村淘'
	when t2.shop_id in (18532,19347,19405) then '代发'
	else '站内' end) as tab,
	if(t3.shop_id is not null,1,0) as tab2,
	t1.count_pd,
	t6.pv_7d,
	t4.count_order_pd_7d,
	t4.fee_7d,
	t4.pay_count_7d,
	t4.cck_commission_7d,
	t5.count_updatepd_7d,
	t7.pv_30d,
	t8.fee_30d,
	t8.pay_count_30d,
	(t8.fee_30d / t8.pay_count_30d) as price_30d,
	(t8.pay_count_30d / t7.pv_30d) as order_rate_30d,
	t9.count_vl_30d,
	t9.count_pl_30d,
	(t9.count_pl_30d / t9.count_vl_30d) as rate_pl_30d,
	t9.refund_count_30d,
	(t9.refund_count_30d / t8.pay_count_30d) as rate_refund_30d,
	t9.order_delivery_30d,
	t9.delivery_time_30d,
	t9.order_ship_30d,
	t9.time_30d,
	t10.count_huihua,
	(t10.count_jiedai/t10.count_huihua) as rate_jiedai,
	(t10.totalwaitetime/t10.count_jiedai/60) as time_huifu
from
(
	select
		app_shop_id as shop_id,
		count(distinct item_id) as count_pd
	from
		origin_common.cc_ods_dwxk_fs_wk_ad_items
	where
		audit_status =1
		and
		status =1
		and
		app_id =2
	group by
		app_shop_id
)t1
left join
(
	select
		s1.shop_id,
		s1.shop_name,
		s2.name as c1_name
	from
		origin_common.cc_ods_fs_business_basic  s1
	join
		origin_common.cc_ods_fs_category s2
	on s1.category1 = s2.cid
)t2
on t1.shop_id=t2.shop_id
left join
(
	select
		shop_id
	from
		tmp.temp_wuwei_cct_shop
)t3
on t1.shop_id=t3.shop_id
left join
(
	select
		s2.shop_id,
		count(distinct s1.product_id) as count_order_pd_7d,
		sum(s1.item_price/100) as fee_7d,
		count(s1.third_tradeno) as pay_count_7d,
		sum(s1.cck_commission/100) as cck_commission_7d
	from
		origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
	left join
		cc_ods_dwxk_fs_wk_items s2
	on s1.product_id = s2.app_item_id
	where
		s1.ds>= '${begin_date_7d}'
		and
		s1.ds <='${end_date_7d}'
		and
		s1.app_id=2
	group by
		s2.shop_id
)t4
on t1.shop_id=t4.shop_id
left join
(
	select
		app_shop_id as shop_id,
		count(distinct item_id) as count_updatepd_7d
	from
		origin_common.cc_ods_dwxk_fs_wk_ad_items
	where
		audit_status =1
		and
		app_id =2
		and
		create_time >= unix_timestamp('${begin_date_7d}','yyyyMMdd')
		and
		create_time <= unix_timestamp('${end_date_7d}','yyyyMMdd')
	group by
		app_shop_id
)t5
on t1.shop_id=t5.shop_id
left join
(
	select
		a2.shop_id,
		sum(a1.pv) as pv_7d
 	from
	(
		select
			product_id,
			count(1) as pv
		from
			origin_common.cc_ods_log_cctui_product_coupon_detail_hourly
		where
			ds>= '${begin_date_7d}'
			and
			ds<= '${end_date_7d}'
			and
			detail_type='item'
		group by
			product_id
		union all
		select
			product_id,
			count(1) as pv
		from
			origin_common.cc_ods_log_gwapp_product_detail_hourly
		where
			ds>= '${begin_date_7d}'
			and
			ds<= '${end_date_7d}'
		group by
			product_id
	)a1
	left join
	(
		select
			app_item_id as product_id,
			shop_id
		from
			origin_common.cc_ods_dwxk_fs_wk_items
	)a2
	on a1.product_id=a2.product_id
	group by
		a2.shop_id
)t6
on t1.shop_id=t6.shop_id
left join
(
	select
		a2.shop_id,
		sum(a1.pv) as pv_30d
 	from
	(
		select
			product_id,
			count(1) as pv
		from
			origin_common.cc_ods_log_cctui_product_coupon_detail_hourly
		where
			ds>= '${begin_date_30d}'
			and
			ds<= '${end_date_30d}'
			and
			detail_type='item'
		group by
			product_id
		union all
		select
			product_id,
			count(1) as pv
		from
			origin_common.cc_ods_log_gwapp_product_detail_hourly
		where
			ds>= '${begin_date_30d}'
			and
			ds<= '${end_date_30d}'
		group by
			product_id
	)a1
	left join
	(
		select
			app_item_id as product_id,
			shop_id
		from
			origin_common.cc_ods_dwxk_fs_wk_items
	)a2
	on a1.product_id=a2.product_id
	group by
		a2.shop_id
)t7
on t1.shop_id=t7.shop_id
left join
(
	select
		s2.shop_id,
		sum(s1.item_price/100) as fee_30d,
		count(s1.third_tradeno) as pay_count_30d
	from
		origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
	left join
		cc_ods_dwxk_fs_wk_items s2
	on s1.product_id = s2.app_item_id
	where
		s1.ds>= '${begin_date_30d}'
		and
		s1.ds <='${end_date_30d}'
		and
		s1.app_id=2
	group by
		s2.shop_id
)t8
on t1.shop_id=t8.shop_id
left join
(
	select
		a4.shop_id,
		count(distinct a5.rate_id) as count_vl_30d,
		count(distinct a6.rate_id) as count_pl_30d,
		count(a7.order_sn) as refund_count_30d,
		count(distinct a8.order_sn) as order_delivery_30d,
		avg((a8.delivery_time - a4.create_time)/3600) as delivery_time_30d,
		count(a9.order_sn) as order_ship_30d,
		avg(a9.time/3600) as time_30d
	from
	(
		select
			distinct
			s2.shop_id,
			s1.third_tradeno as order_sn,
			s1.create_time
		from
			origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
		left join
			cc_ods_dwxk_fs_wk_items s2
		on s1.product_id = s2.app_item_id
		where
			s1.ds>= '${begin_date_30d}'
			and
			s1.ds <='${end_date_30d}'
			and
			s1.app_id=2
	)a4
	left join
	(
		select
			distinct
			order_sn,
			rate_id
		from
			cc_rate_star
		where
			rate_id !=0
			and
			ds>= '${begin_date_30d}'
			and
			ds<= '${end_date_30d}'
	)a5
	on a4.order_sn=a5.order_sn
	left join
	(
		select
			distinct
			order_sn,
			rate_id
		from
			cc_rate_star
		where
			rate_id !=0
			and
			ds>= '${begin_date_30d}'
			and
			ds<= '${end_date_30d}'
			and
			star_num =1
	)a6
	on a4.order_sn=a6.order_sn
	left join
	(
		select
			distinct
			order_sn
		from
			cc_ods_fs_refund_order
		where
			stop_time >= unix_timestamp('${begin_date_30d}','yyyyMMdd') 
			and
			stop_time <= unix_timestamp('${end_date_30d}','yyyyMMdd') 
			and
			status =1
			and
			is_without_shipping =0

	)a7
	on a4.order_sn=a7.order_sn
	left join
	(
		select
			order_sn,
			delivery_time
		from
			cc_order_user_delivery_time
		where
			ds>= '${begin_date_30d}'
			and
			ds<= '${end_date_30d}'
	)a8
	on a4.order_sn=a8.order_sn
    left join
	(
		select
			order_sn,
			(update_time - create_time) as time
		from
			data.cc_cct_product_ship_info
		where
			ship_state =3
	)a9
	on a4.order_sn = a9.order_sn
	group by
		a4.shop_id
)t9
on t1.shop_id=t9.shop_id
left join
(
	select
		shop_id,
		sum(mantakesessioncount) as count_huihua,
		sum(waiterreplaysessioncount) as count_jiedai,
		sum(totalwaitetime) as totalwaitetime
	from
		report.cc_rpt_cctui_im_shop_stat
	where
		ds>= '${begin_date_30d}'
		and
		ds<= '${end_date_30d}'
	group by
		shop_id
)t10
on t1.shop_id=t10.shop_id