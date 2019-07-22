select
	p1.shop_id,
	p2.shop_name,
	p3.cname1,
	p4.cname2,
	(case
	when p1.shop_id in(19903,20305,20322) then '百诺优品'
	when p1.shop_id in(18470) then '冰冰购'
	when p1.shop_id in(18532,19141,19268,19347,20471) then '代发'
	when p1.shop_id in(18635,18240) then '极便利'
	when p1.shop_id in(17791,18731) then '京东'
	when p1.shop_id in(18704,18636) then '每日优鲜'
	when p1.shop_id in(18730,18723,18542,17636,18482,19089,19667,20203,20314,20343,20065,20548,18871) then '其他'
	when p1.shop_id in(18898,18735,18848,18662,18558,18896,18775,18891,18729,18733,18543,18815,18245,17772,18588,18740,18849,18799,18893,19543,18581,18660,18666,19572,19254,19319,18535,18408,18582,18488,18732,19405,19441,18303,18400,19257,19085,17303,18655,18671,19402,19530,19869,18883,20216,18965) then '生鲜'
	when p1.shop_id in(18706,18586,18569,18262,19392,18606,15426,18314,19534,2873,19708,2369,9872,19871,19756,19755,19709,16851,20179,17691,20242,456,3559,13930,15907,20513,20652,20653) then '小团子'
	when p1.shop_id in(18455) then '严选'
	when p1.shop_id in(18838,19239,19505,19504,19486,19470,19404,19527,19521,19525,19542,19613,19609,19599,19580,19664,19701,19699,19683,19682,19678,19765,19742,19722,19753,20016,19906,19907,20063,20064,20178,20168,20236,20237,20202,20188) then '一亩田'
	when p1.shop_id in(18335,18164,17801) then '自营'
	when p1.shop_id in(19310,18928,19324,18746,19361,19340,19339,19432,19468,19298,19444,19410,19421,19506,18508,19476,19519,19552,18531,19545,19546,18491,19611,18436,19435,18500,19665,19749,19764,19891,18765,19870,19894,20109,20142,20273,20249,20292,19905,20332,18574,20334,20392,20423,20422,20444,20543,20549,20600,20588,20621,20620,20636,20655,20638,20654) then '总监店铺'
	else 'POP' end) as tab,
	p1.count_pd,
	p1.pay_count,
	p1.fee,
	p1.ipv_uv,
	p1.fx_cnt
from
(
	select
		t3.shop_id,
		count(t1.product_id) as count_pd,
		sum(t1.pay_count) as pay_count,
		sum(t1.fee) as fee,
		sum(t2.ipv_uv) as ipv_uv,
		sum(t4.fx_cnt) as fx_cnt,
		sum(t5.pay_count_30d) as pay_count_30d,
		sum(t5.refund_count_30d) as refund_count_30d,
		sum(t6.eva_cnt) as eva_cnt,
		sum(t6.bad_eva_cnt) as bad_eva_cnt,
		sum(t7.count_delivery) as count_delivery,
		sum(t7.count_delivery_overtime) as count_delivery_overtime
	from
	(
		select
			s1.product_id,
			count(distinct s1.third_tradeno) as pay_count,
			sum(s1.item_price/100) as fee
		from
			origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
		join
			origin_common.cc_ods_dwxk_fs_wk_cck_user s2
		on s1.cck_uid=s2.cck_uid
		where
			s1.ds>= '${begin_date}'
			and
			s1.ds<= '${end_date}'
			and
			s2.platform =14
			and
			s2.ds = '${end_date}'
		group by
			s1.product_id
	)t1
	left join
	(
		select
			a1.product_id,
			sum(a1.ipv_uv) as ipv_uv
		from
		(
			select
				ds,
				product_id,
				count(distinct user_id) as ipv_uv
			from
				origin_common.cc_ods_log_cctui_product_coupon_detail_hourly
			where
				ds>= '${begin_date}'
				and
				ds<= '${end_date}'
				and
				detail_type='item'
			group by
				ds,
				product_id
		)a1
		group by
			a1.product_id
	)t2
	on t1.product_id=t2.product_id
	left join
	(
		select
			distinct
			app_item_id as product_id,
			shop_id
		from
			origin_common.cc_ods_dwxk_fs_wk_items 
	)t3
	on t1.product_id=t3.product_id
	left join
	(
		select
	    m3.product_id,
	    count(m1.user_id) as fx_cnt
	from
	    (select
	      ad_material_id as ad_id,
	      user_id
	    from 
	      origin_common.cc_ods_log_cctapp_click_hourly
	    where 
	        ds>= '${begin_date}'
			and
			ds<= '${end_date}' and ad_type in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
	    union all
	    select
	      ad_id,
	      user_id
	    from 
	      origin_common.cc_ods_log_cctapp_click_hourly
	    where 
	        ds>= '${begin_date}'
			and
			ds<= '${end_date}'and ad_type not in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
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
	            ds>= '${begin_date}'
				and
				ds<= '${end_date}' and module='vip' and ad_type in ('single_product','9_cell') and zone in ('material_group-share','material_moments-share')
	        ) s1
	    inner join
	        (select
	            distinct ad_material_id as ad_material_id,
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
	    (select
	        ad_id,
	        item_id
	    from 
	        origin_common.cc_ods_fs_dwxk_ad_items_daily
	    ) m2
	    on 
	        m1.ad_id = m2.ad_id
	    inner join
	    (select
	        item_id,
	        app_item_id as product_id
	    from 
	        origin_common.cc_ods_dwxk_fs_wk_items
	    ) m3
	    on 
	        m3.item_id = m2.item_id
	group by
	    m3.product_id
	)t4
	on t1.product_id=t4.product_id
	left join
	(
		select
			a1.product_id,
			count(a1.third_tradeno) as pay_count_30d,
			count(a2.order_sn) as refund_count_30d
		from
		(
			select
				distinct
				s1.product_id,
				s1.third_tradeno
			from
				origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
			join
				origin_common.cc_ods_dwxk_fs_wk_cck_user s2
			on s1.cck_uid=s2.cck_uid
			where
				s1.ds>= '${begin_date_30d}'
				and
				s1.ds<= '${end_date}'
				and
				s2.platform =14
				and
				s2.ds = '${end_date}'
		)a1
		left join
		(
			select distinct
			  	t1.order_sn
			from
			  	origin_common.cc_ods_fs_refund_order t1
			inner join
			  	origin_common.cc_order_user_delivery_time t2
			on
			  	t1.order_sn = t2.order_sn
			where
			  	t2.ds >= '${begin_date_30d}'
		)a2
		on a1.third_tradeno=a2.order_sn
		group by
			a1.product_id
	)t5
	on t1.product_id = t5.product_id
	left join
	(
		select
			a1.product_id,
			count(a2.rate_id) as eva_cnt,
			sum(if(a2.star_num=1,1,0)) as bad_eva_cnt
		from
		(
			select
				distinct
				s1.product_id,
				s1.third_tradeno
			from
				origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
			join
				origin_common.cc_ods_dwxk_fs_wk_cck_user s2
			on s1.cck_uid=s2.cck_uid
			where
				s1.ds>= '${begin_date_30d}'
				and
				s1.ds<= '${end_date}'
				and
				s2.platform =14
				and
				s2.ds = '${end_date}'
		)a1
		left join
		(
			select 
				distinct
			    order_sn,
			    rate_id,
			    star_num
			from
			    origin_common.cc_rate_star
	  		where
	    		rate_id != 0
	   			and
			  	ds >= '${begin_date_30d}'
		)a2
		on a1.third_tradeno=a2.order_sn
		group by
			a1.product_id
	)t6
	on t1.product_id = t6.product_id
	left join
	(
		select
			a1.product_id,
			count(a2.order_sn) as count_delivery,
			sum(if(a2.delivery_time - a1.create_time > 86400,1,0)) as count_delivery_overtime
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
			on s1.cck_uid=s2.cck_uid
			where
				s1.ds>= '${begin_date_30d}'
				and
				s1.ds<= '${end_date}'
				and
				s2.platform =14
				and
				s2.ds = '${end_date}'
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
		)a2
		on a1.third_tradeno=a2.order_sn
		group by
			a1.product_id
	)t7
	on t1.product_id=t7.product_id
	group by
		t3.shop_id
)p1
left join
(
	select
		shop_id,
		shop_name,
		category1,
		category2
	from
		cc_ods_fs_business_basic
)p2
on p1.shop_id=p2.shop_id
left join
(
	select
		cid,
		name as cname1
	from
		cc_ods_fs_category
)p3
on p2.category1=p3.cid
left join
(
	select
		cid,
		name as cname2
	from
		cc_ods_fs_category
)p4
on p2.category2=p4.cid