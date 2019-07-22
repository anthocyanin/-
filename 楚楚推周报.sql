####大盘 ipv_uv

select
	avg(t2.ipv_uv) as ipv_uv,--平均每天被浏览的商品的浏览总人数，如果count_pd/ipv_uv,则代表平均每个商品的浏览人数。
	avg(t2.count_pd) as count_pd--平均每天被浏览的商品个数
from
(
	select
		t1.ds,--某日
		count(t1.product_id) as count_pd,--被浏览的商品总个数
		sum(t1.ipv_uv) as ipv_uv--被浏览的商品的浏览总人数
	from
	(
		select
			ds,--某日
			product_id,--某商品
			count(distinct user_id) as ipv_uv--浏览人数
		from
			origin_common.cc_ods_log_cctui_product_coupon_detail_hourly
		where
			ds>= '${begin_date}'
		and
			ds<= '${end_date}'
		and
			detail_type ='item' 
		group by
			ds,
			product_id
	)t1
	group by
		t1.ds
)t2

### 商品数据
select
	t1.product_id,--商品ID
	t9.cn_title,--标题
	t8.pdname1,--商品一级类目
	t8.pdname2,--商品二级类目
	t3.shop_id,--店铺id
	t3.shop_name,--店铺名称
	(case	
	when t3.shop_id in (19903,20305,20322) then '百诺优品'
	when t3.shop_id = 18470 then '冰冰购'
	when t3.shop_id in (18532,19141,19268,19347,20471) then '代发'
	when t3.shop_id in (18635,18240) then '极便利'
	when t3.shop_id in (17791,18731) then '京东'
	when t3.shop_id in (18704,18636) then '每日优鲜'
	when t3.shop_id in (18730,18723,18542,17636,18482,19089,19667,20203,20314,20343,20065,20548,20738,19517) then '其他'
	when t3.shop_id in (18662,18729,18588,18740,18799,19319,19405,19402,20216,18965,20696) then '生鲜'
	when t3.shop_id in (18706,18586,18569,18262,19392,18606,15426,18314,19534,2873,19708,2369,9872,19871,19756,19755,19709,16851,20179,17691,20242,456,3559,13930,15907,20513,20652,20653,20725,20789) then '小团子'
	when t3.shop_id = 18455 then '严选'
	when t3.shop_id in (18838,19239,19505,19504,19486,19470,19404,19527,19521,19525,19542,19613,19609,19599,19580,19664,19701,19699,19683,19682,19678,19765,19742,19722,19753,20016,19906,19907,20063,20064,20178,20168,20236,20237,20202,20188,4086,20697,20737,19627,20748,18327,12902,11974,12334,15670,15912,14715,5649,16898,15729,2752,12375,4599,13706,15395,12461,19654,16293,4024,20353,17929,104,3037,19170,14948,1793,19207,4999,16137,3885,16671,18791,17210,5987,14956,1341,15499,1555,18381,16194,5107,16133,8670,2254,18253,3803,17773,13698,17576,14832,18565,739,9349,7693,14720,15044,13638,7200,4318,12033,12766,17639,13363,16305,15853,6163,11500,9806,4539,20818,17845,12523,13559,13991,1412,14823,15129,1655,17157,17397,1802,18057,1831,18812,18814,1937,7572,9621,20784) then '一亩田'
	when t3.shop_id in (18335,18164,17801) then '自营'
	when t3.shop_id in (19339,19468,19298,18491,19611,19435,18765,19870,20142,20332,18574,20392,20423,20543,20600,20770) then '总监店铺'
	else 'pop' end) as tab,--店铺类型
	t1.pay_count,--订单数
	t1.fee,--支付金额
	t2.ipv_uv,--7日ipv_uv
	t4.fx_cnt,--推广次数
	t5.pay_count_30d,--30日订单数
	t5.refund_count_30d,--30日发货后退款数
	(t5.refund_count_30d/t5.pay_count_30d) as rate_refund,--发货退款率
	t6.eva_cnt,--有效评价数
	t6.bad_eva_cnt,--差评数
	(t6.bad_eva_cnt/t6.eva_cnt) as rate_bad,--差评率
	t7.count_delivery,--发货数
	t7.count_delivery_overtime,--超时发货数
	(t7.count_delivery_overtime/t7.count_delivery) as rate_delivery_overtime--超时发货率
from
(
	select
		s1.product_id,
		count(s1.third_tradeno) as pay_count,--7日订单数
		sum(s1.item_price/100) as fee--7日支付金额
	from
		origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
	join
		origin_common.cc_ods_dwxk_fs_wk_cck_user s2
	on 
		s1.cck_uid=s2.cck_uid
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
		sum(a1.ipv_uv) as ipv_uv--7日ipv_uv
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
			detail_type = 'item'
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
		s1.app_item_id as product_id,
		s1.shop_id,--店铺id
		s2.shop_name,--店铺名称
		s1.category_id,
		s2.category1
	from
		origin_common.cc_ods_dwxk_fs_wk_items s1
	left join
		origin_common.cc_ods_fs_business_basic s2
	on 
		s1.shop_id=s2.shop_id
)t3
on t1.product_id=t3.product_id
left join
(
	select
	    m3.product_id,
	    count(m1.user_id) as fx_cnt--7日推广次数
    from
    (
    	select
	        ad_material_id as ad_id,
	        user_id
	    from 
	        origin_common.cc_ods_log_cctapp_click_hourly
	    where 
	        ds>= '${begin_date}'
		and
			ds<= '${end_date}' 
		and 
			ad_type in ('search','category') 
		and 
			module = 'detail_material' 
		and 
			zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC')
	    union all
	    select
	        ad_id,
	        user_id
	    from 
	        origin_common.cc_ods_log_cctapp_click_hourly
	    where 
	        ds>= '${begin_date}'
		and
			ds<= '${end_date}'
		and 
			ad_type not in ('search','category') 
		and 
			module = 'detail_material' 
		and 
			zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC')
		union all
	    select
	        s2.ad_id,
	        s1.user_id
	    from
        (
        	select
	            ad_material_id,
	            user_id
	        from
	            origin_common.cc_ods_log_cctapp_click_hourly
	        where 
	            ds>= '${begin_date}'
			and
				ds<= '${end_date}' 
			and 
				module='vip' 
			and 
				ad_type in ('single_product','9_cell') 
			and 
				zone in ('material_group-share','material_moments-share')
        ) s1
	    inner join
        (
        	select
	            distinct 
	            ad_material_id as ad_material_id,
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
    (
    	select
	        ad_id,
	        item_id
	    from 
	        origin_common.cc_ods_fs_dwxk_ad_items_daily
    ) m2
    on 
        m1.ad_id = m2.ad_id
    inner join
    (
    	select
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
		count(a1.third_tradeno) as pay_count_30d,--30日订单数
		count(a2.order_sn) as refund_count_30d--30日发货后退款数
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
		on 
			s1.cck_uid=s2.cck_uid
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
		  	t1.order_sn
		from
		  	origin_common.cc_ods_fs_refund_order t1
		inner join
		  	origin_common.cc_order_user_delivery_time t2
		on
		  	t1.order_sn = t2.order_sn
		where
            from_unixtime(t1.create_time,'yyyyMMdd')>='${begin_date_30d}' 
        and
            from_unixtime(t1.create_time,'yyyyMMdd')<='${end_date}'
		and
			t1.status = 1
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
		count(a2.rate_id) as eva_cnt,--30日评价数
		sum(if(a2.star_num=1,1,0)) as bad_eva_cnt--30日差评数
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
		on 
			s1.cck_uid=s2.cck_uid
		where
			s1.ds >= '${begin_date_30d}'
		and
			s1.ds <= '${end_date}'
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
		  	ds >= '${begin_date_30d}'
		and 
            ds <= '${end_date}' 
		and
    		rate_id != 0
		and
			order_sn!='170213194354LFo017564wk'
	)a2
	on 
		a1.third_tradeno=a2.order_sn
	group by
		a1.product_id
)t6
on t1.product_id = t6.product_id
left join
(
	select
		a1.product_id,
		count(a2.order_sn) as count_delivery,--30日发货数
		sum(if(a2.delivery_time - a1.create_time > 86400,1,0)) as count_delivery_overtime--30日超时发货数
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
		on 
			s1.cck_uid=s2.cck_uid
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
        and 
		    ds<='${end_date}'
	)a2
	on 
		a1.third_tradeno = a2.order_sn
	group by
		a1.product_id
)t7
on t1.product_id=t7.product_id
left join
(
	select
		a1.last_cid,
		a2.pdname1,--商品一级类目
		a3.pdname2--商品二级类目
	from
	(
		select
			last_cid,
			c1,
			c2
		from
			origin_common.cc_category_cascade
		where
			ds = '${end_date}'
	)a1
	left join
	(
		select
			cid,
			name as pdname1
		from
			origin_common.cc_ods_fs_category
	)a2
	on a1.c1=a2.cid
	left join
	(
		select
			cid,
			name as pdname2
		from
			origin_common.cc_ods_fs_category
	)a3
	on a1.c2=a3.cid
)t8
on t3.category_id=t8.last_cid
left join
(
	select
		product_id,
		cn_title--商品名称
	from
		origin_common.cc_ods_fs_product
)t9
on t1.product_id = t9.product_id

###### 店铺维度数据##############
select
	p1.shop_id,--店铺id
	p2.shop_name,--店铺名称
	p3.cname1,--店铺一级类目
	p4.cname2,--店铺二级类目
	(case	
	when p1.shop_id in (19903,20305,20322) then '百诺优品'
	when p1.shop_id = 18470 then '冰冰购'
	when p1.shop_id in (18532,19141,19268,19347,20471) then '代发'
	when p1.shop_id in (18635,18240) then '极便利'
	when p1.shop_id in (17791,18731) then '京东'
	when p1.shop_id in (18704,18636) then '每日优鲜'
	when p1.shop_id in (18730,18723,18542,17636,18482,19089,19667,20203,20314,20343,20065,20548,20738,19517) then '其他'
	when p1.shop_id in (18662,18729,18588,18740,18799,19319,19405,19402,20216,18965,20696) then '生鲜'
	when p1.shop_id in (18706,18586,18569,18262,19392,18606,15426,18314,19534,2873,19708,2369,9872,19871,19756,19755,19709,16851,20179,17691,20242,456,3559,13930,15907,20513,20652,20653,20725,20789) then '小团子'
	when p1.shop_id = 18455 then '严选'
	when p1.shop_id in (18838,19239,19505,19504,19486,19470,19404,19527,19521,19525,19542,19613,19609,19599,19580,19664,19701,19699,19683,19682,19678,19765,19742,19722,19753,20016,19906,19907,20063,20064,20178,20168,20236,20237,20202,20188,4086,20697,20737,19627,20748,18327,12902,11974,12334,15670,15912,14715,5649,16898,15729,2752,12375,4599,13706,15395,12461,19654,16293,4024,20353,17929,104,3037,19170,14948,1793,19207,4999,16137,3885,16671,18791,17210,5987,14956,1341,15499,1555,18381,16194,5107,16133,8670,2254,18253,3803,17773,13698,17576,14832,18565,739,9349,7693,14720,15044,13638,7200,4318,12033,12766,17639,13363,16305,15853,6163,11500,9806,4539,20818,17845,12523,13559,13991,1412,14823,15129,1655,17157,17397,1802,18057,1831,18812,18814,1937,7572,9621,20784) then '一亩田'
	when p1.shop_id in (18335,18164,17801) then '自营'
	when p1.shop_id in (19339,19468,19298,18491,19611,19435,18765,19870,20142,20332,18574,20392,20423,20543,20600,20770) then '总监店铺'
	else 'pop' end) as tab,--店铺类型
	p1.count_pd,--动销商品数
	p1.pay_count,--7日订单数
	p1.fee,--7日支付金额
	p1.ipv_uv,--7日ipv_uv
	p1.fx_cnt,--7日推广数
	p1.pay_count_30d,--30日订单数
	p1.refund_count_30d,--30日发货退款单数
	(p1.refund_count_30d/p1.pay_count_30d) as rate_refund,--退款率即退款数除以订单数
	p1.eva_cnt,--30日评价数
	p1.bad_eva_cnt,--30日差评数
	(p1.bad_eva_cnt/p1.eva_cnt) as rate_bad,--差评率
	p1.count_delivery,--30日发货数
	p1.count_delivery_overtime,--30日超时发货数
	(p1.count_delivery_overtime/p1.count_delivery) as rate_delivery_overtime,--超时发货率
	p5.count_task,--工单数
	p5.count_task_overtime,--超时工单数
	(p5.count_task_overtime/p5.count_task) as rate_task_overtime--超时工单率
from
(
	select
		t3.shop_id,
		count(t1.product_id) as count_pd,--动销商品数
		sum(t1.pay_count) as pay_count,--7日订单数
		sum(t1.fee) as fee,--7日支付金额
		sum(t2.ipv_uv) as ipv_uv,--7日ipv_uv
		sum(t4.fx_cnt) as fx_cnt,--7日推广数
		sum(t5.pay_count_30d) as pay_count_30d,--30日订单数
		sum(t5.refund_count_30d) as refund_count_30d,--30日发货退款单数
		sum(t6.eva_cnt) as eva_cnt,--30日评价数
		sum(t6.bad_eva_cnt) as bad_eva_cnt,--30日差评数
		sum(t7.count_delivery) as count_delivery,--30日发货数
		sum(t7.count_delivery_overtime) as count_delivery_overtime--30日超时发货数
	from
	(
		select
			s1.product_id,
			count(s1.third_tradeno) as pay_count,--7日订单数
			sum(s1.item_price/100) as fee--7日支付金额
		from
			origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
		join
			origin_common.cc_ods_dwxk_fs_wk_cck_user s2
		on 
			s1.cck_uid=s2.cck_uid
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
			sum(a1.ipv_uv) as ipv_uv--7日ipv_uv
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
	    count(m1.user_id) as fx_cnt--7日推广次数
		from
	    (
	    	select
		        ad_material_id as ad_id,
		        user_id
		    from 
		        origin_common.cc_ods_log_cctapp_click_hourly
		    where 
		        ds >= '${begin_date}'
			and
				ds <= '${end_date}' 
			and 
				ad_type in ('search','category') 
			and 
				module = 'detail_material' 
			and 
				zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC')
		    union all
		    select
		        ad_id,
		        user_id
		    from 
		        origin_common.cc_ods_log_cctapp_click_hourly
		    where 
		        ds >= '${begin_date}'
			and
				ds <= '${end_date}'
			and 
				ad_type not in ('search','category') 
			and 
				module = 'detail_material' 
			and 
				zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC')
		    union all
		    select
		        s2.ad_id,
		        s1.user_id
		    from
	        (
	        	select
		            ad_material_id,
		            user_id
		        from
		            origin_common.cc_ods_log_cctapp_click_hourly
		        where 
		            ds >= '${begin_date}'
				and
					ds <= '${end_date}' 
				and 
					module = 'vip' 
				and 
					ad_type in ('single_product','9_cell') 
				and 
					zone in ('material_group-share','material_moments-share')
	        ) s1
		    inner join
	        (
	        	select
		            distinct 
		            ad_material_id as ad_material_id,
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
	    (
	    	select
		        ad_id,
		        item_id
		    from 
		        origin_common.cc_ods_fs_dwxk_ad_items_daily
	    ) m2
	    on 
	        m1.ad_id = m2.ad_id
	    inner join
	    (
	    	select
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
			count(a1.third_tradeno) as pay_count_30d,--30日订单数
			count(a2.order_sn) as refund_count_30d--30日发货后退款数
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
			on 
				s1.cck_uid=s2.cck_uid
			where
				s1.ds >= '${begin_date_30d}'
			and
				s1.ds <= '${end_date}'
			and
				s2.platform = 14
			and
				s2.ds = '${end_date}'
		)a1
		left join
		(
			select 
				distinct
			  	t1.order_sn
			from
			  	origin_common.cc_ods_fs_refund_order t1
			inner join
			  	origin_common.cc_order_user_delivery_time t2
			on
			  	t1.order_sn = t2.order_sn
			where
	            from_unixtime(t1.create_time,'yyyyMMdd')>='${begin_date_30d}' 
	        and
	            from_unixtime(t1.create_time,'yyyyMMdd')<='${end_date}'
			and
				t1.status = 1
		)a2
		on 
			a1.third_tradeno = a2.order_sn
		group by
			a1.product_id
	)t5
	on t1.product_id = t5.product_id
	left join
	(
		select
			a1.product_id,
			count(a2.rate_id) as eva_cnt,--30日评价数
			sum(if(a2.star_num=1,1,0)) as bad_eva_cnt--30日差评数
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
			on 
				s1.cck_uid=s2.cck_uid
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
			  	ds >= '${begin_date_30d}'
		  	and
			  	ds <= '${end_date}'
   			and
	    		rate_id != 0
    		and 
	    		order_sn!='170213194354LFo017564wk'
		)a2
		on a1.third_tradeno = a2.order_sn
		group by
			a1.product_id
	)t6
	on t1.product_id = t6.product_id
	left join
	(
		select
			a1.product_id,
			count(a2.order_sn) as count_delivery,--30日发货数
			sum(if(a2.delivery_time - a1.create_time > 86400,1,0)) as count_delivery_overtime--30超时发货数
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
			on 
				s1.cck_uid = s2.cck_uid
			where
				s1.ds >= '${begin_date_30d}'
			and
				s1.ds <= '${end_date}'
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
     			ds >= '${begin_date_30d}' 
		  	and
			  	ds <= '${end_date}'
		)a2
		on 
			a1.third_tradeno=a2.order_sn
		group by
			a1.product_id
	)t7
	on 
		t1.product_id=t7.product_id
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
left join
(
	select
		s1.shop_id,
		count(s1.id) as count_task,--工单数
		sum(s1.is_overtime) as count_task_overtime--超时工单数
	from
	(
		select
			shop_id,
			id,
			is_overtime,
			order_id
		from 
			cc_ods_fs_task
		where 
			from_unixtime(created_on,'yyyyMMdd') >= '${begin_date_30d}' 
		and 
			from_unixtime(created_on,'yyyyMMdd') <= '${end_date}'
	) s1 
	inner join
	(
		select
			distinct 
			m1.third_tradeno as order_sn--订单号
		from 
			cc_ods_dwxk_wk_sales_deal_ctime m1
		inner join
			cc_ods_dwxk_fs_wk_cck_user m2
		on 
			m1.cck_uid=m2.cck_uid
		where 
			m1.ds>='${begin_date_30d}' 
		and 
			m1.ds<='${end_date}' 
		and 
			m2.platform =14 
		and 
			m2.ds='${end_date}'
	) s2
	on 
		s1.order_id=s2.order_sn
	group by 
		s1.shop_id
)p5
on p1.shop_id=p5.shop_id

#### 服务数据7日自然日数据
select
	p1.pay_count_7d,--7日订单数
	p1.refun_count_delivery_7d,--7日发货退款数
	(p1.refun_count_delivery_7d/p1.pay_count_7d) as rate_refund_dealivery,--退货率
	p1.count_vl,--7日评价数
	p1.count_pl,--7日差评数
	(p1.count_pl/p1.count_vl) as rate_pingjia,--差评率
	p2.count_task,--7日工单数
	p2.count_task_overtime,--7日超时工单数
	(p2.count_task_overtime/p2.count_task) as rate_task_overtime,--超时工单率
	p1.count_delivery_7d,--7日发货订单数
	p1.count_delivery_overtime,--7日超时发货订单数
	(p1.count_delivery_overtime/p1.count_delivery_7d) as rate_delivery_overtime,--超时发货率
	p1.delivery_time,--平均发货时间
	p1.count_ship_7d,
	p1.count_ship_overtime,
	(p1.count_ship_overtime/p1.count_ship_7d) as rate_ship_overtime,
	p1.time_avg
from
(
	select
		1 as tab,
		t1.pay_count_7d,--7日订单数
		t2.refun_count_delivery_7d,--7日发货退款数
		t3.count_vl,--7日评价数
		t3.count_pl,--7日差评数
		t4.count_delivery_7d,--7日发货数
		t4.count_delivery_overtime,--7日超时发货数
		t4.delivery_time,----平均发货时间
		t5.count_ship_7d,
		t5.count_ship_overtime,
		t5.time_avg
	from
	(
		select
			1 as tab,
			count(distinct third_tradeno) as pay_count_7d--7日订单数
		from
			origin_common.cc_ods_dwxk_wk_sales_deal_ctime 
		where
			ds>= '${begin_date_7d}'
		and
			ds<= '${end_date}'
	)t1
	left join
	(
		select
			1 as tab,
			count(a1.order_sn) as refun_count_delivery_7d--7日发货后退款数
  		from
		(
			select 
				distinct
				s1.order_sn
			from
			  	origin_common.cc_ods_fs_refund_order s1
			inner join
			  	origin_common.cc_order_user_delivery_time s2
			on
			  	s1.order_sn = s2.order_sn
			where
			  	s2.ds >= 20181001
		  	and
			  	s1.status=1
		  	and
			  	from_unixtime(s1.create_time,'yyyyMMdd') >= '${begin_date_7d}'
		  	and
			  	from_unixtime(s1.create_time,'yyyyMMdd') <= '${end_date}'
		)a1
		inner join
		(
			select
				distinct
				third_tradeno as order_sn
			from
				origin_common.cc_ods_dwxk_wk_sales_deal_ctime 
			where
				ds>= '${begin_date_40d}' 
		)a2
		on a1.order_sn=a2.order_sn
	)t2
	on t1.tab=t2.tab
	left join
	(
		select
			1 as tab,
			count(distinct a1.rate_id) as count_vl,--7日评价数
			sum(if(a1.star_num=1,1,0)) as count_pl--7日差评数
		from
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
		    	order_sn!='170213194354LFo017564wk'
	   		and
			  	ds >= '${begin_date_7d}'
		  	and
			  	ds <= '${end_date}'
		)a1
		inner join
		(
			select
				distinct
				third_tradeno as order_sn
			from
				origin_common.cc_ods_dwxk_wk_sales_deal_ctime 
			where
				ds>= '${begin_date_40d}' 
		)a2
		on a1.order_sn=a2.order_sn
	)t3
	on t1.tab=t3.tab
	left join
	(
		select
			1 as tab,
			count(distinct a1.order_sn) as count_delivery_7d,--7日发货数
			sum(if(a1.delivery_time - a2.create_time >86400,1,0)) as count_delivery_overtime,--7日超时发货数
			avg((a1.delivery_time - a2.create_time)/3600) as delivery_time--平均发货时间
		from
		(
			select
			    order_sn,
			    delivery_time
	    	from 
	    		origin_common.cc_order_user_delivery_time
	   		where 
	     		ds>='${begin_date_7d}' 
     		and 
	     		ds <= '${end_date}' 
		)a1
		inner join
		(
			select
				distinct
				third_tradeno as order_sn,
				create_time
			from
				origin_common.cc_ods_dwxk_wk_sales_deal_ctime 
			where
				ds>= '${begin_date_40d}' 
		)a2
		on a1.order_sn=a2.order_sn
	)t4
	on t1.tab=t4.tab
	left join
	(
		select
			1 as tab,
			count(distinct a1.order_sn) as count_ship_7d,
			sum(if(a1.time >80*3600,1,0)) as count_ship_overtime,
			avg(a1.time/3600) as time_avg
		from
		(
			select
			    distinct
				order_sn,
				(update_time - create_time) as time
			from
				data.cc_cct_product_ship_info
			where
				ship_state =3
			and
				ds >= '${begin_date_7d}'
			and
				ds <= '${end_date}'
		)a1
		inner join
		(
			select
				distinct
				third_tradeno as order_sn
			from
				origin_common.cc_ods_dwxk_wk_sales_deal_ctime 
			where
				ds>= '${begin_date_40d}' 
		)a2
		on a1.order_sn = a2.order_sn
	)t5
	on t1.tab=t5.tab
)p1
left join
(
	select
		1 as tab,
		count(a1.id) as count_task,--工单数
		sum(a1.is_overtime) as count_task_overtime--超时工单数
	from
	(
		select
			1 as tab,
			id,
			is_overtime,
			order_id
		from
			origin_common.cc_ods_fs_task
		where
			from_unixtime(created_on,'yyyyMMdd') >='${begin_date_7d}' 
		and
			from_unixtime(created_on,'yyyyMMdd') <= '${end_date}' 
	)a1
	inner join
	(
		select
			distinct
			third_tradeno as order_sn
		from
			origin_common.cc_ods_dwxk_wk_sales_deal_ctime 
		where
			ds>= '${begin_date_40d}' 
	)a2
	on a1.order_id=a2.order_sn
)p2
on p1.tab=p2.tab

抢购数据=产品汇总数据
select
	count(m1.product_id)   as count_pd,
	sum(m2.ipv_uv)         as ipv_uv,
	sum(m3.fx_cnt)         as fx_cnt,
	sum(m1.pay_count)      as pay_count,
	sum(m1.fee)            as fee,
	sum(m1.cck_commission) as cck_commission
from
(
	select
		p1.date,
	 	p1.product_id as product_id,
	 	count(distinct p2.third_tradeno) as pay_count,
	 	sum(p2.item_price/100) as fee,
	 	sum(p2.cck_commission/100) as cck_commission
	from
	(
		select
			n1.date,
	 		n2.product_id,
	 		min(n1.begin_time) as begin_time,
	 		max(n1.end_time) as end_time
		from
		(
			select
				from_unixtime(begin_time,'yyyyMMdd') as date,
	 			ad_material_id,
	 			begin_time,
	 			end_time
			from
	 			origin_common.cc_ods_fs_cck_xb_policies_hourly
			where
	 			from_unixtime(begin_time,'yyyyMMdd') >= '${begin_date}'
 			and
	 			from_unixtime(begin_time-86400,'yyyyMMdd') < '${end_date}'
			and
	 			ad_key like 'seckill-tab%'
		) as n1
		inner join
		(
			select
	 			ad_material_id,
	 			product_id,
	 			operator
			from
	 			origin_common.cc_ods_fs_cck_ad_material_products_hourly
			where
	 			ad_material_id >0
		) as n2
		on n1.ad_material_id = n2.ad_material_id
		where
			n2,product_id not in()
		group by
			n1.date,
	 		n2.product_id
	) as p1
	left join
	(
		select
			ds as date,
	 		product_id,
		 	third_tradeno,
		 	item_price,
		 	cck_commission,
		 	create_time
		from
	 		origin_common.cc_ods_dwxk_wk_sales_deal_ctime
		where
	 		ds >= '${begin_date}'
		and
			ds <= '${end_date}'
	) as p2
	on p1.product_id = p2.product_id and p1.date=p2.date
	group by
	 	p1.date,
	 	p1.product_id
)m1
left join
(
	select
		ds as date,
		product_id,
		count(distinct user_id) as ipv_uv
	from
		origin_common.cc_ods_log_cctui_product_coupon_detail_hourly
	where
		ds >= '${begin_date}'
	and
		ds <= '${end_date}'
	and
		detail_type='item'
	group by
		ds,
		product_id
)m2
on m1.product_id=m2.product_id and m1.date = m2.date
left join
(
	select
		t1.ds as date,
	    t3.product_id,
	    count(t1.user_id) as fx_cnt
	from
	(
		select
			ds,
			ad_material_id as ad_id,
			user_id
	    from 
	        origin_common.cc_ods_log_cctapp_click_hourly
	    where 
	        ds >= '${begin_date}'
		and
			ds <= '${end_date}' 
		and 
			ad_type in ('search','category') 
		and 
			module = 'detail_material' 
		and 
			zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC')
	    union all
	    select
	        ds,
	        ad_id,
	        user_id
	    from 
	        origin_common.cc_ods_log_cctapp_click_hourly
	    where 
	        ds >= '${begin_date}'
		and
			ds <= '${end_date}' 
		and 
			ad_type not in ('search','category') 
		and 
			module = 'detail_material' 
		and 
			zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC')
	    union all
	    select
	    	s1.ds,
	        s2.ad_id,
	        s1.user_id
	    from
        (
        	select
	        	ds,
	            ad_material_id,
	            user_id
	        from
	            origin_common.cc_ods_log_cctapp_click_hourly
	        where 
	            ds >= '${begin_date}'
			and
				ds <= '${end_date}' 
			and 
				module='vip' 
			and 
				ad_type in ('single_product','9_cell') 
			and 
				zone in ('material_group-share','material_moments-share')
        ) s1
	    inner join
        (
        	select
	            distinct 
	            ad_material_id as ad_material_id,
	            ad_id
	        from 
	            data.cc_dm_gwapp_new_ad_material_relation_hourly
	        where 
	            ds >= '${begin_date}'
			and
				ds <= '${end_date}'
        ) s2
	    on  
	        s1.ad_material_id = s2.ad_material_id
    ) as t1
    inner join
    (
    	select
	        ad_id,
	        item_id
	    from 
	        origin_common.cc_ods_fs_dwxk_ad_items_daily
    ) t2
    on 
        t1.ad_id = t2.ad_id
    inner join
    (
    	select
	        item_id,
	        app_item_id as product_id
	    from 
	        origin_common.cc_ods_dwxk_fs_wk_items
	) t3
    on 
        t3.item_id = t2.item_id
	group by
		t1.ds,
    	t3.product_id
)m3
on m1.product_id=m3.product_id and m1.date=m3.date

抢购运营数据=按负责人分的产品数据
select
	m1.author,
	count(m1.product_id)   as count_pd,
	sum(m2.ipv_uv)         as ipv_uv,
	sum(m3.fx_cnt)         as fx_cnt,
	sum(m1.pay_count)      as pay_count,
	sum(m1.fee)            as fee,
	sum(m1.cck_commission) as cck_commission
from
(
	select
		p1.date,
	 	p1.product_id as product_id,
	 	p1.author,
	 	count(distinct p2.third_tradeno) as pay_count,
	 	sum(p2.item_price/100) as fee,
	 	sum(p2.cck_commission/100) as cck_commission
	from
	(
		select
			n1.date,
	 		n2.product_id,
	 		min(n1.begin_time) as begin_time,
	 		max(n1.end_time) as end_time,
	 		max(operator) as author
		from
		(
			select
				from_unixtime(begin_time,'yyyyMMdd') as date,
	 			ad_material_id,
	 			begin_time,
	 			end_time
			from
	 			origin_common.cc_ods_fs_cck_xb_policies_hourly
			where
	 			from_unixtime(begin_time,'yyyyMMdd') >= '${begin_date}'
	 		and
	 			from_unixtime(begin_time-86400,'yyyyMMdd') < '${end_date}'
			and
	 			ad_key like 'seckill-tab%'
		) as n1
		inner join
		(
			select
	 			product_id,
	 			ad_material_id,
	 			operator
			from
	 			origin_common.cc_ods_fs_cck_ad_material_products_hourly
			where
	 			ad_material_id >0
		) as n2
		on n1.ad_material_id = n2.ad_material_id
		where
			n2.product_id not in()
		group by
			n1.date,
	 		n2.product_id
	) as p1
	left join
	(
		select
			ds as date,
	 		product_id,
		 	third_tradeno,
		 	item_price,
		 	cck_commission,
		 	create_time
		from
	 		origin_common.cc_ods_dwxk_wk_sales_deal_ctime
		where
	 		ds >= '${begin_date}'
		and
			ds <= '${end_date}'
	) as p2
	on p1.product_id = p2.product_id and p1.date=p2.date
	group by
	 	p1.date,
	 	p1.product_id,
	 	p1.author
)m1
left join
(
	select
		ds as date,
		product_id,
		count(distinct user_id) as ipv_uv
	from
		origin_common.cc_ods_log_cctui_product_coupon_detail_hourly
	where
		ds >= '${begin_date}'
	and
		ds <= '${end_date}'
	and
		detail_type='item'
	group by
		ds,
		product_id
)m2
on m1.product_id=m2.product_id and m1.date = m2.date
left join
(
	select
		t1.ds as date,
	    t3.product_id,
	    count(t1.user_id) as fx_cnt
	from
    (
    	select
			ds,
			ad_material_id as ad_id,
			user_id
	    from 
	        origin_common.cc_ods_log_cctapp_click_hourly
	    where 
	        ds >= '${begin_date}'
		and
			ds <= '${end_date}' 
		and 
			ad_type in ('search','category') 
		and 
			module = 'detail_material' 
		and 
			zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC')
	    union all
	    select
			ds,
			ad_id,
			user_id
	    from 
	        origin_common.cc_ods_log_cctapp_click_hourly
	    where 
	        ds >= '${begin_date}'
		and
			ds <= '${end_date}' 
		and 
			ad_type not in ('search','category') 
		and 
			module = 'detail_material' 
		and 
			zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC')
	    union all
	    select
	    	s1.ds,
	        s2.ad_id,
	        s1.user_id
	    from
        (
        	select
	        	ds,
	            ad_material_id,
	            user_id
	        from
	            origin_common.cc_ods_log_cctapp_click_hourly
	        where 
	            ds >= '${begin_date}'
			and
				ds <= '${end_date}' 
			and 
				module='vip' 
			and 
				ad_type in ('single_product','9_cell') 
			and 
				zone in ('material_group-share','material_moments-share')
        ) s1
	    inner join
        (
        	select
	            distinct 
	            ad_material_id as ad_material_id,
	            ad_id
	        from 
	            data.cc_dm_gwapp_new_ad_material_relation_hourly
	        where 
	            ds >= '${begin_date}'
			and
				ds <= '${end_date}'
        ) s2
	    on  
	        s1.ad_material_id = s2.ad_material_id
    ) as t1
    inner join
    (
    	select
	        ad_id,
	        item_id
	    from 
	        origin_common.cc_ods_fs_dwxk_ad_items_daily
    ) t2
    on 
        t1.ad_id = t2.ad_id
    inner join
    (
    	select
	        item_id,
	        app_item_id as product_id
	    from 
	        origin_common.cc_ods_dwxk_fs_wk_items
	) t3
    on 
        t3.item_id = t2.item_id
	group by
		t1.ds,
    	t3.product_id
)m3
on m1.product_id=m3.product_id and m1.date=m3.date
group by
	m1.author

抢购商品数据=限时抢购商品数据=抢购活动运营数据
select
	m1.date,--日期
	m1.product_id,--商品id
	m5.cn_title,--商品名称
	m4.pdname1,--商品一级类目
	m4.pdname2,--商品二级类目
	m1.author,--负责人
	m2.ipv_uv,--ipv_uv
	m3.fx_cnt,--分享数
	m1.pay_count,--订单数
	m1.fee,--支付金额
	m1.cck_commission--直接佣金
from
(
	select
		p1.date,--日期
	 	p1.product_id as product_id,--商品id
	 	p1.author,--负责人
	 	count(distinct p2.third_tradeno) as pay_count,--订单数
	 	sum(p2.item_price/100) as fee,--支付金额
	 	sum(p2.cck_commission/100) as cck_commission--直接佣金
	from
	(
		select
			n1.date,
	 		n2.product_id,
	 		min(n1.begin_time) as begin_time,
	 		max(n1.end_time) as end_time,
	 		max(operator) as author
		from
		(
			select
				from_unixtime(begin_time,'yyyyMMdd') as date,
	 			ad_material_id,
	 			begin_time,
	 			end_time
			from
	 			origin_common.cc_ods_fs_cck_xb_policies_hourly
			where
	 			from_unixtime(begin_time,'yyyyMMdd') >= '${begin_date}'
 			and
	 			from_unixtime(begin_time-86400,'yyyyMMdd') < '${end_date}'
			and
	 			ad_key like 'seckill-tab%'
		) as n1
		inner join
		(
			select
	 			ad_material_id,
	 			product_id,
	 			operator--提报人
			from
	 			origin_common.cc_ods_fs_cck_ad_material_products_hourly
			where
	 			ad_material_id >0
		) as n2
		on n1.ad_material_id = n2.ad_material_id
		where n2.product_id not in()
		group by
			n1.date,
	 		n2.product_id
	) as p1
	left join
	(
		select
			ds as date,
	 		product_id,
		 	third_tradeno,
		 	item_price,
		 	cck_commission,
		 	create_time
		from
	 		origin_common.cc_ods_dwxk_wk_sales_deal_ctime
		where
	 		ds >= '${begin_date}'
		and
			ds <= '${end_date}'
	) as p2
	on p1.product_id = p2.product_id and p1.date=p2.date
	group by
	 	p1.date,
	 	p1.product_id,
	 	p1.author
)m1
left join
(
	select
		ds as date,
		product_id,
		count(distinct user_id) as ipv_uv
	from
		origin_common.cc_ods_log_cctui_product_coupon_detail_hourly
	where
		ds >= '${begin_date}'
	and
		ds <= '${end_date}'
	and
		detail_type='item'
	group by
		ds,
		product_id
)m2
on m1.product_id=m2.product_id and m1.date = m2.date
left join
(
	select
		t1.ds as date,
	    t3.product_id,
	    count(t1.user_id) as fx_cnt
	from
    (
    	select
			ds,
			ad_material_id as ad_id,
			user_id
	    from 
	        origin_common.cc_ods_log_cctapp_click_hourly
	    where 
	        ds >= '${begin_date}'
		and
			ds <= '${end_date}' 
		and 
			ad_type in ('search','category') 
		and 
			module = 'detail_material' 
		and 
			zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC')
	    union all
	    select
			ds,
			ad_id,
			user_id
	    from 
	        origin_common.cc_ods_log_cctapp_click_hourly
	    where 
	        ds >= '${begin_date}'
		and
			ds <= '${end_date}' 
		and 
			ad_type not in ('search','category') 
		and 
			module = 'detail_material' 
		and 
			zone in ('circleFriendPro','wechatPro','circleFriendPQC','wechatPQC')
	    union all
	    select
	    	s1.ds,
	        s2.ad_id,
	        s1.user_id
	    from
        (
        	select
	        	ds,
	            ad_material_id,
	            user_id
	        from
	            origin_common.cc_ods_log_cctapp_click_hourly
	        where 
	            ds >= '${begin_date}'
			and
				ds <= '${end_date}' 
			and 
				module='vip' 
			and 
				ad_type in ('single_product','9_cell') 
			and 
				zone in ('material_group-share','material_moments-share')
        ) s1
	    inner join
        (
        	select
	            distinct 
	            ad_material_id as ad_material_id,
	            ad_id
	        from 
	            data.cc_dm_gwapp_new_ad_material_relation_hourly
	        where 
	            ds >= '${begin_date}'
			and
				ds <= '${end_date}'
        ) s2
	    on  
	        s1.ad_material_id = s2.ad_material_id
    ) as t1
    inner join
    (
    	select
	        ad_id,
	        item_id
	    from 
	        origin_common.cc_ods_fs_dwxk_ad_items_daily
    ) t2
    on 
        t1.ad_id = t2.ad_id
    inner join
    (
    	select
	        item_id,
	        app_item_id as product_id
	    from 
	        origin_common.cc_ods_dwxk_fs_wk_items
	) t3
    on 
        t3.item_id = t2.item_id
	group by
		t1.ds,
    	t3.product_id
)m3
on m1.product_id=m3.product_id and m1.date=m3.date
left join
(
	select
		a1.product_id,
		a3.pdname1,
		a4.pdname2
	from
	(
		select
			distinct
			app_item_id as product_id,
			category_id
		from
			origin_common.cc_ods_dwxk_fs_wk_items
	)a1
	left join
	(
		select
			last_cid,
			c1,
			c2
		from
			origin_common.cc_category_cascade
		where
			ds = '${end_date}'
	)a2
	on  a1.category_id=a2.last_cid
	left join
	(
		select
			cid,
			name as pdname1
		from
			origin_common.cc_ods_fs_category
	)a3
	on a2.c1=a3.cid
	left join
	(
		select
			cid,
			name as pdname2
		from
			origin_common.cc_ods_fs_category
	)a4
	on a2.c2=a4.cid
)m4
on m1.product_id= m4.product_id
left join
(
	select
		product_id,
		cn_title
	from
		origin_common.cc_ods_fs_product
)m5
on m1.product_id= m5.product_id
