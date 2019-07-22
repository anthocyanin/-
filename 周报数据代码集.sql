####大盘 ipv_uv
select
	avg(t2.ipv_uv) as ipv_uv,
	avg(t2.count_pd) as count_pd
from
(
	select
		t1.ds,
		count(t1.product_id) as count_pd,
		sum(t1.ipv_uv) as ipv_uv
	from
	(  
		select
			ds,
			product_id,
			count(distinct user_id) as ipv_uv
		from cc_ods_log_cctui_product_coupon_detail_hourly
		where ds>= '${begin_date}' and ds<= '${end_date}' and detail_type ='item' 
		group by ds,product_id
	)t1
	group by t1.ds
)t2

### 商品数据
select
	t1.product_id,--商品id
	t9.cn_title,--商品名称
	t8.pdname1,--一级类目
	t8.pdname2,--二级类目
	t3.shop_id,--店铺id
	t3.shop_name,--店铺名称
    (case
	when t3.shop_id in (18164,18335,17801,18628,18635) then '自营'
	when t3.shop_id = 17791 then '京东'
	when t3.shop_id in (18532,19347) then '甄选代发'
	when t3.shop_id in (19141,19268)  then '自营代发'
	when t3.shop_id = 18455 then '严选'
	when t3.shop_id = 18470 then '冰冰购'
	when t3.shop_id in (18636,18704) then '每日优鲜'
	when t3.shop_id in (18641,18642,18643,18644) then '拼多多'
	when t3.shop_id in (17927,18253,17891) then '村淘'
	else 'pop' end) as tab,--店铺类型
	t1.pay_count,--订单数
	t1.fee,--支付金额
	t2.ipv_uv,--ipv_uv
	t4.fx_cnt,--推广次数
	t5.pay_count_30d,--30日订单数
	t5.refund_count_30d,--30日发货后退款数
	(t5.refund_count_30d/t5.pay_count_30d) as rate_refund,--发货退款率
	t6.eva_cnt,--有效评价数
	t6.bad_eva_cnt,--差评数
	(t6.bad_eva_cnt/t6.eva_cnt) as rate_bad,--差评率
	t7.count_delivery,--30日发货订单数
	t7.count_delivery_overtime,--超时发货订单数
	(t7.count_delivery_overtime/t7.count_delivery) as rate_delivery_overtime--超时发货率
from
(
	select
		s1.product_id,
		count(s1.third_tradeno) as pay_count,--订单数
		sum(s1.item_price/100) as fee
	from
		origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
	join
		origin_common.cc_ods_dwxk_fs_wk_cck_user s2
	on s1.cck_uid=s2.cck_uid
	where s1.ds>= '${begin_date}' and s1.ds<= '${end_date}' and s2.platform =14 and s2.ds = '${end_date}'
	group by s1.product_id
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
		from origin_common.cc_ods_log_cctui_product_coupon_detail_hourly
		where ds>= '${begin_date}' and ds<= '${end_date}' and detail_type='item'
		group by ds,product_id
	)a1
	group by a1.product_id
)t2
on t1.product_id=t2.product_id
left join
(
	select
		distinct
		s1.app_item_id as product_id,
		s1.shop_id,--店铺id
		s2.shop_name,--店铺名称
		s1.category_id,--类目id
		s2.category1
	from
		origin_common.cc_ods_dwxk_fs_wk_items s1
	left join
		origin_common.cc_ods_fs_business_basic s2
	on s1.shop_id=s2.shop_id
)t3
on t1.product_id=t3.product_id
left join
(
	select
    m3.product_id,
    count(m1.user_id) as fx_cnt--分享次数
from
    (select
      ad_material_id as ad_id,
      user_id
    from origin_common.cc_ods_log_cctapp_click_hourly
    where ds>= '${begin_date}'and ds<= '${end_date}' and ad_type in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
    union all
    select
      ad_id,
      user_id
    from origin_common.cc_ods_log_cctapp_click_hourly
    where ds>= '${begin_date}'and ds<= '${end_date}'and ad_type not in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
    union all
    select
        s2.ad_id,
        s1.user_id
    from
        (select
            ad_material_id,
            user_id
        from origin_common.cc_ods_log_cctapp_click_hourly
        where ds>= '${begin_date}'and ds<= '${end_date}' and module='vip' and ad_type in ('single_product','9_cell') and zone in ('material_group-share','material_moments-share')
        ) s1
    inner join
        (select
            distinct ad_material_id as ad_material_id,
            ad_id
        from data.cc_dm_gwapp_new_ad_material_relation_hourly
        where ds>= '${begin_date}'and ds<= '${end_date}'
        ) s2
    on s1.ad_material_id = s2.ad_material_id
    ) as m1
    inner join
    (select
        ad_id,
        item_id
    from origin_common.cc_ods_fs_dwxk_ad_items_daily
    ) m2
    on m1.ad_id = m2.ad_id
    inner join
    (select
        item_id,
        app_item_id as product_id
    from origin_common.cc_ods_dwxk_fs_wk_items
    ) m3
    on m3.item_id = m2.item_id
group by m3.product_id
)t4
on t1.product_id=t4.product_id
left join
(
	select
		a1.product_id,
		count(a1.third_tradeno) as pay_count_30d,
		count(a2.order_sn) as refund_count_30d--楚楚推30天内发货后的退款单数
	from
	(
		select
			distinct
			s1.product_id,
			s1.third_tradeno
		from origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
		join
			origin_common.cc_ods_dwxk_fs_wk_cck_user s2
		on s1.cck_uid=s2.cck_uid
		where s1.ds>= '${begin_date_30d}'and s1.ds<= '${end_date}'and s2.platform =14 and s2.ds = '${end_date}'
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
		on t1.order_sn = t2.order_sn
		where t2.ds >= '${begin_date_30d}'
	)a2
	on a1.third_tradeno=a2.order_sn
	group by a1.product_id
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
	group by a1.product_id
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
	group by a1.product_id
)t7
on t1.product_id=t7.product_id
left join
(
	select
		a1.last_cid,--不知道
		a2.pdname1,--不知道
		a3.pdname2--不知道
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
		cn_title
	from
		origin_common.cc_ods_fs_product
)t9
on t1.product_id = t9.product_id

###### 店铺维度数据
select
	p1.shop_id,--店铺id
	p2.shop_name,--店铺名称
	p3.cname1,--一级类目
	p4.cname2,--二级类目
    (case
	when p1.shop_id in (18164,18335,17801,18628,18635) then '自营'
	when p1.shop_id = 17791 then '京东'
	when p1.shop_id in (18532,19347) then '甄选代发'
	when p1.shop_id in (19141,19268)  then '自营代发'
	when p1.shop_id = 18455 then '严选'
	when p1.shop_id = 18470 then '冰冰购'
	when p1.shop_id in (18636,18704) then '每日优鲜'
	when p1.shop_id in (18641,18642,18643,18644) then '拼多多'
	when p1.shop_id in (17927,18253,17891) then '村淘'
	else 'pop' end) as tab,--店铺类型
	p1.count_pd,--动销商品数
	p1.pay_count,--订单数
	p1.fee,--支付金额
	p1.ipv_uv,--ipv_uv
	p1.fx_cnt,--推广次数
	p1.pay_count_30d,--30日订单数
	p1.refund_count_30d,--30日发货后退款数
	(p1.refund_count_30d/p1.pay_count_30d) as rate_refund,--30日发货退货率
	p1.eva_cnt,--有效评价数
	p1.bad_eva_cnt,--差评数
	(p1.bad_eva_cnt/p1.eva_cnt) as rate_bad,--差评率
	p1.count_delivery,--30日发货订单数
	p1.count_delivery_overtime,--30日超时发货订单数
	(p1.count_delivery_overtime/p1.count_delivery) as rate_delivery_overtime--超时发货率
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
			count(s1.third_tradeno) as pay_count,
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

#### 服务数据7日自然日数据
select
	p1.pay_count_7d,--7日订单数
	p1.refun_count_delivery_7d,--7日发货后退款数
	(p1.refun_count_delivery_7d/p1.pay_count_7d) as rate_refund_dealivery,--7日发货后退款率
	p1.count_vl,--评价数
	p1.count_pl,--差评数
	(p1.count_pl/p1.count_vl) as rate_pingjia,--差评率
	p2.count_task,--工单数
	p2.count_task_overtime,--超时工单数
	(p2.count_task_overtime/p2.count_task) as rate_task_overtime,--工单超时率
	p1.count_delivery_7d,--7日发货订单数
	p1.count_delivery_overtime,--7日超时发货订单数
	(p1.count_delivery_overtime/p1.count_delivery_7d) as rate_delivery_overtime,--7日超时发货订单率
	p1.delivery_time,--7日发货时长
	p1.count_ship_7d,--7日签收单数
	p1.count_ship_overtime,--7日物流超时单数
	(p1.count_ship_overtime/p1.count_ship_7d) as rate_ship_overtime,--7日物流超时单率
	p1.time_avg--7日物流时长
from
(
	select
		1 as tab,
		t1.pay_count_7d,
		t2.refun_count_delivery_7d,
		t3.count_vl,
		t3.count_pl,
		t4.count_delivery_7d,
		t4.count_delivery_overtime,
		t4.delivery_time,
		t5.count_ship_7d,
		t5.count_ship_overtime,
		t5.time_avg
	from
	(
		select
			1 as tab,
			count(distinct third_tradeno) as pay_count_7d
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
			count(a1.order_sn) as refun_count_delivery_7d
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
			  	s2.ds >= 20180401
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
				ds>= 20180401
		)a2
		on a1.order_sn=a2.order_sn
	)t2
	on t1.tab=t2.tab
	left join
	(
		select
			1 as tab,
			count(distinct a1.rate_id) as count_vl,
			sum(if(a1.star_num=1,1,0)) as count_pl
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
			  	ds >= '${begin_date_7d}'
		)a1
		inner join
		(
			select
				distinct
				third_tradeno as order_sn
			from
				origin_common.cc_ods_dwxk_wk_sales_deal_ctime 
			where
				ds>= 20180401
		)a2
		on a1.order_sn=a2.order_sn
	)t3
	on t1.tab=t3.tab
	left join
	(
		select
			1 as tab,
			count(distinct a1.order_sn) as count_delivery_7d,
			sum(if(a1.delivery_time - a2.create_time >86400,1,0)) as count_delivery_overtime,
			avg((a1.delivery_time - a2.create_time)/3600) as delivery_time
		from
		(
			select
		    order_sn,
		    delivery_time
    	from 
    		origin_common.cc_order_user_delivery_time
   		where 
     		ds>='${begin_date_7d}' 

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
				ds>= 20180401
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
				ds>= 20180401
		)a2
		on a1.order_sn = a2.order_sn
	)t5
	on t1.tab=t5.tab
)p1
left join
(
	select
		1 as tab,
		count(a1.id) as count_task,
		sum(a1.is_overtime) as count_task_overtime
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
	left join
	(
		select
			distinct
			third_tradeno as order_sn
		from
			origin_common.cc_ods_dwxk_wk_sales_deal_ctime 
		where
			ds>= 20180401
	)a2
	on a1.order_id=a2.order_sn
)p2
on p1.tab=p2.tab

#######  抢购数据 也是产品数据
select
	count(m1.product_id) as count_pd,--商品数
	sum(m2.ipv_uv) as ipv_uv,--ipv_uv
	sum(m3.fx_cnt) as fx_cnt,--推广次数
	sum(m1.pay_count) as pay_count,--付款单数
	sum(m1.fee) as fee,--支付金额
	sum(m1.cck_commission) as cck_commission--直接佣金

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
	    (select
	      ds,
	      ad_material_id as ad_id,
	      user_id
	    from 
	      origin_common.cc_ods_log_cctapp_click_hourly
	    where 
	      ds >= '${begin_date}'
		and
		ds <= '${end_date}' and ad_type in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
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
		ds <= '${end_date}' and ad_type not in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
	    union all
	    select
	    	s1.ds,
	        s2.ad_id,
	        s1.user_id
	    from
	        (select
	        	ds,
	            ad_material_id,
	            user_id
	        from
	            origin_common.cc_ods_log_cctapp_click_hourly
	        where 
	            ds >= '${begin_date}'
				and
				ds <= '${end_date}' and module='vip' and ad_type in ('single_product','9_cell') and zone in ('material_group-share','material_moments-share')
	        ) s1
	    inner join
	        (select
	            distinct ad_material_id as ad_material_id,
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
	    (select
	        ad_id,
	        item_id
	    from 
	        origin_common.cc_ods_fs_dwxk_ad_items_daily
	    ) t2
	    on 
	        t1.ad_id = t2.ad_id
	    inner join
	    (select
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

####### 抢购运营数据 也是产品数据 按负责人统计
select
	m1.author,--负责人
	count(m1.product_id) as count_pd,--商品数
	sum(m2.ipv_uv) as ipv_uv,--ipv_uv
	sum(m3.fx_cnt) as fx_cnt,--推广次数
	sum(m1.pay_count) as pay_count,--订单数
	sum(m1.fee) as fee,--支付金额
	sum(m1.cck_commission) as cck_commission--直接佣金

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
	    (select
	      ds,
	      ad_material_id as ad_id,
	      user_id
	    from 
	      origin_common.cc_ods_log_cctapp_click_hourly
	    where 
	      ds >= '${begin_date}'
		and
		ds <= '${end_date}' and ad_type in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
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
		ds <= '${end_date}' and ad_type not in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
	    union all
	    select
	    	s1.ds,
	        s2.ad_id,
	        s1.user_id
	    from
	        (select
	        	ds,
	            ad_material_id,
	            user_id
	        from
	            origin_common.cc_ods_log_cctapp_click_hourly
	        where 
	            ds >= '${begin_date}'
				and
				ds <= '${end_date}' and module='vip' and ad_type in ('single_product','9_cell') and zone in ('material_group-share','material_moments-share')
	        ) s1
	    inner join
	        (select
	            distinct ad_material_id as ad_material_id,
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
	    (select
	        ad_id,
	        item_id
	    from 
	        origin_common.cc_ods_fs_dwxk_ad_items_daily
	    ) t2
	    on 
	        t1.ad_id = t2.ad_id
	    inner join
	    (select
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
group by m1.author

### 抢购活动数据
select
	m1.date,--日期
	m1.product_id,--商品id
	m5.cn_title,--商品标题
	m4.pdname1,--商品一级类目
	m4.pdname2,--商品二级类目
	m1.author,--负责人
	m2.ipv_uv,--ipv_uv
	m3.fx_cnt,--总推广次数
	m1.pay_count,--订单数
	m1.fee,--支付金额
	m1.cck_commission--直接佣金
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
	    count(t1.user_id) as fx_cnt--总推广次数
	from
	    (select
	      ds,
	      ad_material_id as ad_id,
	      user_id
	    from 
	      origin_common.cc_ods_log_cctapp_click_hourly
	    where 
	      ds >= '${begin_date}'
		and
		ds <= '${end_date}' and ad_type in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
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
		ds <= '${end_date}' and ad_type not in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
	    union all
	    select
	    	s1.ds,
	        s2.ad_id,
	        s1.user_id
	    from
	        (select
	        	ds,
	            ad_material_id,
	            user_id
	        from
	            origin_common.cc_ods_log_cctapp_click_hourly
	        where 
	            ds >= '${begin_date}'
				and
				ds <= '${end_date}' and module='vip' and ad_type in ('single_product','9_cell') and zone in ('material_group-share','material_moments-share')
	        ) s1
	    inner join
	        (select
	            distinct ad_material_id as ad_material_id,
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
	    (select
	        ad_id,
	        item_id
	    from 
	        origin_common.cc_ods_fs_dwxk_ad_items_daily
	    ) t2
	    on 
	        t1.ad_id = t2.ad_id
	    inner join
	    (select
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
	m1.author




###### 抢购商品数据

////////////////////////////////////////////////////////////

pop商家
select
	t1.shop_id,--店铺ID
	t2.shop_name,--店铺名称
	t2.c1_name,--一级类目
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
	if(t3.shop_id is not null,1,0) as tab2,--店铺类型
	t1.count_pd,--在线商品数
	t6.pv_7d,--7日PV
	t4.count_order_pd_7d,--7日动销商品数
	t4.fee_7d,--7日支付金额
	t4.pay_count_7d,--7日订单数
	t4.cck_commission_7d,--7日直接佣金
	t5.count_updatepd_7d,--7日上新数
	t7.pv_30d,--30日PV
	t8.fee_30d,--30日支付金额
	t8.pay_count_30d,--30日订单数
	(t8.fee_30d / t8.pay_count_30d) as price_30d,--30日客单价
	(t8.pay_count_30d / t7.pv_30d) as order_rate_30d,--30日转化率
	t9.count_vl_30d,--30日有效评价数
	t9.count_pl_30d,--30日差评数
	(t9.count_pl_30d / t9.count_vl_30d) as rate_pl_30d,--30日差评率
	t9.refund_count_30d,--30日退款成功数
	(t9.refund_count_30d / t8.pay_count_30d) as rate_refund_30d,--30日退款率
	t9.order_delivery_30d,--30日发货订单数
	t9.delivery_time_30d,--30日平均发货时长
	t9.order_ship_30d,--30日签收订单数
	t9.time_30d,--30日平均物流时长
	t10.count_huihua,--会话数
	(t10.count_jiedai/t10.count_huihua) as rate_jiedai,--接待率
	(t10.totalwaitetime/t10.count_jiedai/60) as time_huifu--平均回复时长
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

