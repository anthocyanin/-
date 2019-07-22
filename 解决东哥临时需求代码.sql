解决东哥临时需求代码
select
	p1.date,
	p1.pay_count,
	p2.eva_count,
	p2.bad_eva_count,
	p4.count_refund,
	p5.cur_count_refund,
	p6.ser_cnt,
	p6.over_ser_cnt,
	p7.order_count_delivery_success,
	p7.delivery_time_sum,
	p7.over_delivery_cnt
from
(
	select
		ds as date,
		count(distinct third_tradeno) as pay_count,
		sum(item_price/100) as pay_fee--item_price/100把分变成元
	from
		origin_common.cc_ods_dwxk_wk_sales_deal_ctime
	where
		ds >= '${begin_date_30dago}'
		and
		ds <= '${end_date_yesterday}'
	group by
		ds
)p1
left join
(--当日楚楚通有效评价数
	select
		t1.date,
		count(distinct t1.rate_id) as eva_count,--评价数
		sum(if(t1.star_num = 1,1,0)) as bad_eva_count--差评数
	from
	(
		select
			distinct
			ds as date,
			order_sn,
			rate_id,
			star_num
		from
			origin_common.cc_rate_star
		where
			rate_id !=0
			and
			ds >= '${begin_date_30dago}'
			and
			ds <= '${end_date_yesterday}'
	)t1
	inner join
	(
		select
			distinct
			third_tradeno as order_sn
		from
			origin_common.cc_ods_dwxk_wk_sales_deal_ctime
		where
			ds >= '${begin_date_60dago}'
			and
			ds <= '${end_date_yesterday}'
	)t2
	on t1.order_sn=t2.order_sn
	group by
		t1.date
)p2
on p1.date=p2.date
left join
(
	select
		t1.date,
		count(t1.order_sn) as count_refund--退款数
	from
	(
		select
			distinct
			from_unixtime(create_time,'yyyyMMdd') as date,
			order_sn
		from
			origin_common.cc_ods_fs_refund_order
		where
			create_time >= unix_timestamp('${begin_date_30dago}','yyyyMMdd')
		and
			status=1
	)t1
	inner join
	(
		select
			distinct
			t1.third_tradeno as order_sn
		from
			origin_common.cc_ods_dwxk_wk_sales_deal_ctime t1
		inner join
			origin_common.cc_order_user_delivery_time t2
		on 
			t1.third_tradeno = t2.order_sn
		where
			t1.ds >= '${begin_date_60dago}'
		and
			t1.ds <= '${end_date_yesterday}'
		and
			t2.ds >= '${begin_date_60dago}'
		and
			t2.ds <= '${end_date_yesterday}'
	)t2
	on t1.order_sn=t2.order_sn
	group by
		t1.date
)p4
on p1.date=p4.date
left join
( --评价数，差评数,退货数
	select
		t1.ds,
		count(t4.order_sn) as cur_count_refund--发货后退款数
	from
	(
		select
			distinct
			ds,
			third_tradeno as order_sn
		from
			origin_common.cc_ods_dwxk_wk_sales_deal_ctime
		where
			ds >= '${begin_date_30dago}'
			and
			ds <= '${end_date_yesterday}'
	)t1
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
		and
			t1.create_time >= unix_timestamp('${begin_date_30dago}','yyyyMMdd')
		and
			t1.status=1
	)t4--发货后退款订单表
	on t1.order_sn=t4.order_sn
	group by
		t1.ds
)p5
on p1.date=p5.ds
left join
(
	select
		from_unixtime(created_on,'yyyyMMdd') as date,
		count(id) as ser_cnt,--不知道什么意思
		sum(if(is_overtime=1,1,0)) as over_ser_cnt--不知道什么意思
	from
		origin_common.cc_ods_fs_task
	where
		from_unixtime(created_on,'yyyyMMdd') >= '${begin_date_30dago}'
	group by from_unixtime(created_on,'yyyyMMdd')
)p6
on p1.date = p6.date
left join
(--发货订单数，发货时间
  select
    s2.ds,
    count(s1.order_sn) as order_count_delivery_success,--每天发货数
    sum(s1.delivery_time - s2.create_time) as delivery_time_sum,--每天所有订单的发货时长之和
    sum(if(s1.delivery_time-s2.create_time>=24*3600,1,0)) as over_delivery_cnt--超时发货数
  from
    (
    select
      order_sn,
      delivery_time
    from origin_common.cc_order_user_delivery_time
    where ds>= '${begin_date_30dago}'
  ) as s1
  inner join
  (
    select 
      ds,
      third_tradeno as order_sn,
      create_time
    from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where ds>='${begin_date_30dago}'
  ) as s2
  on s1.order_sn = s2.order_sn
  group by s2.ds
) as p7
on p7.ds=p1.date
/////////////////////////////////////////////////////////////////
    select
  	  shop_id,
  	  shop_name,
  	  count(order_id) as task_num,
  	  sum(if (is_overtime=1,1,0)) as overtime_task_num
  	from  cc_ods_fs_task
  	where created_on>1529251200 and created_on<1529683200
  	group by shop_id, shop_name
  