明细
select 
	shop_id,
	shop_name,
	order_sn,
	ship_sn,
	ship_com,
	pay_time,
	delivery_time,
	delivery_address,
	from_unixtime(first_logistics_time,'yyyyMMdd') as frist_logistics_time
	tag
from
	slq_sham_detailed 
where 
    delivery_time >='${begin_date}'
and 
	delivery_time <='${end_date}'
and 
	date='${statis_date}';
////////////////////////////////////////////////////////////////////////////////////////////////////
汇总
select 
	shop_id,
	shop_name,
	delivery_date,
	sum(delivery_num) as delivery_num,
	sum(sham_num) as sham_num
from
	slq_sham_count
where 
    delivery_date >=20180701
and 
	delivery_date <=20180901
and 
    date=20180901
group by
	shop_id,
	shop_name,
	delivery_date;
////////////////////////////////////////////////////////////////////////////////////////////////////
select 
	shop_id,
	shop_name,
	order_sn,
	ship_sn,
	ship_com,
	pay_time,
	delivery_time,
	delivery_address,
	from_unixtime(first_logistics_time,'yyyyMMdd') as frist_logistics_time,
	tag
from
	slq_sham_detailed 
where 
    delivery_time >=20180701
and 
	delivery_time <=20180901
and 
	date=20180901


[mysql.hosts.shangcheng_statistic]
host = "m6002.mysql.internal.chuchujie.com"
port = "6002"
user = "statistic"
pass = "qatwCP5bSdJpB42n" 
