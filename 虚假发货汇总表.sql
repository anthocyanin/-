select 
	shop_id,
	shop_name,
	delivery_date,
	sum(delivery_num) as delivery_num ,
	sum(sham_num) as sham_num
from
	slq_sham_count
where 
    delivery_date >='${begin_date}'
and 
	delivery_date <='${end_date}'
and 
    date='${statis_date}'
group by
	shop_id,
	shop_name,
	delivery_date;
