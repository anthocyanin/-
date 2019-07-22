




select
  o1.*
from
(select
p1.*,
p2.refund_rate,
p2.prd_cnt,
rank() over(partition by p1.product_cname1,p1.product_cname2,p1.product_cname3 order by p1.refund_rate/p2.refund_rate desc) as num
from
(select
  n1.product_id,
  n1.order_count,
  n1.pay_fee,
  if(n2.refund_cnt is null,0,n2.refund_cnt) as refund_cnt,
  n3.product_title,
  n3.shop_id,
  n3.shop_title,
  n3.product_cname1,
  n3.product_cname2,
  n3.product_cname3,
  if(n2.refund_cnt is null,0,n2.refund_cnt/n1.order_count) as refund_rate
from
(select
  product_id,
  count(third_tradeno) as order_count,
  sum(item_price/100) as pay_fee
from
  origin_common.cc_ods_dwxk_wk_sales_deal_ctime
where
  ds >= '${begin_date}'
and
  ds <= '${end_date}'
group by
  product_id
) as n1
left join
(select
  a1.product_id,
  count(a1.third_tradeno) as refund_cnt
from
(select
  ds,
  product_id,
  third_tradeno
from
  origin_common.cc_ods_dwxk_wk_sales_deal_ctime
where
  ds >= '${begin_date}'
and
  ds <= '${end_date}'
) as a1
inner join
(select
  order_sn
from
  origin_common.cc_order_user_delivery_time
where
  ds >= '${begin_date}'  
) as a2
on a1.third_tradeno = a2.order_sn
inner join
(
select
  order_sn,
  from_unixtime(create_time,'yyyyMMdd') as refund_date
from
  origin_common.cc_ods_fs_refund_order
where
  create_time >= unix_timestamp('${begin_date}','yyyyMMdd')
) as a3
on a1.third_tradeno = a3.order_sn
where
  unix_timestamp(a3.refund_date,'yyyyMMdd') - unix_timestamp(a1.ds,'yyyyMMdd') <= 8*3600*24
group by
  a1.product_id
) as n2
on n1.product_id = n2.product_id
left join
(select
  product_id,
  product_title,
  shop_id,
  shop_title,
  product_cname1,
  product_cname2,
  product_cname3
from
  data.cc_dw_fs_products_shops
) as n3
on n1.product_id = n3.product_id
) as p1
left join
(select
  n3.product_cname1,
  n3.product_cname2,
  n3.product_cname3,
  sum(n2.refund_cnt)/sum(n1.order_count) as refund_rate,
  count(n1.product_id) as prd_cnt
from
(select
  product_id,
  count(third_tradeno) as order_count,
  sum(item_price/100) as pay_fee
from
  origin_common.cc_ods_dwxk_wk_sales_deal_ctime
where
  ds >= '${begin_date}'
and
  ds <= '${end_date}'
group by
  product_id
) as n1
left join
(select
  a1.product_id,
  count(a1.third_tradeno) as refund_cnt
from
(select
  ds,
  product_id,
  third_tradeno
from
  origin_common.cc_ods_dwxk_wk_sales_deal_ctime
where
  ds >= '${begin_date}'
and
  ds <= '${end_date}'
) as a1
inner join
(select
  order_sn
from
  origin_common.cc_order_user_delivery_time
where
  ds >= '${begin_date}'  
) as a2
on a1.third_tradeno = a2.order_sn
inner join
(
select
  order_sn,
  from_unixtime(create_time,'yyyyMMdd') as refund_date
from
  origin_common.cc_ods_fs_refund_order
where
  create_time >= unix_timestamp('${begin_date}','yyyyMMdd')
) as a3
on a1.third_tradeno = a3.order_sn
where
  unix_timestamp(a3.refund_date,'yyyyMMdd') - unix_timestamp(a1.ds,'yyyyMMdd') <= 8*3600*24
group by
  a1.product_id
) as n2
on n1.product_id = n2.product_id
left join
(select
  product_id,
  product_cname1,
  product_cname2,
  product_cname3
from
  data.cc_dw_fs_products_shops
) as n3
on n1.product_id = n3.product_id
group by
  n3.product_cname1,n3.product_cname2,n3.product_cname3
) as p2
on p1.product_cname1 = p2.product_cname1 and p1.product_cname2 = p2.product_cname2 and p1.product_cname3 = p2.product_cname3
where
p1.order_count > 100 and p2.prd_cnt >10 and p2.refund_rate > 0 
) as o1
where
  o1.num <= 3


select
  n1.product_id,
  n2.refund_reason
from
(select
  product_id,
  third_tradeno
from
  origin_common.cc_ods_dwxk_wk_sales_deal_ctime
where
  ds >= '${begin_date}'
and
  ds <= '${end_date}'
and
  product_id in (
11001845774,
110018164332,
110018704540,
11001748994,
11001884310,
11001889314,
11001926861,
11001748985,
1100179818,
11001578127,
110018704727,
10011548165,
100181102,
110017981164,
110018124423,
110017801107,
110017845308,
11001780123,
11000415827,
110018704726,
110018704455,
11000973568,
11001777221,
110018704446,
110017981172,
1100115685,
11001809245,
10014502308,
110018174256,
11008960476,
110018704422,
110181429,
1001497531,
11001800716,
110017702303,
110018704386,
11001882717,
1001769347,
10004360184,
11001735615,
1100188603,
10002776406,
10013589612,
110012964529,
11001748519,
11001780116,
110181571,
1101706081,
1100178765,
110017769148
    )
) as n1
inner join
(select
  order_sn,
  refund_reason
from
  origin_common.cc_ods_fs_refund_order
where
  create_time >= unix_timestamp('${begin_date}','yyyyMMdd')
) as n2
on n1.third_tradeno = n2.order_sn
inner join
(select
  order_sn
from
  origin_common.cc_order_user_delivery_time
where
  ds >= '${begin_date}'  
) as n3
on n1.order_sn = n3.order_sn


