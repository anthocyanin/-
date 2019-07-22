副本
select
    n1.item_price as pay_fee,
    n1.order_count,
    n1.sales_num,
    n1.user_count,
    n2.pv,
    n2.ipv_uv,
    n3.user_count as self_user_count,
    n3.order_count as self_order_count,
    n4.fx_user_cnt as detail_fx_user_cnt,
    n5.fx_user_cnt,
    n5.fx_cnt,  
    n6.cck_count,
    n6.pv,
    n7.user_count as fx_od_user_cnt
from
(--订单数，支付金额，购买人数，佣金
  select
    product_id,
    count(third_tradeno) as order_count, --订单数
    sum(sale_num) as sales_num,  --销量
    count(distinct cck_uid) as user_count, --有成交用户数
    sum(item_price/100) as item_price,  --支付金额
    sum(cck_commission/100) as cck_commission --佣金
  from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
  where ds = '${stat_date}' and product_id = '${product_id}'
  group by product_id
) as n1
left join
( select
    a1.product_id,
    count(a1.cck_uid) as pv,
    count(distinct a1.cck_uid) as cck_count,
    count(distinct a1.user_id) as ipv_uv
  from
  ( select
      product_id,
      cck_uid,
      user_id
    from origin_common.cc_ods_log_cctui_product_coupon_detail_hourly 
    where ds = '${stat_date}' and product_id = '${product_id}' and detail_type='item' and is_in_app = 1
  ) as a1 
    group by a1.product_id
) as n2
on n1.product_id = n2.product_id
left join
(   select
      a0.product_id,
      count(distinct a0.third_tradeno) as order_count,
      count(distinct a0.cck_uid) as user_count
    from
    (   select
          product_id,
          third_tradeno,
          cck_uid,
          item_price,
          cck_commission
        from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
        where ds = '${stat_date}' and product_id = '${product_id}'
    ) as a0
     inner join
    (  select 
         distinct order_sn
       from  origin_common.cc_ods_log_gwapp_order_track_hourly
       where ds = '${stat_date}' and source='cctui'
    ) as a1
     on a0.third_tradeno = a1.order_sn
     group by a0.product_id
) as n3
on n1.product_id = n3.product_id
left join
(select
    m3.product_id,
    count(m1.user_id) as fx_cnt,
    count(distinct m1.user_id) as fx_user_cnt
from
    (select
      ad_material_id as ad_id,
      user_id
    from origin_common.cc_ods_log_cctapp_click_hourly
    where ds = '${stat_date}' and ad_type in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
    union all
    select
      ad_id,
      user_id
    from origin_common.cc_ods_log_cctapp_click_hourly
    where ds = '${stat_date}' and ad_type not in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
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
    where m3.product_id = '${product_id}'
    group by m3.product_id
) as n4
on n1.product_id = n4.product_id
left join
(select
    m3.product_id,
    count(m1.user_id) as fx_cnt,
    count(distinct m1.user_id) as fx_user_cnt
from
    (select
      ad_material_id as ad_id,
      user_id
    from origin_common.cc_ods_log_cctapp_click_hourly
    where ds = '${stat_date}' and ad_type in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
    union all
    select
      ad_id,
      user_id
    from origin_common.cc_ods_log_cctapp_click_hourly
    where ds = '${stat_date}' and ad_type not in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
    union all
    select
        s2.ad_id,
        s1.user_id
    from
        (select
            ad_material_id,
            user_id
        from origin_common.cc_ods_log_cctapp_click_hourly
        where ds = '${stat_date}' and module='vip' and ad_type in ('single_product','9_cell') and zone in ('material_group-share','material_moments-share')
        ) s1
    inner join
        (select
            distinct ad_material_id as ad_material_id,
            ad_id
        from data.cc_dm_gwapp_new_ad_material_relation_hourly
        where ds = '${stat_date}'
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
    where m3.product_id = '${product_id}'
    group by m3.product_id
) as n5
on n1.product_id = n5.product_id
left join
(select
    a1.product_id,
    count(a1.cck_uid) as pv,
    count(distinct a1.cck_uid) as cck_count,
    count(distinct a1.user_id) as ipv_uv
from
(select
    product_id,
    cck_uid,
    user_id
from origin_common.cc_ods_log_cctui_product_coupon_detail_hourly 
where ds = '${stat_date}'and product_id = '${product_id}'and detail_type='item'and is_in_app = 0
) as a1 
group by a1.product_id
) as n6
on n1.product_id = n6.product_id
left join
(select
    a0.product_id,
    count(distinct a0.cck_uid) as user_count
from
(select
    product_id,
    third_tradeno,
    cck_uid,
    item_price,
    cck_commission
from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
where ds = '${stat_date}'and product_id = '${product_id}'
) as a0
left join
(select distinct 
    order_sn
from origin_common.cc_ods_log_gwapp_order_track_hourly
where ds = '${stat_date}'and source='cctui'
) as a1
on a0.third_tradeno = a1.order_sn
where a1.order_sn is null
group by a0.product_id
) as n7
on n1.product_id = n7.product_id

/////////////////////////////////////////////////////////////////////////////////
我写白名单订单明细
select
  t1.shop_id as shop_id,
  t5.shop_name as shop_name,
  t1.order_sn as order_sn,
  t1.product_id as product_id,
  t1.product_title as product_title,
  t1.product_count as product_count,
  t1.pay_fee as pay_fee, 
  t2.province_name as province_name,
  t2.city_name as city_name,
  from_unixtime(t1.pay_time,'yyyyMMdd HH:mm:ss') as pay_time,
  if(t3.delivery_time is null,0,from_unix_time(t3.delivery_time,'yyMMdd HH:mm:ss')) as delivery_time,
  if(t4.create_time is null,0,from_unix_time(t4.create_time,'yyMMdd HH:mm:ss')) as create_time,
from
( select 
    s1.order_sn as order_sn, 
    s1.shop_id as shop_id,
    s1.area_id as area_id,
    s1.pay_fee as pay_fee,
    s2.product_id as product_id,
    s2.product_title as product_title,
    s2.pay_time as pay_time,
    s2.product_count as product_count 
  from
  ( select 
      order_sn,
      shop_id,
      area_id,
      pay_fee,
    from cc_order_user_pay_time
    where ds>=20180501 and ds<20180611 and source_channel=2 and shop_id in( )
  ) s1
  inner join 
  ( select 
      order_sn,
      product_id,
      product_title,
      pay_time,
      product_count 
    from cc_order_products_user_pay_time
    where ds>=20180501 and ds<20180611 
  ) s2
  on s1.order_sn=s2.order_sn 
) t1 
left join 
( select 
    area_id,
    province_name,
    city_name
  from cc_area_city_province
) t2
on t1.area_id=t2.area_id
left join 
( select 
    order_sn,
    delivery_time
  from cc_order_user_delivery_time
  where ds>=20180501 and shop_id in ()
) t3
on t1.order_sn=t3.order_sn 
left join 
( select 
    order_sn,
    create_time
  from cc_ods_fs_refund_order
  where shop_id in ()
) t4
on t1.order_sn=t4.order_sn 
left join 
( select 
    order_sn,
    shop_name
  from cc_ods_fs_shop
  where ds=20180619 
) t5
on t1.order_sn=t5.order_sn
//////////////////////////////////////////////////////////////////////////////////////////////////
select
  username,
  phone_number
from cc_ods_dwxk_fs_wk_cck_user
where create_time>=1528214400 and create_time<=1529424000 and platform=14
//////////////////////////////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////////////////////////////
SELECT 
  concat('\t',a.third_tradeno) as order_sn,
  from_unixtime(a.create_time,'%Y%m%d %H:%i:%s') as pay_time, 
  a.cck_uid as cck_uid,
  b.username as phone,
  c.real_name as real_name
FROM `wk_sales_deal` a
inner join `wk_cck_user` b
on a.cck_uid=b.cck_uid
inner join `wk_business_info` c
on a.cck_uid=c.cck_uid
where a.create_time>=1528441380 and a.create_time<1528513200 and a.status<3 and a.product_id=110018704560 and b.platform=18
////////////////////////////////////////////////////////////////////////////////////////////////////////
select
  count(distinct ip) as DAU 

from cc_ods_log_cctapp_click_hourly
where timestamp>=1528992000 and timestamp<=1529164800 and source=cctui
//////////////////////////////////////////////////////////////////////////////////////////
select
  t1.product_id as product_id,
  t1.sale_fee as sale_fee,
  t2.product_cname1 as product_cname1,
  t2.product_cname2 as product_cname2,
  t2.product_cname3 as product_cname3,
  t2.shop_id as shop_id,
  t2.shop_title
from 
(
 select
   s1.product_id as product_id,
   s1.product_price*s1.order_cnt as sale_fee
 from 
  (select
     product_id,
     order_sn,
     product_discount_price,
     count(distinct order_sn) as order_cnt
   from cc_order_products_user_pay_time
   where ds>=20180507 and ds<=20180621 
   
  ) s1
inner join 
  (select
     order_sn
   from cc_order_user_pay_time
   where ds>=20180507 and ds<=20180621 and source_channel=2
  ) s2
  on s1.order_sn=s2.order_sn
) t1
left join 
(select
   product_id,
   product_cname1,
   product_cname2,
   product_cname3,
   shop_id,
   shop_title
 from data.cc_dw_fs_products_shops
) t2
on s1.product_id=s3.product_id
////////////////////////////////////////////////////////////////////////////////////////
select
  t1.product_id as product_id,
  t1.order_cnt as order_cnt,
  t1.sale_fee as sale_fee,
  t2.product_cname1 as product_cname1,
  t2.product_cname2 as product_cname2,
  t2.product_cname3 as product_cname3,
  t2.shop_id as shop_id,
  t2.shop_title as shop_title,
  t2.product_title as product_title,
  t3.rate_num as rate_num,
  t3.bad_rate_num as bad_rate_num,
  t4.refund_order_num_after_delivery as refund_order_num_after_delivery
from 
(
 select
   s1.product_id as product_id,
   sum(s1.product_discount_price*s1.product_count) as sale_fee,
   count(distinct s1.order_sn) as order_cnt
 from 
  (select
     product_id,
     order_sn,
     product_discount_price,
     product_count
   from cc_order_products_user_pay_time
   where ds>=20180507 and ds<=20180621 
  ) s1
 left join 
  (select
     order_sn
   from cc_order_user_pay_time
   where ds>=20180507 and ds<=20180621 and source_channel=2
  ) s2
  on s1.order_sn=s2.order_sn
  group by s1.product_id
) t1
left join 
(select
   product_id,
   product_title,
   product_cname1,
   product_cname2,
   product_cname3,
   shop_id,
   shop_title
 from data.cc_dw_fs_products_shops
) t2
on t1.product_id=t2.product_id
inner join 
(select
   h1.product_id,
   count(h1.rate_id) as rate_num,--评价数
   sum(if (h1.star_num=1,1,0)) as bad_rate_num--差评价数
 from    
  (select 
     product_id,
     order_sn,
     rate_id,
     star_num
   from cc_rate_star 
   where ds>=20180507 and ds<=20180621 and rate_id>0 and order_sn != '170213194354LFo017564wk'
  ) h1
  left join 
  (select
     distinct third_tradeno as order_sn
   from cc_ods_dwxk_wk_sales_deal_ctime  
   where ds>=20180407 and ds<=20180621
  ) h2
  on h1.order_sn=h2.order_sn
  group by h1.product_id
) t3
on t1.product_id=t3.product_id
inner join 
(select
  n5.product_id as product_id,
  sum(if(n5.create_time-n5.delivery_time>0,1,0)) as refund_order_num_after_delivery--发货后退款数
from 
 (select
    n1.order_sn as order_sn,
    n1.delivery_time as delivery_time,
    n2.create_time as create_time,
    n3.product_id as product_id
  from 
   (select
      order_sn,
      delivery_time
    from cc_order_user_delivery_time  
    where ds>=20180507 and ds<=20180621 and delivery_time!=0
   ) n1
   inner join 
   (select
      order_sn,
      create_time
    from cc_ods_fs_refund_order  
    where create_time>=1525622400 and create_time<1529510400
   ) n2
   on n1.order_sn=n2.order_sn
   inner join
   (select
      distinct third_tradeno as order_sn,
      product_id
    from cc_ods_dwxk_wk_sales_deal_ctime  
    where ds>=20180407 and ds<=20180621
   ) n3
   on n1.order_sn=n3.order_sn
 ) n5 
  group by n5.product_id
 ) t4 
 on t1.product_id=t4.product_id

 /////////////////////////////////////////////////////////////////////////////

select
  t1.product_id as product_id,--商品id
  t1.order_cnt as order_cnt,--订单数
  t2.rate_num as rate_num,--评价数
  t2.bad_rate_num as bad_rate_num,--差评数
  t3.delivery_num as delivery_num,--发货数
  t3.refund_num_after_delivery as refund_num_after_delivery--发货退款数
from 
(
  select 
    s1.product_id as product_id,
    count(distinct s1.order_sn) as order_cnt
  from   
    (select
       product_id,
       order_sn
     from cc_order_products_user_pay_time
     where ds>=20180507 and ds<=20180621 
    ) s1
    left join
    (select
       order_sn
     from cc_order_user_pay_time
     where ds>=20180507 and ds<=20180621 and source_channel=2
    ) s2
    on s1.order_sn=s2.order_sn
    group by s1.product_id
) t1
left join 
(
 select
   h1.product_id,
   count(h1.rate_id) as rate_num,--评价数
   sum(if (h1.star_num=1,1,0)) as bad_rate_num--差评价数
 from    
  (select 
     product_id,
     order_sn,
     rate_id,
     star_num
   from cc_rate_star 
   where ds>=20180507 and ds<=20180621 and rate_id>0 and order_sn != '170213194354LFo017564wk'
  ) h1
  left join 
  (select
     distinct third_tradeno as order_sn
   from cc_ods_dwxk_wk_sales_deal_ctime  
   where ds>=20180407 and ds<=20180621
  ) h2
  on h1.order_sn=h2.order_sn
  group by h1.product_id
) t2
on t1.product_id=t2.product_id
left join 
(select
  n5.product_id as product_id,
  count(n5.delivery_time) as delivery_num,
  sum(if(n5.create_time-n5.delivery_time>0,1,0)) as refund_num_after_delivery--发货后退款数
from 
 (select
    n1.product_id as product_id,
    n2.create_time as create_time,
    n3.delivery_time as delivery_time 
  from 
   (select
      distinct third_tradeno as order_sn,
      product_id
    from cc_ods_dwxk_wk_sales_deal_ctime  
    where ds>=20180407 and ds<=20180621
   ) n1
   left join 
   (select
      order_sn,
      create_time
    from cc_ods_fs_refund_order  
    where create_time>=1525622400 and create_time<1529510400
   ) n2
   on n1.order_sn=n2.order_sn
   left join
   (select
      order_sn,
      delivery_time
    from cc_order_user_delivery_time  
    where ds>=20180507 and ds<=20180621 and delivery_time!=0
   ) n3
   on n1.order_sn=n3.order_sn
 ) n5 
  group by n5.product_id
) t3 
on t1.product_id=t3.product_id