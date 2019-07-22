取食品类目等
select
      t1.product_id as product_id,
      t2.product_title as product_title,
      t2.product_cname1 as product_cname1,
      t2.product_cname2 as product_cname2,
      t2.product_cname3 as product_cname3,
      t2.shop_id as shop_id,
      t2.shop_title as shop_title,
      t1.pay_cnt as pay_cnt,
      t1.pay_fee as pay_fee,
      t3.ipv_uv as ipv_uv
    from
    (
      select
        product_id,
        count(distinct order_sn) as pay_cnt,
        sum(product_price*product_count)           as total_fee,
        sum(product_discount_price*product_count)  as pay_fee
      from cc_order_products_user_pay_time 
      where ds>=20180501 and ds<=20180610 and (source_channel&1=0 and source_channel&2=0 and source_channel&4=0)
      group by product_id
    ) t1
    inner join
    (
      select
        product_id, 
        product_title, 
        product_cname1, 
        product_cname2,
        product_cname3,
        shop_id, 
        shop_title
      from data.cc_dw_fs_products_shops where product_c1=-5
    ) t2
    on t1.product_id=t2.product_id
    left join
    (
      select
        product_id,
        sum(ipv_uv) as ipv_uv
      from data.cc_dm_product_flow_stat 
      where ds>=20180501 and ds<=20180610
      group by product_id
    ) t3
    on t1.product_id=t3.product_id

    /////////////////////////////////////////////////////////////  
留存率
select
  from_unixtime(t1.create_time,'yyyyMMdd') as pay_date,
  count(distinct t1.cck_uid) as pay_cck_cnt,
  sum(if(from_unixtime(t1.create_time,'yyyyMMdd')=t2.date,1,0))/count(distinct t1.cck_uid)  as cur_pay_order_cck_rate,
  sum(if(from_unixtime(t1.create_time+86400,'yyyyMMdd')=t2.date,1,0))/count(distinct t1.cck_uid)  as pay_order_cck_rate1,
  sum(if(from_unixtime(t1.create_time+86400*2,'yyyyMMdd')=t2.date,1,0))/count(distinct t1.cck_uid)  as pay_order_cck_rate2,
  sum(if(from_unixtime(t1.create_time+86400*3,'yyyyMMdd')=t2.date,1,0))/count(distinct t1.cck_uid)  as pay_order_cck_rate3,
  sum(if(from_unixtime(t1.create_time+86400*4,'yyyyMMdd')=t2.date,1,0))/count(distinct t1.cck_uid)  as pay_order_cck_rate4,
  sum(if(from_unixtime(t1.create_time+86400*5,'yyyyMMdd')=t2.date,1,0))/count(distinct t1.cck_uid)  as pay_order_cck_rate5,
  sum(if(from_unixtime(t1.create_time+86400*6,'yyyyMMdd')=t2.date,1,0))/count(distinct t1.cck_uid)  as pay_order_cck_rate6,
  sum(if(from_unixtime(t1.create_time+86400*7,'yyyyMMdd')=t2.date,1,0))/count(distinct t1.cck_uid)  as pay_order_cck_rate7,
  sum(if(from_unixtime(t1.create_time+86400*14,'yyyyMMdd')=t2.date,1,0))/count(distinct t1.cck_uid)  as pay_order_cck_rate14,
  sum(if(from_unixtime(t1.create_time+86400*30,'yyyyMMdd')=t2.date,1,0))/count(distinct t1.cck_uid)  as pay_order_cck_rate30
from
(   
  select
    cck_uid,
    create_time
  from cc_ods_fs_wk_cct_layer_info
  where platform=14 and create_time>=1527782400 and create_time<1528905600
) t1
left join
(
   select
      s1.date    as date,
      s2.cck_uid as cck_uid
   from
   (
     select
       distinct ds as date,
       cct_uid
     from origin_common.cc_ods_log_gwapp_pv_hourly  
     where ds>=20180601 and ds<20180614 and module  = 'https://app-h5.daweixinke.com/chuchutui/index.html'
   ) s1
   inner join
   (
      select
        distinct cct_uid as cct_uid,
        cck_uid
      from cc_ods_dwxk_fs_wk_cck_user_hourly
      where ds=20180614
    ) s2
    on s1.cct_uid=s2.cct_uid
)t2
on t1.cck_uid=t2.cck_uid
group by from_unixtime(t1.create_time,'yyyyMMdd')
////////////////////////////////////////////////////////////////////
激活率
select
  from_unixtime(t1.create_time,'yyyyMMdd') as pay_date,
  count(distinct t1.cck_uid) as pay_cck_cnt,

  concat
  (
  cast
  (count(distinct if(t2.create_time is not null and unix_timestamp(from_unixtime(t2.create_time,'yyyyMMdd'),'yyyyMMdd')-
  unix_timestamp(from_unixtime(t1.create_time,'yyyyMMdd'),'yyyyMMdd')=0,t2.cck_uid,null))/count(distinct t1.cck_uid)*100 as decimal(20,2)
  ),'%'
  ) as cur_pay_order_cck_rate,

  concat(cast(count(distinct if(t2.create_time is not null and unix_timestamp(from_unixtime(t2.create_time,'yyyyMMdd'),'yyyyMMdd')-
    unix_timestamp(from_unixtime(t1.create_time,'yyyyMMdd'),'yyyyMMdd')<86400*2,t2.cck_uid,null))/count(distinct t1.cck_uid)*100 as decimal(20,2)),'%'
  ) as pay_order_cck_rate1,
  concat(cast(count(distinct if(t2.create_time is not null and 
  unix_timestamp(from_unixtime(t2.create_time,'yyyyMMdd'),'yyyyMMdd')-
  unix_timestamp(from_unixtime(t1.create_time,'yyyyMMdd'),'yyyyMMdd')<86400*3,t2.cck_uid,null))/count(distinct t1.cck_uid)*100 as decimal(20,2)),'%'
  ) as pay_order_cck_rate2,
  concat(cast(count(distinct if(t2.create_time is not null and 
  unix_timestamp(from_unixtime(t2.create_time,'yyyyMMdd'),'yyyyMMdd')-
  unix_timestamp(from_unixtime(t1.create_time,'yyyyMMdd'),'yyyyMMdd')<86400*4,t2.cck_uid,null))/count(distinct t1.cck_uid)*100 as decimal(20,2)),'%'
  ) as pay_order_cck_rate3,
  concat(cast(count(distinct if(t2.create_time is not null and 
  unix_timestamp(from_unixtime(t2.create_time,'yyyyMMdd'),'yyyyMMdd')-
  unix_timestamp(from_unixtime(t1.create_time,'yyyyMMdd'),'yyyyMMdd')<86400*5,t2.cck_uid,null))/count(distinct t1.cck_uid)*100 as decimal(20,2)),'%'
  ) as pay_order_cck_rate4,
  concat(cast(count(distinct if(t2.create_time is not null and 
  unix_timestamp(from_unixtime(t2.create_time,'yyyyMMdd'),'yyyyMMdd')-
  unix_timestamp(from_unixtime(t1.create_time,'yyyyMMdd'),'yyyyMMdd')<86400*6,t2.cck_uid,null))/count(distinct t1.cck_uid)*100 as decimal(20,2)),'%'
  ) as pay_order_cck_rate5,
  concat(cast(count(distinct if(t2.create_time is not null and 
  unix_timestamp(from_unixtime(t2.create_time,'yyyyMMdd'),'yyyyMMdd')-
  unix_timestamp(from_unixtime(t1.create_time,'yyyyMMdd'),'yyyyMMdd')<86400*7,t2.cck_uid,null))/count(distinct t1.cck_uid)*100 as decimal(20,2)),'%'
  ) as pay_order_cck_rate6,
  concat(cast(count(distinct if(t2.create_time is not null and 
  unix_timestamp(from_unixtime(t2.create_time,'yyyyMMdd'),'yyyyMMdd')-
  unix_timestamp(from_unixtime(t1.create_time,'yyyyMMdd'),'yyyyMMdd')<86400*8,t2.cck_uid,null))/count(distinct t1.cck_uid)*100 as decimal(20,2)),'%'
  ) as pay_order_cck_rate7,
  concat(cast(count(distinct if(t2.create_time is not null and 
  unix_timestamp(from_unixtime(t2.create_time,'yyyyMMdd'),'yyyyMMdd')-
  unix_timestamp(from_unixtime(t1.create_time,'yyyyMMdd'),'yyyyMMdd')<86400*15,t2.cck_uid,null))/count(distinct t1.cck_uid)*100 as decimal(20,2)),'%'
  ) as pay_order_cck_rate14,
  concat(cast(count(distinct if(t2.create_time is not null and 
  unix_timestamp(from_unixtime(t2.create_time,'yyyyMMdd'),'yyyyMMdd')-
  unix_timestamp(from_unixtime(t1.create_time,'yyyyMMdd'),'yyyyMMdd')<86400*31,t2.cck_uid,null))/count(distinct t1.cck_uid)*100 as decimal(20,2)),'%'
  ) as pay_order_cck_rate30
from
(   
  select
    cck_uid,
    create_time
  from cc_ods_fs_wk_cct_layer_info
  where platform =14 and create_time>=1527782400 and create_time<1528905600 --今日0点时间搓
) t1
left join
(
 select
   cck_uid,
   create_time
 from cc_ods_dwxk_wk_sales_deal_ctime
 where ds>=20180601 and create_time>=1527782400 and create_time<1528905600
)t2
on t1.cck_uid=t2.cck_uid
group by from_unixtime(t1.create_time,'yyyyMMdd')


//////////////////////////////////////////////////////////////////////////////

select
  from_unixtime(t1.create_time,'yyyyMMdd') as date,
  count(distinct t1.cck_uid) as cur_cnt,
  count(distinct t2.cck_uid) as thor_cnt
from 
(   
  select
    cck_uid,
    create_time
  from cc_ods_fs_wk_cct_layer_info
  where platform=14 and create_time>=1527782400 and create_time<1528905600 --今日0点时间搓
) t1
left join
(
 select
   cck_uid,
   create_time
 from cc_ods_dwxk_wk_sales_deal_ctime
 where ds>=20180601 and ds<20180603
) t2
on t1.cck_uid=t2.cck_uid
group by from_unixtime(t1.create_time,'yyyyMMdd')