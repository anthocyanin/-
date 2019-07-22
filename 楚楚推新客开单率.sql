select
  from_unixtime(t1.create_time,'yyyyMMdd') as date,
  count(distinct t1.cck_uid) as pay_cck_cnt,
  cast(count(distinct if(t2.create_time is not null and
  unix_timestamp(from_unixtime(t2.create_time,'yyyyMMdd'),'yyyyMMdd')-
  unix_timestamp(from_unixtime(t1.create_time,'yyyyMMdd'),'yyyyMMdd')=0,t2.cck_uid,null))/count(distinct t1.cck_uid)*100 as decimal(20,2)) as cur_pay_order_cck_rate,
  cast(count(distinct if(t2.create_time is not null and
  unix_timestamp(from_unixtime(t2.create_time,'yyyyMMdd'),'yyyyMMdd')-
  unix_timestamp(from_unixtime(t1.create_time,'yyyyMMdd'),'yyyyMMdd')<86400*2,t2.cck_uid,null))/count(distinct t1.cck_uid)*100 as decimal(20,2)) as pay_order_cck_rate1,
  cast(count(distinct if(t2.create_time is not null and
  unix_timestamp(from_unixtime(t2.create_time,'yyyyMMdd'),'yyyyMMdd')-
  unix_timestamp(from_unixtime(t1.create_time,'yyyyMMdd'),'yyyyMMdd')<86400*3,t2.cck_uid,null))/count(distinct t1.cck_uid)*100 as decimal(20,2)) as pay_order_cck_rate2,
  cast(count(distinct if(t2.create_time is not null and
  unix_timestamp(from_unixtime(t2.create_time,'yyyyMMdd'),'yyyyMMdd')-
  unix_timestamp(from_unixtime(t1.create_time,'yyyyMMdd'),'yyyyMMdd')<86400*4,t2.cck_uid,null))/count(distinct t1.cck_uid)*100 as decimal(20,2)) as pay_order_cck_rate3,
  cast(count(distinct if(t2.create_time is not null and
  unix_timestamp(from_unixtime(t2.create_time,'yyyyMMdd'),'yyyyMMdd')-
  unix_timestamp(from_unixtime(t1.create_time,'yyyyMMdd'),'yyyyMMdd')<86400*5,t2.cck_uid,null))/count(distinct t1.cck_uid)*100 as decimal(20,2)) as pay_order_cck_rate4,
  cast(count(distinct if(t2.create_time is not null and
  unix_timestamp(from_unixtime(t2.create_time,'yyyyMMdd'),'yyyyMMdd')-
  unix_timestamp(from_unixtime(t1.create_time,'yyyyMMdd'),'yyyyMMdd')<86400*6,t2.cck_uid,null))/count(distinct t1.cck_uid)*100 as decimal(20,2)) as pay_order_cck_rate5,
  cast(count(distinct if(t2.create_time is not null and
  unix_timestamp(from_unixtime(t2.create_time,'yyyyMMdd'),'yyyyMMdd')-
  unix_timestamp(from_unixtime(t1.create_time,'yyyyMMdd'),'yyyyMMdd')<86400*7,t2.cck_uid,null))/count(distinct t1.cck_uid)*100 as decimal(20,2)) as pay_order_cck_rate6,
  cast(count(distinct if(t2.create_time is not null and
  unix_timestamp(from_unixtime(t2.create_time,'yyyyMMdd'),'yyyyMMdd')-
  unix_timestamp(from_unixtime(t1.create_time,'yyyyMMdd'),'yyyyMMdd')<86400*8,t2.cck_uid,null))/count(distinct t1.cck_uid)*100 as decimal(20,2)) as pay_order_cck_rate7,
  cast(count(distinct if(t2.create_time is not null and
  unix_timestamp(from_unixtime(t2.create_time,'yyyyMMdd'),'yyyyMMdd')-
  unix_timestamp(from_unixtime(t1.create_time,'yyyyMMdd'),'yyyyMMdd')<86400*15,t2.cck_uid,null))/count(distinct t1.cck_uid)*100 as decimal(20,2)) as pay_order_cck_rate14,
  cast(count(distinct if(t2.create_time is not null and
  unix_timestamp(from_unixtime(t2.create_time,'yyyyMMdd'),'yyyyMMdd')-
  unix_timestamp(from_unixtime(t1.create_time,'yyyyMMdd'),'yyyyMMdd')<86400*31,t2.cck_uid,null))/count(distinct t1.cck_uid)*100 as decimal(20,2)) as pay_order_cck_rate30,
  cast(count(distinct t2.cck_uid)/count(distinct t1.cck_uid)*100 as decimal(20,2)) as pay_order_cck_ratelast
from
(
  select
    cck_uid,
    create_time
  from origin_common.cc_ods_fs_wk_cct_layer_info
  where platform =14 and create_time>=1527782400 and create_time<'${gmtdate_ts}'
) t1
left join
(
    select
      distinct cck_uid as cck_uid,
      create_time
    from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
    where ds>=20180601 and create_time<'${gmtdate_ts}'
)t2
on t1.cck_uid=t2.cck_uid
group by from_unixtime(t1.create_time,'yyyyMMdd')
