五月份用户跟踪
select
  t1.cct_uid   as cct_uid,
  t1.cck_uid   as cck_uid,
  t1.real_name as real_name,
  t1.phone     as phone,
  t1.ctime     ctime,
  t1.create_time as vip_time,
  coalesce(t2.pre_pay_cnt,0) as pre_pay_cnt,
  coalesce(t2.pre_gmv,0) as pre_gmv,
  coalesce(t3.inner_pay_cnt,0) as inner_pay_cnt,
  coalesce(t3.inner_gmv,0) as inner_gmv,
  coalesce(t4.out_pay_cnt,0) as out_pay_cnt,
  coalesce(t4.out_gmv,0) as out_gmv,
  coalesce(t5.invite_cnt,0) as invite_cnt
from
(
  select
    s1.cct_uid  as cct_uid,
    s1.cck_uid  as cck_uid,
    s1.ctime    as ctime,
    if(s3.create_time is null,0,from_unixtime(s3.create_time,'yyyyMMdd HH:mm:ss')) as create_time,
    if(s4.real_name is null,0,s4.real_name)     as real_name,
    if(s4.phone is null,0,s4.phone)             as phone
  from
  (
    select
      cct_uid, 
      cck_uid,
      replace(ctime,'.0','') as ctime
    from cc_ods_fs_tui_relation
    where ctime>='2018-05-01 00:00:00.0' and ctime<'2018-06-01 00:00:00.0'
  ) s1
  inner join
  (
    select
        distinct cct_uid as cct_uid
    from origin_common.cc_ods_log_gwapp_pv_hourly  
    where ds>=20180501 and ds<20180601 and module = 'https://app-h5.daweixinke.com/chuchutui/index.html'
  ) s2
  on s1.cct_uid=s2.cct_uid
  left join
  (
    select
      cck_uid,
      create_time
    from cc_ods_fs_wk_cct_layer_info
    where platform=14 and create_time>=1525104000
  ) s3
  on s1.cck_uid=s3.cck_uid
  left join
  (
    select
      cck_uid,
      real_name,
      phone
    from cc_ods_dwxk_fs_wk_business_info
    where ds=20180612
  ) s4
  on s1.cck_uid=s4.cck_uid
) t1
left join
(
  select
    s1.cct_uid        as cct_uid,
    count(distinct s1.order_sn) as pre_pay_cnt,
    cast(sum(s1.item_price)/100 as decimal(20,2)) as pre_gmv
  from
  (
    select
      h1.uid as cct_uid,
      h1.third_tradeno as order_sn,
      h1.create_time as create_time,
      h1.item_price  as item_price
    from
    (
      select
        cck_uid,
        uid,
        third_tradeno,
        create_time,
        item_price
      from cc_ods_dwxk_wk_sales_deal_ctime
      where ds>=20180501
    ) h1
    inner join
    (
      select
        cck_uid
      from cc_ods_fs_wk_cct_layer_info
      where platform=14 and create_time>=1525104000
    ) h2
    on h1.cck_uid=h2.cck_uid
  ) s1
  inner join
  (
    select
      h1.cct_uid as cct_uid,
      h2.create_time as create_time
    from
    (
      select
        cct_uid, 
        cck_uid
      from cc_ods_fs_tui_relation
      where ctime>='2018-05-01 00:00:00.0' and ctime<'2018-06-01 00:00:00.0'
    ) h1
    left join
    (
      select
        cck_uid,
        create_time
      from cc_ods_fs_wk_cct_layer_info
      where platform=14 and create_time>=1525104000
    ) h2
    on h1.cck_uid=h2.cck_uid
  ) s2
  on s1.cct_uid=s2.cct_uid
  where s2.create_time is null or s1.create_time<s2.create_time
  group by s1.cct_uid
) t2
on t1.cct_uid=t2.cct_uid
left join
(
  select
    s1.cck_uid        as cck_uid,
    count(distinct s1.order_sn) as inner_pay_cnt,
    cast(sum(s1.item_price)/100 as decimal(20,2)) as inner_gmv
  from
  (
    select
      h1.third_tradeno as order_sn,
      h1.cck_uid       as cck_uid,
      h1.item_price    as item_price
    from
    (
      select
        third_tradeno,
        cck_uid,
        item_price
      from cc_ods_dwxk_wk_sales_deal_ctime
      where ds>=20180501
    ) h1
    inner join
    (
      select
        cck_uid
      from cc_ods_fs_wk_cct_layer_info
      where platform=14 and create_time>=1525104000
    ) h2
    on h1.cck_uid=h2.cck_uid
  ) s1
  inner join
  (
     select
       distinct order_sn as order_sn
     from cc_ods_log_gwapp_order_track_hourly
     where ds>=20180428 and source='cctui'
  ) s2
  on s1.order_sn=s2.order_sn
  group by s1.cck_uid
) t3
on t1.cck_uid=t3.cck_uid
left join
(
   select
    s1.cck_uid        as cck_uid,
    count(distinct s1.order_sn) as out_pay_cnt,
    cast(sum(s1.item_price)/100 as decimal(20,2)) as out_gmv
  from
  (
    select
      h1.third_tradeno as order_sn,
      h1.cck_uid  as cck_uid,
      h1.item_price as item_price
    from
    (
      select
        third_tradeno,
        cck_uid,
        item_price
      from cc_ods_dwxk_wk_sales_deal_ctime
      where ds>=20180501
    ) h1
    inner join
    (
      select
        cck_uid
      from cc_ods_fs_wk_cct_layer_info
      where platform=14 and create_time>=1525104000
    ) h2
    on h1.cck_uid=h2.cck_uid
  ) s1
  inner join
  (
     select
       distinct order_sn as order_sn
     from cc_ods_log_gwapp_order_track_hourly
     where ds>=20180428 and source!='cctui'
  ) s2
  on s1.order_sn=s2.order_sn
  group by s1.cck_uid
) t4
on t1.cck_uid=t4.cck_uid
left join
(
  select
    invite_uid,
    count(cck_uid) as invite_cnt
  from cc_ods_fs_wk_cct_layer_info
  where platform=14 and create_time>=1525104000 and invite_uid>0
  group by invite_uid
) t5
on t1.cck_uid=t5.invite_uid
///////////////////////////////////////////////////////////////////////////////
select
  t1.product_id as product_id,
  t1.sale_fee as sale_fee,
  t2.product_cname1 as product_cname1,
  t2.product_cname2 as product_cname2,
  t2.product_cname3 as product_cname3,
  t2.shop_id as shop_id,
  t2.shop_title,
  t2.product_title
from 
(
 select
   s1.product_id as product_id,
   sum(s1.product_price*s1.order_cnt) as sale_fee,
   count(distinct s1.order_sn) as order_cnt
 from 
  (select
     product_id,
     order_sn,
     product_price
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
group by
  s1.product_id
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