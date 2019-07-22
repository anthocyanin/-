练习
select
    t1.leader_uid,
    t2.real_name,
    t2.phone,
  count(t1.cck_uid) as gg
from cc_ods_fs_wk_cct_layer_info t1 
left join cc_ods_dwxk_fs_wk_business_info t2
on t1.leader_uid = t2.cck_uid
where t1.platform=14 and t1.create_time>=1527782400 and t2.ds=20180613
group by t1.leader_uid

///////////////////////////////////////////////////////////////////////////
select
    t1.leader_uid,
    t2.real_name,
    t2.phone,
    t1.hh
from 
(  
    select
        leader_uid, 
        count(cck_uid) as hh
    from cc_ods_fs_wk_cct_layer_info where platform=14 and create_time>=1527782400 and leader_uid=invite_uid
    group by leader_uid
) t1
left join
( 
    select
        cck_uid,
        real_name,
        phone
    from cc_ods_dwxk_fs_wk_business_info 
    where ds=20180613 
) t2
on t1.leader_uid = t2.cck_uid

//////////////////////////////////////////////////////////////////////////////
select
    t1.gm_uid, --总经理id 
    max(t3.real_name), --楚客姓名
    max(t3.phone), --楚客手机号
    sum(t2.pay_cnt), --销售单数
    sum(t2.total_fee), --销售额
    sum(t2.cck_commission) --预估佣金
from
(
    select 
        gm_uid,
        cck_uid --楚客id 
    from cc_ods_fs_wk_cct_layer_info
    where platform=18
    union all
    select 
        gm_uid,
        gm_uid as cck_uid --楚客id  
    from cc_ods_fs_wk_cct_layer_info
    where platform=18
) t1
inner join 
( 
    select 
        cck_uid, --楚客id
        count(distinct third_tradeno) as pay_cnt, --销售单数
        sum(item_price) as total_fee, --销售额
        sum(cck_commission/100) as cck_commission --预估佣金
    from cc_ods_dwxk_wk_sales_deal_ctime
    where ds=20180613 
    group by cck_uid
) t2
on t1.cck_uid = t2.cck_uid
inner join 
( 
    select
        cck_uid,
        real_name,
        phone
    from cc_ods_dwxk_fs_wk_business_info 
    where ds=20180613 
) t3
on t1.gm_uid=t3.cck_uid
group by t1.gm_uid
//////////////////////////////////////////////////////////////////////////////
select
    t1.invite_uid as cck_uid,
    t2.type       as type,
    t3.real_name  as real_name,
    t3.phone      as phone,
    t1.invite_cnt as invite_cnt -- 邀请人数
from
(
    select
        invite_uid,
        count(cck_uid) as invite_cnt
    from cc_ods_fs_wk_cct_layer_info
    where platform=18 and create_time>=1528992000 and create_time<1529337600
    group by invite_uid
) t1
left join
(
    select
        cck_uid,
        if(type=0,'VIP',if(type=1,'总监','总经理')) as type
    from cc_ods_fs_wk_cct_layer_info
    where platform=18
) t2
on t1.invite_uid=t2.cck_uid
left join
(
    select
        cck_uid,
        real_name,
        phone
    from cc_ods_dwxk_fs_wk_business_info
    where ds=20180619
) t3
on t1.invite_uid=t3.cck_uid
//////////////////////////////////////////////////////////////////////////
select
    from_unixtime(t1.create_time,'yyyyMMdd') as pay_date,
    count(distinct t1.cck_uid) as pay_cck_cnt,
    count(distinct t2.cck_uid)/count(distinct t1.cck_uid) as cur_pay_order_cck_rate
from
(   
    select
        cck_uid,
        create_time
    from cc_ods_fs_wk_cct_layer_info
    where create_time>=1527868800 and create_time<1527868800+86400*8
) t1
left join
(
    select
        cck_uid,
        create_time
    from cc_ods_dwxk_wk_sales_deal_ctime
    where ds>=20180602 and create_time>=1527868800 and create_time<1527868800+86400*30
)t2
on t1.cck_uid=t2.cck_uid 
where t2.create_time is null or (t2.create_time is not null and unix_timestamp(from_unixtime(t2.create_time,'yyyyMMdd'),'yyyyMMdd')-unix_timestamp(from_unixtime(t1.create_time,'yyyyMMdd'),'yyyyMMdd')<86400*${num})
group by from_unixtime(t1.create_time,'yyyyMMdd')
//////////////////////////////////////////////////////////////////////////
select
    from_unixtime(t1.create_time,'yyyyMMdd') as pay_date,
    count(distinct t1.cck_uid) as pay_cck_cnt,
    concat(cast(count(distinct if(t2.create_time is not null and 
    unix_timestamp(from_unixtime(t2.create_time,'yyyyMMdd'),'yyyyMMdd')-
    unix_timestamp(from_unixtime(t1.create_time,'yyyyMMdd'),'yyyyMMdd')=0,t2.cck_uid,null))/count(distinct t1.cck_uid)*100 as decimal(20,2)),'%'
    ) as cur_pay_order_cck_rate,
    concat(cast(count(distinct if(t2.create_time is not null and 
    unix_timestamp(from_unixtime(t2.create_time,'yyyyMMdd'),'yyyyMMdd')-
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
) t2
on t1.cck_uid=t2.cck_uid
group by from_unixtime(t1.create_time,'yyyyMMdd')
//////////////////////////////////////////////////////////////////////////
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
    where ds>=20180501 and ds<20180601 and module  = 'https://app-h5.daweixinke.com/chuchutui/index.html'
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
//////////////////////////////////////////////////////////////////////////
SELECT 
    concat('\t',a.third_tradeno) as order_sn,
    from_unixtime(a.create_time,'%Y%m%d %H:%i:%s') as pay_time, 
    a.cck_uid as cck_uid,
    b.username as phone,
    c.real_name as real_name
FROM 
    `wk_sales_deal` a
inner join 
    `wk_cck_user` b
on a.cck_uid=b.cck_uid
inner join `wk_business_info` c
on a.cck_uid=c.cck_uid
where a.create_time>=1528441380 and a.create_time<1528513200 and a.status<3 and a.product_id=110018704560 and b.platform=18
//////////////////////////////////////////////////////////////////////////
select
  t1.shop_id as shop_id,
  t5.cn_name as shop_name,
  t1.order_sn as order_sn,
  t1.product_id as product_id,
  t1.product_title as product_title,
  t1.product_count as product_count,
  t1.pay_fee as pay_fee,
  t4.province_name as province_name,
  t4.city_name as city_name,
  from_unixtime(t1.pay_time,'yyyyMMdd HH:mm:ss') as pay_time,
  if(t2.delivery_time is null,0,from_unixtime(t2.delivery_time,'yyyyMMdd HH:mm:ss'))  as delivery_time,
  if(t3.create_time is null,0,from_unixtime(t3.create_time,'yyyyMMdd HH:mm:ss')) as create_time
from
(

  select
    s1.order_sn as order_sn,
    s1.shop_id as shop_id,
    s1.area_id as area_id,
    s1.pay_time  as pay_time,
    s2.product_id as product_id,
    s2.product_title as product_title,
    s2.product_count as product_count,
    s2.product_count*s2.product_discount_price as pay_fee
  from
  (
    select
      order_sn,
      shop_id,
      pay_time,
      area_id
    from cc_order_user_pay_time
    where ds>=20180501 and ds<20180611 and source_channel=2 and shop_id in ()
  ) s1
  inner join
  (
    select
      order_sn, 
      product_id, 
      product_title, 
      product_discount_price,
      product_count
    from cc_order_products_user_pay_time
    where ds>=20180501 and ds<20180611
  ) s2
  on s1.order_sn=s2.order_sn
) t1
left join
(
  select
    order_sn,
    delivery_time
  from cc_order_user_delivery_time 
  where ds>=20180501 and shop_id in ()
) t2
on t1.order_sn=t2.order_sn
left join
(
  select
    order_sn,
    create_time
  from cc_ods_fs_refund_order
  where shop_id in ()
) t3
on t1.order_sn=t3.order_sn
left join
(
  select
    area_id, 
    city_name, 
    province_name
  from cc_area_city_province 
) t4
on t1.area_id=t4.area_id
left join
(
  select
    id as shop_id,
    cn_name
  from cc_ods_fs_shop 
) t5
on t1.shop_id=t5.shop_id
//////////////////////////////////////////////////////////////////////////留存率
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
grou
////////////////////////////////////////////////////////////////////////////////////////////
SELECT 
* 
FROM `cc_shop_white_list` 
where swl=1
////////////////////////////////////////////////////////////////////////////////////////////
select
      t1.product_id as product_id,
      t3.product_title as product_title,
      t2.product_cname1 as product_cname1,
      t2.product_cname2 as product_cname2,
      t2.product_cname3 as product_cname3,
      t3.shop_id as shop_id,
      t3.shop_title as shop_title,
      count(distinct t1.order_sn) as pay_cnt,
      sum(t1.pay_fee) as as pay_fee,
      sum(t4.ipv_uv) as ipv_uv
    from 
    (
      select
        order_sn,
        product_id,
        product_price*product_count           as total_fee, 
        product_discount_price*product_count  as pay_fee
      from cc_order_products_user_pay_time
      where ds>=20180501 and ds<=20180610
    ) t1
    inner join
    (
      select
        order_sn
      from cc_order_user_pay_time
      where ds>=20180501 and ds<=20180610 and source_channel=1
    ) t2  
    on t1.order_sn=t2.order_sn
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
      from cc_dw_fs_products_shops
      where product_c1=-5
    ) t3
    on t1.product_id=t3.product_id
    left join
    (
      select
        product_id, 
        pv as ipv, 
        uv as ipv_uv
      from cc_dm_count_shop_product_source 
      where ds>=20180501 and ds<=20180610
    ) t4
    on t1.product_id=t4.product_id
    group by  t1.product_id,t3.product_title,t2.product_cname1,t2.product_cname2,t2.product_cname3,t3.shop_id,t3.shop_title

SELECT
product_id,
ipv,
ipv_uv
FROM
data.cc_dm_product_flow_stat where ds>=20180501 and ds<=20180610
把这个替换掉select
    product_id, 
    pv as ipv, 
    uv as ipv_uv
  from cc_dm_count_shop_product_source 
  where ds>=20180501 and ds<=20180610
//////////////////////////////////////////////////////////////////////////
楚楚街订单
cc_order_products_user_pay_time 
where (source_channel&1=0 and source_channel&2=0 and source_channel&4=0)

//////////////////////////////////////////////////////////////////////////
需求二：6月8日的下午3点03分 到6月9号上午的10点
订单号，支付时间，推广人ID，推广人手机号，推广人姓名
select 
  t1.third_tradeno as order_sn, 
  from_unixtime(t1.create_time,'yyyyMMdd HH:mm:ss') as pay_time,
  t1.cck_uid as cck_uid,
  t2.real_name as real_name,
  t2.phone as phone
 from
(
  select 
    cck_uid,
    third_tradeno,
    create_time
  from cc_ods_dwxk_wk_sales_deal_ctime
  where ds>=20180608 and ds<20180610 and create_time>=1528398180 and create_time<1528509600
) t1
inner join 
(
  select 
    cck_uid,
    real_name,
    phone
  from cc_ods_dwxk_fs_wk_business_info
  where ds=20180618
) t2
on t1.cck_uid=t2.cck_uid

////////////////////////////////////////////////////////////////////////////////////

楚楚推近一月销售额

select
  t1.cck_uid as cck_uid,
  COALESCE(t2.pay_fee_30,0) as pay_fee_30
from
(select
cck_uid
from cc_ods_fs_wk_cct_layer_info
where platform=14
) t1
inner join 
(select
  cck_uid,
  sum(item_price/100) as pay_fee_30
from cc_ods_dwxk_wk_sales_deal_ctime
where ds>=20180526 and ds<20180626 
group by cck_uid
) t2
on t1.cck_uid=t2.cck_uid

////////////////////////////////////////////////////////////////////////

  select 

   avg(b.delivery_time- a.pay_time) as avg_delivery_time,
   min(b.delivery_time- a.pay_time) as min_delivery_time,
   max(b.delivery_time- a.pay_time) as max_delivery_time,

  cc_order_user_pay_time a
  left join
  cc_order_user_delivery_time b
  on a.order_sn=b.order_sn

  ////////////////////////////////////////////////////////////////////////////////////////////

白名单监控报表
select 
  t1.shop_id as shop_id,
  t3.cn_name as cn_name,
  count(t1.order_sn) as whitelist_order_cnt,
  count(t2.order_sn) as delivery_order_cnt,
  sum(if(t2.delivery_time is null,0,t2.delivery_time-t1.pay_time)) as avg_delivery_time,
  min(if(t2.delivery_time is null,9800000000,t2.delivery_time-t1.pay_time)) as min_delivery_time,
  max(if(t2.delivery_time is null,0,t2.delivery_time-t1.pay_time)) as max_delivery_time,
  sum(if(t2.delivery_time-t1.pay_time <= 24*3600,1,0)) as delivery_ok_cnt
from
(
  select 
    shop_id,
    order_sn,
    pay_time
  from cc_order_user_pay_time 
  where ds>=20180501 and ds<=20180610 and shop_id IN( 18893,18730,18706,18662,18660,18645,18609,18608,18606,18604,18586,18579,18573,
18572,18569,18557,18542,18532,18467,18441,18440,18367,18355,18335,18283,18262,18253,
18240,18164,18157,17957,17927,17891,17803,17801,17791,17776,17772,17636,17461,16581,
16567,16298,11845,9342,8990,8266,5138,4128,965,182,122) and source_channel = 2
 ) t1
left join
(
  select 
    shop_id,
    order_sn,
    delivery_time
  from cc_order_user_delivery_time 
  where ds>=20180501 and shop_id IN( 18893,18730,18706,18662,18660,18645,18609,18608,18606,18604,18586,18579,18573,
18572,18569,18557,18542,18532,18467,18441,18440,18367,18355,18335,18283,18262,18253,
18240,18164,18157,17957,17927,17891,17803,17801,17791,17776,17772,17636,17461,16581,
16567,16298,11845,9342,8990,8266,5138,4128,965,182,122) 
) t2
on t1.order_sn=t2.order_sn
inner join 
( 
  select 
	id,
	cn_name
  from cc_ods_fs_shop
) t3
on t1.shop_id=t3.id
group by t1.shop_id,t3.cn_name


//////////////////////////////////////////////////////////////////////////////////
白名单订单明细
select
  t1.shop_id as shop_id,
  t5.cn_name as shop_name,
  t1.order_sn as order_sn,
  t1.product_id as product_id,
  t1.product_title as product_title,
  t1.product_count as product_count,
  t1.pay_fee as pay_fee,
  t4.province_name as province_name,
  t4.city_name as city_name,
  from_unixtime(t1.pay_time,'yyyyMMdd HH:mm:ss') as pay_time,
  if(t2.delivery_time is null,0,from_unixtime(t2.delivery_time,'yyyyMMdd HH:mm:ss'))  as delivery_time,
  if(t3.create_time is null,0,from_unixtime(t3.create_time,'yyyyMMdd HH:mm:ss')) as create_time
from
(
  select
    s1.order_sn as order_sn,
    s1.shop_id as shop_id,
    s1.area_id as area_id,
    s1.pay_time  as pay_time,
    s2.product_id as product_id,
    s2.product_title as product_title,
    s2.product_count as product_count,
    s2.product_count*s2.product_discount_price as pay_fee
  from
  (
    select
      order_sn,
      shop_id,
      pay_time,
      area_id
    from cc_order_user_pay_time
    where ds>=20180501 and ds<20180611 and source_channel=2 and shop_id in (18893,18730,18706,18662,18660,18645,18609,18608,18606,18604,18586,18579,18573,
          18572,18569,18557,18542,18532,18467,18441,18440,18367,18355,18335,18283,18262,18253,
          18240,18164,18157,17957,17927,17891,17803,17801,17791,17776,17772,17636,17461,16581,
          16567,16298,11845,9342,8990,8266,5138,4128,965,182,122) 
  ) s1
  inner join
  (
    select
      order_sn, 
      product_id, 
      product_title, 
      product_discount_price,
      product_count
    from cc_order_products_user_pay_time
    where ds>=20180501 and ds<20180611
  ) s2
  on s1.order_sn=s2.order_sn
) t1
left join
(
  select
    order_sn,
    delivery_time
  from cc_order_user_delivery_time 
  where ds>=20180501 and shop_id in (18893,18730,18706,18662,18660,18645,18609,18608,18606,18604,18586,18579,18573,
        18572,18569,18557,18542,18532,18467,18441,18440,18367,18355,18335,18283,18262,18253,
        18240,18164,18157,17957,17927,17891,17803,17801,17791,17776,17772,17636,17461,16581,
        16567,16298,11845,9342,8990,8266,5138,4128,965,182,122)
) t2
on t1.order_sn=t2.order_sn
left join
(
  select
    order_sn,
    create_time
  from cc_ods_fs_refund_order
  where shop_id in (18893,18730,18706,18662,18660,18645,18609,18608,18606,18604,18586,18579,18573,
        18572,18569,18557,18542,18532,18467,18441,18440,18367,18355,18335,18283,18262,18253,
        18240,18164,18157,17957,17927,17891,17803,17801,17791,17776,17772,17636,17461,16581,
        16567,16298,11845,9342,8990,8266,5138,4128,965,182,122)
) t3
on t1.order_sn=t3.order_sn
left join
(
  select
    area_id, 
    city_name, 
    province_name
  from cc_area_city_province 
) t4
on t1.area_id=t4.area_id
left join
(
  select
    id as shop_id,
    cn_name
  from cc_ods_fs_shop 
) t5
on t1.shop_id=t5.shop_id
//////////////////////////////////////////////////////////////////////////
select
  t1.invite_uid,
  t1.zhiyao_num,
  t2.type,
  t3.real_name,
  t3.phone
from 
( select 
    invite_uid,
    count(cck_uid) as zhiyao_num
  from cc_ods_fs_wk_cct_layer_info
  where create_time>=1528992000 and create_time<=1529337600 and platform=18
  group by invite_uid
) t1
left join 
(
  select
    cck_uid,
    if(type=0，'VIP',if(type=1,'总监'，'总经理')) as type
  from cc_ods_fs_wk_cct_layer_info
  where platform=18
) t2
on t1.invite_uid=t2.cck_uid
left join 
( select 
    cck_uid,
    real_name,
    phone
  from cc_ods_dwxk_fs_wk_business_info
  where ds=20180619
) t3
on t1.invite_uid=t3.cck_uid
/////////////////////////////////////////////////////////////////////////////////
select
  t1.invite_uid,
  t1.zhiyao_num,
  t2.real_name,
  t2.phone
from 
( select 
    invite_uid,
    count(cck_uid) as zhiyao_num
  from cc_ods_fs_wk_cct_layer_info
  where create_time>=1528992000 and create_time<=1529337600 and platform=18
  group by invite_uid
) t1
left join 
( select 
    cck_uid,
    real_name,
    phone
  from cc_ods_dwxk_fs_wk_business_info
  where ds=20180619
 ) t2
on t1.invite_uid=t2.cck_uid
//////////////////////////////////////////////////////////////////////////////////

select 
  t1.cck_uid,
  t2.phone_number
from
( select
    cck_uid,
  from cc_ods_dwxk_wk_sales_deal_ctime a
  where a.ds>=20180415 and a.ds<20180615 and a.cck_uid not in (select distinct b.cck_uid from cc_ods_dwxk_wk_sales_deal_realtime where b.ds=20180615 and b.create_time>=1528992000)
) t1
inner join
( 
  select 
    cck_uid,
    phone_number
  from cc_ods_dwxk_fs_wk_cck_user
  where ds=20180614 and platorm=14
) t2
on t1.cck_uid=t2.cck_uid

///////////////////////////////////////////////////////////////////////////////////
select 
  *
  from cc_ods_dwxk_fs_wk_cck_user
  where ds=20180614 and platorm=14

//////////////////////////////////////////////////////////////////////////////////

select
  t1.invite_uid as cck_uid,
  t2.real_name  as real_name,
  t2.phone      as phone,
  t3.cck_type   as cck_type,
  t1.invite_cnt as invite_cnt--拉新人数
from 
(  
  select 
    invite_uid,
    count(cck_uid) as invite_cnt--拉新人数
  from cc_ods_fs_wk_cct_layer_info 
  where platform=18 and create_time>1528387200 and create_time<1528992000
  group by invite_uid
) t1
 inner join
(
  select 
    cck_uid,
    real_name,
    phone
  from cc_ods_dwxk_fs_wk_business_info 
  where ds=20180614
) t2
on t1.invite_uid=t2.cck_uid
left join
(
  select 
     cck_uid,
     if(type=0,'VIP',if(type=1,'总监','总经理')) as cck_type
  from cc_ods_fs_wk_cct_layer_info 
  where platform=18
) t3
on t1.invite_uid=t3.cck_uid

/////////////////////////////////////////////////////////////////////////////////////////

select
  t1.cck_uid as cck_uid,
  t1.third_tradeno as order_sn,
  from_unixtime(t1.create_time,'yyyyMMdd HH:mm:ss') as pay_time,
  t3.real_name as real_name,
  t3.phone as phone
from
(
  select 
    cck_uid,
    third_tradeno,
    create_time
  from cc_ods_dwxk_wk_sales_deal_ctime
  where ds>=20180608 and create_time>1528398180 and create_time<1528509600
) t1
inner join
(
   select
      cck_uid
   from cc_ods_dwxk_fs_wk_cck_user
   where ds=20180614 and platform=18
) t2
on t1.cck_uid=t2.cck_uid
(
  select
    cck_uid,
    real_name,
    phone
  from cc_ods_dwxk_fs_wk_business_info 
  where ds=20180614
) t3
on t1.cck_uid=t3.cck_uid
//////////////////////////////////////////////////////////////////////////////
select
      t1.product_id as product_id,
      t3.product_title as product_title,
      t2.product_cname1 as product_cname1,
      t2.product_cname2 as product_cname2,
      t2.product_cname3 as product_cname3,
      t3.shop_id as shop_id,
      t3.shop_title as shop_title,
      count(distinct t1.order_sn) as pay_cnt,
      sum(t1.pay_fee) as as pay_fee,
      sum(t4.ipv_uv) as ipv_uv
    from 
    (
      select
        order_sn,
        product_id, 
        product_price*product_count           as total_fee, 
        product_discount_price*product_count  as pay_fee
      from cc_order_products_user_pay_time
      where ds>=20180501 and ds<=20180610 
    ) t1
    inner join
    (
      select 
        order_sn 
      from cc_order_user_pay_time
      where ds>=20180501 and ds<=20180610 and source_channel=1
    ) t2  
    on t1.order_sn=t2.order_sn
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
      from cc_dw_fs_products_shops
      where product_c1=-5
    ) t3
    on t1.product_id=t3.product_id
    left join
    (
      select
        product_id, 
        pv as ipv, 
        uv as ipv_uv
      from cc_dm_count_shop_product_source 
      where ds>=20180501 and ds<=20180610
    ) t4
    on t1.product_id=t4.product_id
    group by  t1.product_id,t3.product_title,t2.product_cname1,t2.product_cname2,t2.product_cname3,t3.shop_id,t3.shop_title

////////////////////////////////////////////////////////////////////
select
    t1.pay_date as pay_date,
    count(distinct t1.cck_uid) as pay_cck_cnt,
    sum(if(from_unixtime(t1.create_time,'yyyyMMdd')=from_unixtime(t2.create_time,'yyyyMMdd'),1,0)) as  cur_pay_order_cck_cnt,
    sum(if(from_unixtime(t1.create_time,'yyyyMMdd')<=from_unixtime(t2.create_time+86400,'yyyyMMdd'),1,0)) as  one_pay_order_cck_cnt,
    sum(if(from_unixtime(t1.create_time,'yyyyMMdd')<=from_unixtime(t2.create_time+86400*2,'yyyyMMdd'),1,0)) as  two_pay_order_cck_cnt,
    sum(if(from_unixtime(t1.create_time,'yyyyMMdd')<=from_unixtime(t2.create_time+86400*3,'yyyyMMdd'),1,0)) as  three_pay_order_cck_cnt,
    sum(if(from_unixtime(t1.create_time,'yyyyMMdd')<=from_unixtime(t2.create_time+86400*4,'yyyyMMdd'),1,0)) as  four_pay_order_cck_cnt,
    sum(if(from_unixtime(t1.create_time,'yyyyMMdd')<=from_unixtime(t2.create_time+86400*5,'yyyyMMdd'),1,0)) as  five_pay_order_cck_cnt
from  
(   
  select
    cck_uid,
    from_unixtime(create_time,'yyyyMMdd') as pay_date,
  from cc_ods_fs_wk_cct_layer_info
  where create_time>= ？ and create_time
) t1
left join
(
 select
 distinct
  ds as pay_date,
  cck_uid,
  create_time
 from cc_ods_dwxk_wk_sales_deal_ctime
 where ds>
)t2
on t1.cck_uid=t2.cck_uid
/////////////////////////////////////////////////////////////////////////////////////////////////
--临时需求 商品ID 名称  单数 佣金 劵后价格  推广人数   推广次数   
 select
   t1.product_id as product_id, 
   t1.product_order_cnt_30 as product_order_cnt_30, --30日商品维度订单数
   t1.product_pay_fee_30 as product_pay_fee_30,--30日商品维度支付金额
   t1.product_cck_commission_30 as product_cck_commission_30,--30日商品维度佣金
   t2.product_title as product_title,--商品名称
   t3.price_after_coupon as price_after_coupon,--券后价格 
   t4.fx_cnt as fx_cnt,--30日商品维度推广次数
   t4.fx_user_cnt as fx_user_cnt--30日商品维度推广人数
from
   (select
      product_id,--商品id
      count(distinct third_tradeno) as product_order_cnt_30,--30日商品维度订单数
      sum(item_price/100) as product_pay_fee_30,--30日商品维度支付金额
      sum(cck_commission/100) as product_cck_commission_30--30日商品维度佣金
    from cc_ods_dwxk_wk_sales_deal_ctime
    where ds>=20180527 and ds<=20180627
    group by product_id
   ) t1
inner join 
   (select
      product_id,--商品id
      product_title--商品名称
    from data.cc_dw_fs_products_shops
    where product_cname1='母婴'
   ) t2
on t1.product_id=t2.product_id 
left join
   (select
      s1.ad_id as product_id,
      s1.ad_price-s2.money as price_after_coupon--券后价格 
    from
       (select
          ad_id,
          ad_price
        from cc_ods_fs_dwxk_ad_items_daily
       ) s1
       inner join 
       (select
          ad_id,
          money
        from cc_ods_dwxk_fs_wk_ad_coupon
       ) s2
       on s1.ad_id=s2.ad_id
    ) t3
on t1.product_id=t3.product_id
left join
   (select
      m3.product_id,
      count(m1.user_id) as fx_cnt,--30日商品维度推广次数
      count(distinct m1.user_id,m1.ds) as fx_user_cnt--30日商品维度推广人数
    from
       (select
          ad_material_id as ad_id,
          user_id,
          ds
        from origin_common.cc_ods_log_cctapp_click_hourly
        where ds>=20180527 and ds<=20180627 and ad_type in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
        union all
        select
          ad_id,
          user_id,
          ds
        from origin_common.cc_ods_log_cctapp_click_hourly
        where ds>=20180527 and ds<=20180627 and ad_type not in ('search','category') and module in ('detail','detail_app') and zone = 'spread'
        union all
        select
          s2.ad_id,
          s1.user_id,
          s1.ds
        from
           (select
              ad_material_id,
              user_id,
              ds
            from origin_common.cc_ods_log_cctapp_click_hourly
            where ds>=20180527 and ds<=20180627 and ad_type in ('single_product','9_cell') and module='vip' and zone in ('material_group-share','material_moments-share')
           ) s1
           inner join
           (select
              distinct ad_material_id as ad_material_id,
              ad_id,
              ds
            from data.cc_dm_gwapp_new_ad_material_relation_hourly
            where ds>=20180527 and ds<=20180627
           ) s2
           on s1.ad_material_id = s2.ad_material_id
        ) m1
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
    ) t4
on t1.product_id=t4.product_id
/////////////////////////////////////////////
取DAU代码
对条件限制进行了评估，加上cct_uid is not null条件对结果没有影响，条件app_partner_id = 14才是最重要的。和亚当数据稍微偏大一点。
select 
    ds,
    hour,
    count(distinct cct_uid)------DAU
from 
    origin_common.cc_ods_log_gwapp_pv_hourly  
where 
    ds>=20180613 
    and 
    ds<=20180618 
    and 
    module='https://app-h5.daweixinke.com/chuchutui/index.html' 
    and 
    cct_uid is not null 
    and 
    app_partner_id = 14
group by ds,hour
/////////////////////////////////////////////////////////////////////////
# 更新首页dau
select
  '{date_sign}' as date_sign,
  case
  when s.cck_vip_status = 0 and s.cck_vip_level = 0 then 0
  when s.cck_vip_status = 0 and s.cck_vip_level = 1 then 2
  else 1 end as source,
  s.dau
from
(
  select
    m1.cck_vip_status,
    m1.cck_vip_level,
    count(distinct m1.cct_uid) as dau
  from
  (
    select
      t1.cct_uid,
      t2.cck_vip_status,
      if(t2.cck_vip_status = 1, -1, t2.cck_vip_level) as cck_vip_level
    from
    (
      select 
        cct_uid 
      from 
        origin_common.cc_ods_log_gwapp_pv_hourly  
      where 
          ds = '{bizdate}' 
        and 
          app_partner_id=14  
        and 
          module  = 'https://app-h5.daweixinke.com/chuchutui/index.html'
    ) t1
    join
    (
      select 
        cct_uid,
        cck_vip_status, 
        cck_vip_level 
      from 
        origin_common.cc_ods_fs_tui_relation 
    ) t2
    on t1.cct_uid = t2.cct_uid
  ) m1
  group by
    m1.cck_vip_status, m1.cck_vip_level
) s

///////////////////////////////////////////////////////////////////////////////
重要
when source='ccj_cct' then 'promotion_buy'--推广
when source='cctui' and coalesce(cck_vip_status,0)=1 then 'vip_inner_buy'--自买
when source='cctui' and coalesce(cck_vip_status,0)=0 then 'no_vip_inner_buy'--自买

///////////////////////////////////////////////////////////////////
翔哥取cck地址
select  
   t1.cck_uid,
   t2.delivery_address,
   t2.delivery_name, 
   t2.delivery_mobilephone
from
(select 
   cck_uid, 
   phone_number
from cc_ods_dwxk_fs_wk_cck_user
where cck_uid in(963010,1103580,1142389,1136350,1060513,1143356,1057167,411766,1142484,793900,385316,1046137,1009200,1045732,1099775,1141173,
	             573181,756393,1096499,1000478,1148018,509530,742820,902858,997051,1137897,896177,492974,736274,732310,1136238,474774,980086)
    and ds=20180703
) t1
left join
(
select 
   s1.order_sn,
   s1.user_id,
   s1.delivery_address,
   s1.delivery_name,
   s1.delivery_mobilephone
 from
   (select 
     order_sn, 
     user_id,
     delivery_address,
     delivery_name,
     delivery_mobilephone
   from cc_order_user_pay_time
   ) s1
   inner join
   (select 
	  order_sn
    from cc_ods_order_gift_products_user_pay_time
   ) s2
   on s1.order_sn=s2.order_sn
) t2
on t1.phone_number=t2.delivery_mobilephone

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
素材页各埋点点击数据
select
    ds,---日期
    zone,---埋点
    count(user_id),---点击次数
    count(distinct user_id)---点击人数
from 
    origin_common.cc_ods_log_cctapp_click_hourly
where 
    ds>='${begin_date}' ---开始时间
and 
    ds<='${end_date}' ---结束时间
and 
    module='detail_material'---素材页
and
    source in ('cct','cctui') ---来源楚楚推app
group by
    ds,
    zone
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
商详页各埋点点击数据
select
    ds,
    zone,
    count(user_id),
    count(distinct user_id)
from 
    origin_common.cc_ods_log_cctapp_click_hourly
where 
    ds>='${begin_date}' 
and 
    ds<='${end_date}'
and 
    module in ('detail','detail_app')
and
    source in ('cct','cctui')
group by
    ds,
    zone
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
商家邮费
select
    s1.shop_id,
    s2.cn_name,
    get_json_object(s1.xinxi,'$.detail.default_fee') as default_fee,
    get_json_object(s1.xinxi,'$.detail.list') as list
from
    tmp.temp_wuwei_json_youfei s1
join
    origin_common.cc_ods_fs_shop s2
on s1.shop_id=s2.id
where
    s2.status=0


where
    delivery_time >= unix_timestamp(20180805,'yyyyMMdd')
或者  
where
    from_unixtime (delivery_time,'yyyyMMdd') >= 20180805
////////////////////////////////////////////////////////////////
爆款SQL
select 
  b.product_id as product_id 
from 
    origin_common.cc_ods_fs_cck_xb_policies_hourly a 
inner join 
    origin_common.cc_ods_fs_cck_ad_material_products_hourly b 
on 
    a.id=b.policies_id 
where 
    a.status!='DELETE'\
and 
    a.zone='productList' 
and 
    a.ad_key='cct-task-bomb-product'\
and 
    a.begin_time < %d 
and 
    a.end_time >= %d " % (to_date_time, cur_date_time)" 

////////////////////////////////////////////////////////////////
此代码查每日爆款的商品
select
*
from
  origin_common.cc_ods_fs_cck_xb_policies_hourly
where
  from_unixtime(begin_time,'yyyyMMdd') >= '${begin_date}'
and
  from_unixtime(begin_time,'yyyyMMdd') <= '${end_date}'
and
  ad_key in ('cct-task-bomb-product','cct-index-tab')

/////////////////////////////////////////////////////////////////
刨除出爆款的sql
select 
    b.product_id as product_id 
from 
    origin_common.cc_ods_fs_cck_xb_policies_hourly a 
inner join 
    origin_common.cc_ods_fs_cck_ad_material_products_hourly b 
on 
    a.id=b.policies_id 
where 
    a.status!='DELETE'
and 
    a.zone='productList' 
and 
    a.ad_key in ('seckill-tab-10','seckill-tab-12','seckill-tab-15','seckill-tab-18','seckill-tab-21','seckill-tab-0')\
and 
    a.begin_time < %d 
and 
    a.end_time >= %d 
and 
    b.product_id not in ()
////////////////////////////////////////////////////////////////
SELECT 
  id,
  concat('\t',remote_area) 
FROM `cc_shop` 
where remote_area!=''

网上看到的：代表的是csv文件里的分隔符，也就是每一列以 \t 符号分隔，每一行以 \n符号分隔
\t 就是TAB键，\n 就是回车了
SELECT 
*
FROM `cc_product_region` 
where product_id in ()


////////////////////////////////////////////////////////////////
取在线商品数，上新商品数
SELECT
    app_shop_id as shop_id,
    count(distinct ad_id)  as online_prd_cnt,
    sum(if(start_time>='${bizdate_ts}' and start_time<'${gmtdate_ts}',1,0)) as new_prd_cnt
FROM origin_common.cc_ods_fs_dwxk_ad_items_daily
WHERE audit_status=1 and status>0 and start_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
GROUP BY app_shop_id
////////////////////////////////////////////////////////////////

SELECT 
product_id,cn_title 
FROM cc_ods_fs_product 
where cn_title like '%苹果%'
////////////////////////////////////////////////////////////

rownum 是新增列 查看现在行的原来行的行数
row_number() over (order by...) as a: 则a这一行是现在每一行的行序号。 这里over(order by ...)的意思是以按照某个字段排序，所以和直接对表的order by效果是一样的 
row_number() over (partition by ... order by...) as a：则a这一行是现在每一行的行序号。这里partition by...是以谁分块，order by... 代表对谁在块内进行排序
以上表明row_number()，就是对数据进行编号
rank() over()和dense_rank() over()来进行编号：rank() over()和dense_rank() over()的区别如图

sum() over()分组求和

first_value() over()求分组第一条

last_value() over()求分组最后一条

其中用row_number() over()取编号第一条的也可以实现first_value() over()的效果
////////////////////////////////////////////////////////////////////////////////////////
pma：
[mysql.hosts.dwxkadsense]
      host = "s5511c.mysql.internal.chuchujie.com"
      port = "5511"
      user = "dwxk_statistic"
      pass = "VbfpEYh71GFASIs"

dwxk_statistic
VbfpEYh71GFASIs


[mysql.hosts.shangcheng_shop1]
      host = "shangcheng-temp-s1.cvdcrhews6fj.rds.cn-north-1.amazonaws.com.cn"
      port = "3306"
      user = "shop_stat1"
      pass = "P2hjqC9eP5PvHGNe"

shop_stat1
P2hjqC9eP5PvHGNe


[mysql.hosts.shop_admin]
     host = "s4035.mysql.internal.chuchujie.com"
     port = "4035"
     user = "data_group"
     pass = "v7iUT8IC90P6fXL"

data_group
v7iUT8IC90P6fXL

[mysql.hosts.inn_cms]
     host = "s4045.mysql.internal.chuchujie.com"
     port = "4045"
     user = "inn_cms"
     pass = "dLa7SnlGACYspt"

[mysql.hosts.co_adsense]
     host = "s4045.mysql.internal.chuchujie.com"
     port = "4045"
     user = "data_group"
     pass = "Ay7dsfqnlkftof"

///////////////////////////////////////////////////////////////////////////
select 
    count(distinct a.thrknow) 
from 
(
    select 
        distinct thrknow 
    from 
        dm_questlabel.fact_quest_label 
    lateral view explode(split(three_know,'\\^\\.\\^')
) c AS thrknow
where 
    questinnersource=4 
    and 
    subject='C01'  
    and 
    fastdfsurl like 'group%'  
    and 
    docfastdfsurl 
    like 
    'group%' 
    and 
    pngfastdfsurl like 'group%'
///////////////////////////////////////////////////////////////////////////
select
    ds,---日期
    zone,---埋点
    count(user_id),---点击次数
    count(distinct user_id)---点击人数
from 
    origin_common.cc_ods_log_cctapp_click_hourly
where 
    ds>='${begin_date}' ---开始时间
    and 
    ds<='${end_date}' ---结束时间
    and 
    module='detail_material'---素材页
    and
    source in ('cct','cctui') ---来源楚楚推app
group by
    ds,
    zone    
///////////////////////////////////////////////////////////////////////////
c.company_name
cc_ods_fs_shop a
cc_ods_fs_business_basic  b
on a.owner_id=b.uid
cc_ods_fs_business_company c 
on b.id=c.basic_id

////////////////////////////////////////////////////////////////////////////////////////////////
cc_ods_op_products_map 可用

split(split(regexp_extract(query,'id=([0-9]*)&', 1),'=')[1],'&')[0] as page_id,
////////////////////////
select 
    user_id,
    count(1) as log_cnt
from 
    origin_common.cc_ods_log_cctapp_click_hourly 
where 
    from_unixtime(timestamp,'yyyyMMdd') >=20181101
and
  from_unixtime(timestamp,'yyyyMMdd') <=20181118
and 
  module ='index'
and
  zone ='tab'
group by
  user_id
////////////////////////////////////////////////////////////////////////////////////////////////////
cc_ods_dwxk_user_train_bill_mtime
////////////////////////////////////////////////////////////
SELECT * FROM cc_white_manger WHERE type =1 and flag = 6
//////////////////////////////
”礼包订单注意一下礼包卷 购买卷时不生成订单
只有用卷兑换商品时才生成订单“ 涛哥原话
但实际上是购买礼包券也产生订单号的，也出现在cc_ods_fs_wk_cck_gifts表里,购买了礼包券也算是VIP了

还有手动创建的楚客 也是有礼包流水 没有订单哈

    手动创建的楚客 他会有pay_sn 一级third_pay_sn 吗？

pay_sn  third_pay_sn 会有，就是代码造出来的 
商品ID大概是 8888888 什么的特俗数据
invite_uidss这个字段并非官方总经理，应是社群同学导入时设定的

而且 订单所对应的金额为 商品与卷的差价（如 卷399 兑换的商品是499 那么用户就得补贴100元 订单表中金额为100元 要是商品是399的 订单表中金额为0）
也可以把买卷的流水金额 和礼包订单金额合并
