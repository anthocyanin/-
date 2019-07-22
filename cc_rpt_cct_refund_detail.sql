ALTER TABLE report.cc_rpt_cct_refund_detail
ADD IF NOT EXISTS PARTITION (ds = '${bizdate}')
LOCATION '${bizdate}';

INSERT OVERWRITE TABLE report.cc_rpt_cct_refund_detail
PARTITION (ds = '${bizdate}')

select 
  p1.date,
  service_refund_return_cnt,
  service_refund_nodeliver_cnt,
  service_refund_deliver_cnt,
  system_refund_return_cnt,
  system_refund_nodeliver_cnt,
  system_refund_deliver_cnt,
  user_refund_return_cnt,
  user_refund_nodeliver_cnt,
  user_refund_deliver_cnt,
  merchant_refund_return_cnt,
  merchant_refund_nodeliver_cnt,
  merchant_refund_deliver_cnt,
  refund_return_cnt,
  refund_nodeliver_cnt,
  refund_deliver_cnt,
  avg_refund_return_time,
  avg_refund_nodeliver_time,
  avg_refund_deliver_time
from
(
  select 
    '${bizdate}' as date,
    sum(if(refund_type=1 and (operator_name like '[客服]%' or operator_name like '[楚楚街]%') ,1,0)) as service_refund_return_cnt,
    sum(if(refund_type=2 and (operator_name like '[客服]%' or operator_name like '[楚楚街]%') ,1,0)) as service_refund_nodeliver_cnt,
    sum(if(refund_type=3 and (operator_name like '[客服]%' or operator_name like '[楚楚街]%') ,1,0)) as service_refund_deliver_cnt,
    sum(if(refund_type=1 and operator_name like '楚楚街小二%' ,1,0)) as system_refund_return_cnt,
    sum(if(refund_type=2 and operator_name like '楚楚街小二%' ,1,0)) as system_refund_nodeliver_cnt,
    sum(if(refund_type=3 and operator_name like '楚楚街小二%' ,1,0)) as system_refund_deliver_cnt,
    sum(if(refund_type=1 and title like '用户取消%' ,1,0)) as user_refund_return_cnt,
    sum(if(refund_type=2 and title like '用户取消%' ,1,0)) as user_refund_nodeliver_cnt,
    sum(if(refund_type=3 and title like '用户取消%' ,1,0)) as user_refund_deliver_cnt,
    sum(if(refund_type=1 and operator_name like '[商家]%' ,1,0)) as merchant_refund_return_cnt,
    sum(if(refund_type=2 and operator_name like '[商家]%' ,1,0)) as merchant_refund_nodeliver_cnt,
    sum(if(refund_type=3 and operator_name like '[商家]%' ,1,0)) as merchant_refund_deliver_cnt
  from
  (
    select 
      t1.order_sn,refund_sn,
      case 
      when is_without_shipping=0 then 1
      when is_deliv is null then 2
      else 3 end as refund_type
    from
    ( 
      select
        s1.order_sn               as order_sn,
        s1.refund_sn              as refund_sn,
        s1.is_without_shipping    as is_without_shipping,
        s1.create_time            as create_time
      from
      (
        select 
          order_sn,refund_sn,is_without_shipping,create_time
        from 
          origin_common.cc_ods_fs_refund_order 
        where (status=1 and step=8) or (status=2 and step=7) or (status=2 and step=11)
      ) s1
      inner join
      (
        select
          order_sn
        from origin_common.cc_order_user_pay_time
        where 
          ds >= '${bizdate-60}' and ds<= '${bizdate}' and source_channel=2
      ) s2
      on s1.order_sn=s2.order_sn  
    )t1
    left join
    ( 
      select 
        order_sn,1 as is_deliv
      from  
        origin_common.cc_order_user_delivery_time
      where 
        ds >= '${bizdate-60}' and ds<= '${bizdate}'
     )t2
     on t1.order_sn=t2.order_sn
  )a1
  join
  (
    select
      f.refund_sn           as refund_sn,
      f.operator_name       as operator_name,
      f.title               as title,
      f.create_time         as create_time,
      f.num                 as num
    from
    (
      select 
        t1.refund_sn           as refund_sn, 
        t1.operator_name       as operator_name, 
        t1.title               as title,
        t1.create_time         as create_time,
        ROW_NUMBER() OVER (PARTITION BY t1.refund_sn ORDER BY t1.create_time DESC) as num
      from 
      (
        select      
          s1.refund_sn       as refund_sn, 
          s1.operator_name   as operator_name, 
          s1.title           as title,
          s1.create_time     as create_time
        from
        (
          select  
            refund_sn, operator_name, title,create_time
          from  
            origin_common.cc_refund_log_ctime
          where ds = '${bizdate}'
        ) s1
        inner join
        (
          select
            refund_sn,
            order_sn
          from 
            origin_common.cc_ods_fs_refund_order
        ) s2
        on s1.refund_sn=s2.refund_sn
        inner join
        (
          select
            order_sn
          from origin_common.cc_order_user_pay_time
          where ds >= '${bizdate-60}' and ds<= '${bizdate}' and source_channel=2
        ) s3
        on s2.order_sn=s3.order_sn
      )t1
    ) f
    where f.num =1
  )a2
  on a1.refund_sn=a2.refund_sn
)p1
join
(
  select 
    '${bizdate}' as date,
    sum(if(is_without_shipping=1 and is_deliv=1,1,0)) as refund_return_cnt,
    sum(if(is_without_shipping=1 and is_deliv is null,1,0)) as refund_nodeliver_cnt,
    sum(if(is_without_shipping=0 ,1,0)) as refund_deliver_cnt
  from
  (
    select
      s1.order_sn             as order_sn,
      s1.is_without_shipping  as is_without_shipping,
      s1.create_time          as create_time,
      s1.time_lv1             as time_lv1
    from
    (
      select
        order_sn, 
        is_without_shipping,
        create_time, 
        time_lv1
      from
        origin_common.cc_ods_fs_refund_order
      where
        from_unixtime(create_time,'yyyyMMdd') = '${bizdate}'
    ) s1
    inner join
    (
      select
        order_sn
      from 
        origin_common.cc_order_user_pay_time
      where 
        ds >= '${bizdate-60}' and ds<= '${bizdate}' and source_channel=2
    ) s2
    on s1.order_sn=s2.order_sn
  )t1
  left join
  (
    select
      order_sn,1 as is_deliv
    from
      origin_common.cc_order_user_delivery_time
    where
      ds >= '${bizdate-60}' and ds<= '${bizdate}'
  )t2
  on t1.order_sn=t2.order_sn
)p2
on p1.date=p2.date
join
(
  select 
    '${bizdate}' as date,
    sum(if(is_without_shipping=1 and is_deliv=1 and time_lv1>0,time_lv1-create_time,0))/sum(if(is_without_shipping=1 and is_deliv=1 and time_lv1>0,1,0)) as avg_refund_deliver_time,
    sum(if(is_without_shipping=0 and time_lv1>0 ,time_lv1-create_time,0))/sum(if(is_without_shipping=0 and time_lv1>0 ,1,0)) as avg_refund_return_time
  from
  (
    select
      s1.order_sn             as order_sn,
      s1.is_without_shipping  as is_without_shipping,
      s1.create_time          as create_time,
      s1.time_lv1             as time_lv1
    from
    (
      select
        order_sn,
        is_without_shipping,
        create_time,
        time_lv1
      from
        origin_common.cc_ods_fs_refund_order
      where
        from_unixtime(time_lv1,'yyyyMMdd') = '${bizdate}'
    ) s1
    inner join
    (
      select
        order_sn
      from origin_common.cc_order_user_pay_time
      where ds >= '${bizdate-60}' and ds<= '${bizdate}' and source_channel=2
    ) s2
    on s1.order_sn=s2.order_sn
  )t1
  left join
  (
    select
      order_sn,1 as is_deliv
    from
      origin_common.cc_order_user_delivery_time
    where 
      ds >= '${bizdate-60}' and ds<= '${bizdate}'
  )t2
  on t1.order_sn=t2.order_sn
)p3
on p1.date=p3.date
join
(
  select 
    '${bizdate}' as date,
    sum(if(is_without_shipping=1 and is_deliv is null and time_lv2>0,time_lv2-create_time,0))/sum(if(is_without_shipping=1 and is_deliv is null and time_lv2>0,1,0)) as avg_refund_nodeliver_time
  from
  (
    select
      s1.order_sn             as order_sn,
      s1.is_without_shipping  as is_without_shipping,
      s1.create_time          as create_time,
      s1.time_lv2             as time_lv2
    from
    (
      select
        order_sn,
        is_without_shipping,
        create_time,
        time_lv2
      from
        origin_common.cc_ods_fs_refund_order
      where
        from_unixtime(time_lv2,'yyyyMMdd') = '${bizdate}'
    ) s1
    inner join
    (
      select
        order_sn
      from origin_common.cc_order_user_pay_time
      where ds >= '${bizdate-60}' and ds<= '${bizdate}' and source_channel=2
    ) s2
    on s1.order_sn=s2.order_sn
  )t1
  left join
  (
    select
      order_sn,1 as is_deliv
    from
      origin_common.cc_order_user_delivery_time
    where 
      ds >= '${bizdate-60}' and ds<= '${bizdate}'
  )t2
  on t1.order_sn=t2.order_sn
)p4
on p1.date=p4.date



