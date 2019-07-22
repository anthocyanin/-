######大微信客小二后台商品最新推广信息
select
  h2.app_item_id,
  h1.ad_id,h1.ad_name as ad_name,
  (h1.ad_price/100)as ad_price,---券前价
  (h1.cck_rate/1000) as cck_rate,---楚客佣金率
  (h1.cck_price/100) as cck_price,---楚客佣金额
  ((h1.cck_price/100)/(h1.cck_rate/1000)) as discount_price,---券后价
  h1.audit_status as audit_status,---审核状态（0待审核，1审核通过，2驳回）
  h1.status as status---商品状态（0下架，1在架）  
from
(
  select 
    item_id, app_item_id
  from cc_ods_dwxk_fs_wk_items
  where shop_id=19405
)h2
inner join
(
   select
     t1.ad_id as ad_id,
     t1.ad_name as ad_name,
     t1.item_id as item_id,
     t1.ad_price as ad_price,
     t1.cck_rate as cck_rate,
     t1.cck_price as cck_price,
     t1.audit_status as audit_status,
     t1.status as status
   from
  (
   select
     id,ad_id, 
     ad_name, 
     item_id, 
     ad_price, 
     cck_rate, 
     cck_price, 
     audit_status, 
     status
   from cc_ods_fs_dwxk_ad_items_daily
   where app_shop_id=19405
  )t1
inner join
  (
   select
     max(id) as id, 
     item_id
   from cc_ods_fs_dwxk_ad_items_daily
   where app_shop_id=19405
   group by item_id
  )t2
on t1.id=t2.id
)h1
on h1.item_id=h2.item_id