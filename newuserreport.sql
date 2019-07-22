use origin_common;

alter table cc_cct_newuserreport
add if not exists partition (ds = '${bizdate}')
location '${bizdate}';

insert overwrite table origin_common.cc_cct_newuserreport
partition (ds = '${bizdate}')

select 
	max(p3.ds) as date,
   --->新增用户数
	count(p1.cct_uid) as newuser,
	count(if(p1.user_type=2,p1.cct_uid,null)) as newuser_c,
	count(if(p1.user_type=1,p1.cct_uid,null)) as newuser_j,
	count(if(p1.user_type=0,p1.cct_uid,null)) as newusaer_v,
	--->登录app的新用户数
	count(p2.cct_uid) as loginapp,
	count(if(p1.user_type=2,p2.cct_uid,null)) as loginapp_c,
	count(if(p1.user_type=1,p2.cct_uid,null)) as loginapp_j,
	count(if(p1.user_type=0,p2.cct_uid,null)) as loginapp_v,
	--->当天有交易的新用户数
	count(p3.cct_uid) as transactions,
	count(if(p1.user_type=2,p3.cct_uid,null)) as transactions_c,
	count(if(p1.user_type=1,p3.cct_uid,null)) as transactions_j,
	count(if(p1.user_type=0,p3.cct_uid,null)) as transactions_v,
   --->新用户客单件
	cast(sum(coalesce(p3.gmv,0))/sum(coalesce(p3.third_tradeno,0)) as decimal(10,2)) as kd,
	cast(sum(if(p1.user_type=2,coalesce(p3.gmv,0),0))/sum(if(p1.user_type=2,coalesce(p3.third_tradeno,0),0)) as decimal(10,2)) as kd_c,
	cast(sum(if(p1.user_type=1,coalesce(p3.gmv,0),0))/sum(if(p1.user_type=1,coalesce(p3.third_tradeno,0),0)) as decimal(10,2)) as kd_j,
	cast(sum(if(p1.user_type=0,coalesce(p3.gmv,0),0))/sum(if(p1.user_type=0,coalesce(p3.third_tradeno,0),0)) as decimal(10,2)) as kd_v,
	--->新用户支付订单数
	sum(coalesce(p3.third_tradeno,0)) as payorder,
	sum(if(p1.user_type=2,coalesce(p3.third_tradeno,0),0)) as payorder_c,
	sum(if(p1.user_type=1,coalesce(p3.third_tradeno,0),0)) as payorder_j,
	sum(if(p1.user_type=0,coalesce(p3.third_tradeno,0),0)) as payorder_v,
	--->领取新人券用户数
	count(p4.cct_uid) as getcoin,
	--->使用新人券支付的用户数
	count(p4.coupon_sn) as usecoin,
	--->新用户gmv
	sum(coalesce(p3.gmv,0)) as gmv,
	sum(if(p1.user_type=2,coalesce(p3.gmv,0),0)) as gmv_c,
	sum(if(p1.user_type=1,coalesce(p3.gmv,0),0)) as gmv_j,
	sum(if(p1.user_type=0,coalesce(p3.gmv,0),0)) as gmv_v
from 
(
    --->t1:新增vip用户-493
    select 
        t1.cck_uid as cck_uid,
        t2.cct_uid as cct_uid,
        0 as user_type
    from
    (
        select 
            date_format(from_unixtime(create_time),'yyyyMMdd') as time,
            cck_uid
        from 
            cc_ods_fs_wk_cct_layer_info
        where 
            platform=14
        and 
        	is_del=0
        and 
        	date_format(from_unixtime(create_time),'yyyyMMdd')='${bizdate}'
    ) t1
    inner join
    (
        select 
            cck_uid,
            cct_uid
        from 
        	cc_ods_dwxk_fs_wk_cck_user
        where 
        	ds='${bizdate}'
        and 
        	cct_uid>0
    ) t2 
    on t1.cck_uid=t2.cck_uid
    union all 
	--->新增积分用户
    select 
        distinct 
        t1.cck_uid as cck_uid,
        t1.cct_uid as cct_uid,
        1 as user_type
    from
    (
        select 
            cck_uid,
            cct_uid,
            guider_uid
        from 
        	cc_ods_fs_tui_relation
        where 
            cck_vip_level=1
        and 
        	cck_vip_status=0
        and 
        	date_format(mtime,'yyyyMMdd')='${bizdate}'
    ) t1
    inner join
    (
        select 
            cck_uid
        from 
        	cc_ods_dwxk_fs_wk_cck_user
        where 
            ds='${bizdate}'
        and 
        	platform=14
    ) t2 
    on t1.guider_uid=t2.cck_uid
    union all 
	--->新增普通用户
    select 
    	distinct 
        t1.cck_uid as cck_uid,
        t1.cct_uid as cct_uid,
        2 as user_type
    from
    (
        select 
            cck_uid,
            cct_uid,
            guider_uid
        from 
        	cc_ods_fs_tui_relation
        where 
            cck_vip_level=0
        and 
        	cck_vip_status=0
        and 
        	date_format(ctime,'yyyyMMdd')='${bizdate}'
    ) t1
    inner join
    (
        select 
            cck_uid
        from 
        	cc_ods_dwxk_fs_wk_cck_user
        where 
            ds='${bizdate}'
        and 
        	platform=14
    ) t2 
    on t1.guider_uid=t2.cck_uid
) p1
left outer join
--->p2:登录app用户数
(
    select
        distinct cct_uid
    from 
    	origin_common.cc_ods_log_gwapp_pv_hourly
    where 
        ds = '${bizdate}'
    and 
        app_partner_id=14
    and 
        module = 'https://app-h5.daweixinke.com/chuchutui/index.html'
) p2 
on p1.cct_uid=p2.cct_uid 
left outer join
--->p3:当天有交易的新用户数-新用户gmv-新用户支付订单数-新用户客单价
(
    select 
        uid as cct_uid,
        max(ds) as ds,
        cast(sum(item_price/100) as decimal(10,2)) as gmv,
        count(distinct third_tradeno) as third_tradeno
    from 
        cc_ods_dwxk_wk_sales_deal_ctime
    where 
        ds='${bizdate}'
    group by 
        uid
) p3
on p1.cct_uid=p3.cct_uid
left outer join
--->p4:	领取新人券用户数-使用新人券用户数
(
    select 
        t1.user_id as cct_uid,
        t2.coupon_sn as coupon_sn
    from
    (
        select 
            user_id,
            coupon_sn,
            status
        from cc_coupon_user
        where ds='${bizdate}'
        and template_id=13980981
    ) t1
    left outer join
    (
        select 
            distinct coupon_sn
        from 
            cc_order_coupon_paytime
        where 
            ds='${bizdate}'
        and 
            template_id=13980981
    ) t2 
    on t1.coupon_sn=t2.coupon_sn
) p4 
on p1.cct_uid=p4.cct_uid

