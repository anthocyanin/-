select
  t1.shop_id as shop_id,--店铺id
  t1.shop_title as shop_title,--店铺名称
  t1.shop_cname1 as shop_cname1,--一级类目
  t1.shop_cname2 as shop_cname2,--二级类目
  sum(t2.ipv_7) as shop_ipv_7,--7日店铺维度ipv
  sum(t3.ipv_30) as ipv_30,--30日店铺维度ipv 
  sum(if(t4.product_order_cnt_7>0,1,0)) as sale_product_num,--7日动销商品数     
  sum(t4.product_order_cnt_7) as shop_order_cnt_7,--7日店铺维度订单数
  sum(t4.product_cck_commission_7) as shop_cck_commission_7,--7日店铺维度佣金数
  sum(t4.product_pay_fee_7) as shop_pay_fee_7,--7日店铺维度支付金额
  sum(t5.product_order_cnt_30) as shop_order_cnt_30,--30日店铺维度订单数
  sum(t5.product_pay_fee_30) as shop_pay_fee_30,--30日店铺维度支付金额
  sum(t6.product_eva_rate_cnt_30) as shop_eva_rate_cnt_30,--30日店铺维度评价数
  sum(t6.product_bad_eva_rate_cnt_30) as shop_bad_eva_rate_cnt_30,--30日店铺维度差评价数
  sum(t7.product_refund_cnt_after_delivery) as shop_refund_cnt_after_delivery,--30日店铺维度发货后退款数
  sum(t8.product_delivery_num) as shop_delivery_num,--30日店铺维度发货数量
  sum(t8.product_delivery_duration) as shop_delivery_duration,--30日店铺维度总发货时长
  sum(t9.product_order_num_ship_success) as shop_order_num_ship_success,--30日店铺维度签收订单数
  sum(t9.product_ship_duration) as shop_ship_duration,--30日店铺维度物流总时长
  t10.online_product_num as shop_online_product_num,--7日在线商品数
  t10.new_product_num as shop_new_product_num,--7日上新商品数
  t11.shop_count_huihua as shop_count_huihua,--30日店铺维度会话数
  t11.shop_count_jiedai as shop_count_jiedai,--30日店铺维度接待数
  t11.shop_count_totalwaitetime as shop_count_totalwaitetime--30日店铺维度等待时长即首次回复时长
from
   (select
      product_id,--商品id
      shop_id,--店铺id
      shop_title,--店铺名称
      shop_cname1,--一级类目
      shop_cname2--二级类目
    from data.cc_dw_fs_products_shops
   ) t1
   inner join 
   (select
      s1.product_id as product_id,--商品id
      count(s1.ds) as ipv_7--7日商品维度详情页浏览数
   	from
       (select
          ds,
          product_id--商品id 
        from cc_ods_log_cctui_product_coupon_detail_hourly
        where ds>=20180618 and ds<=20180624 and detail_type='item'
        union all
        select
          ds,
          product_id--商品id 
        from cc_ods_log_gwapp_product_detail_hourly 
        where ds>=20180618 and ds<=20180624
       ) s1
    group by s1.product_id  
   	) t2
    on t1.product_id=t2.product_id
    inner join 
    (select
       s1.product_id as product_id,--商品id
       count(s1.ds) as ipv_30--30日商品维度详情页浏览数
   	 from
        (select
           ds,
           product_id--商品id 
         from cc_ods_log_cctui_product_coupon_detail_hourly
         where ds>=20180524 and ds<=20180624 and detail_type='item'
         union all
         select
           ds,
           product_id--商品id 
         from cc_ods_log_gwapp_product_detail_hourly 
         where ds>=20180524 and ds<=20180624
        ) s1
     group by s1.product_id  
   	) t3
   	on t1.product_id=t3.product_id
   	inner join 
    (select
       product_id,--商品id
       count(distinct third_tradeno) as product_order_cnt_7,--7日商品维度订单数
       sum(cck_commission/100) as product_cck_commission_7,--7日商品维度佣金数
       sum(item_price/100) as product_pay_fee_7--7日商品维度支付金额
     from cc_ods_dwxk_wk_sales_deal_ctime
     where ds>=20180618 and ds<=20180624
     group by product_id
    ) t4
    on t1.product_id=t4.product_id
    inner join 
    (select
           product_id,--商品id
           count(distinct third_tradeno) as product_order_cnt_30,--30日商品维度订单数
           sum(item_price/100) as product_pay_fee_30--30日商品维度支付金额
     from cc_ods_dwxk_wk_sales_deal_ctime
     where ds>=20180524 and ds<=20180624
     group by product_id
    ) t5
    on t1.product_id=t5.product_id
    inner join
    (select
       s1.product_id as product_id,--商品id
       count(s1.star_num) as product_eva_rate_cnt_30,--30日商品维度评价数
       sum(if(s1.star_num=1,1,0)) as product_bad_eva_rate_cnt_30--30日商品维度差评价数
     from
        (select
           order_sn,--订单号
           product_id,--商品id
           star_num--评价分数
         from cc_rate_star
         where ds>=20180524 and ds<=20180624 and rate_id>0 and order_sn!='170213194354LFo017564wk'
        ) s1	
        inner join 
        (select
           distinct third_tradeno as order_sn--订单号
         from cc_ods_dwxk_wk_sales_deal_ctime
         where ds>=20180424 and ds<=20180624
        ) s2
        on s1.order_sn=s2.order_sn
     group by s1.product_id
    ) t6
    on t1.product_id=t6.product_id
    inner join 
    (select
	   s2.product_id as product_id,
	   count(s1.order_sn) as product_refund_cnt_after_delivery--30日商品维度发货后退款数
	   from	
	    (select
	      distinct m1.order_sn as order_sn
         from cc_ods_fs_refund_order m1
	     inner join
		      cc_order_user_delivery_time m2
	     on m1.order_sn=m2.order_sn and m1.create_time>=1527091200 and m1.status=1
	    ) s1--发货后退款订单表
        inner join
	    (select
           product_id,
	       third_tradeno as order_sn
	     from origin_common.cc_ods_dwxk_wk_sales_deal_ctime
	     where ds>=20180424 and ds<=20180624		
	    ) s2
	    on s1.order_sn=s2.order_sn
	   group by s2.product_id
    ) t7
    on t1.product_id=t7.product_id
    inner join 
    (select
       s2.product_id as product_id,
       count(s1.order_sn) as product_delivery_num,--30日商品维度发货数量
       sum(s1.delivery_time-s2.create_time) as product_delivery_duration--30日商品维度发货总时长
     from
        (select
           order_sn,
           delivery_time
         from cc_order_user_delivery_time 
         where ds>=20180524 and ds<=20180624
        ) s1 
        inner join 
        (select
           product_id,
	       third_tradeno as order_sn,
           create_time
         from cc_ods_dwxk_wk_sales_deal_ctime
         where ds>=20180424 and ds<=20180624	
        ) s2 
        on s1.order_sn=s2.order_sn
     group by s2.product_id
    ) t8
    on t1.product_id=t8.product_id
    inner join
    (select  
       s2.product_id as product_id,
       count(s1.order_sn) as product_order_num_ship_success,--30日商品维度签收订单数
       sum(s1.ship_time)  as product_ship_duration--30日商品维度物流总时长
     from
        (select
           order_sn,
           (update_time-create_time) as ship_time--物流时长
        from data.cc_cct_product_ship_info
        where ds>=20180524 and ds<=20180624
        ) s1
        inner join
        (select 
           product_id,
           third_tradeno as order_sn
         from cc_ods_dwxk_wk_sales_deal_ctime
         where ds>=20180424 and ds<=20180624
        ) s2
        on s1.order_sn = s2.order_sn
     group by s2.product_id
    ) t9
    on t1.product_id=t9.product_id
    inner join 
    (select
       app_shop_id as shop_id,
       count(distinct ad_id)  as online_product_num,--7日在线商品数
       sum(if(start_time>='${bizdate_ts}' and start_time<'${gmtdate_ts}',1,0)) as new_product_num--7日上新商品数
     from origin_common.cc_ods_fs_dwxk_ad_items_daily
     where audit_status=1 and status>0 and start_time<'${gmtdate_ts}' and end_time>='${bizdate_ts}'
     group by app_shop_id
    ) t10
    on t1.shop_id=t10.shop_id
    inner join 
    (select
		   shop_id,
		   sum(mantakesessioncount) as shop_count_huihua,--30日店铺维度会话数
		   sum(waiterreplaysessioncount) as shop_count_jiedai,--30日店铺维度接待数
		   sum(totalwaitetime) as shop_count_totalwaitetime--30日店铺维度等待时长即首次回复时长
	   from report.cc_rpt_cctui_im_shop_stat
	   where ds>=20180524 and ds<=20180624
	   group by shop_id
    ) t11
    on t1.shop_id=t11.shop_id
group by t1.shop_id,t1.shop_title,t1.shop_cname1,t1.shop_cname2,t10.online_product_num,t10.new_product_num,
         t11.shop_count_huihua,t11.shop_count_jiedai,t11.shop_count_totalwaitetime





