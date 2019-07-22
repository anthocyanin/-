select
  t1.shop_id as shop_id,
  t1.shop_title as shop_title,
  t1.shop_c1 as shop_c1,
  t1.shop_c2 as shop_c2,
  t2.shop_ipv_7 as ipv_7,--7日店铺ipv
  t3.shop_ipv_30 as ipv_30,--30日店铺ipv      
  t4.shop_order_cnt_7 as shop_order_cnt_7,--7日店铺订单数
  t4.shop_cck_commission_7 as shop_cck_commission_7,--7日店铺佣金数
  t4.shop_pay_fee_7 as shop_pay_fee_7,--7日店铺支付金额
  t5.shop_order_cnt_30 as shop_order_cnt_30,--30日店铺订单数
  t5.shop_pay_fee_30 as shop_pay_fee_30,--30日店铺支付金额
  t6.shop_eva_rate_cnt_30 as shop_eva_rate_cnt_30,--30日店铺评价数
  t6.shop_bad_eva_rate_cnt_30 as shop_bad_eva_rate_cnt_30,--30日店铺差评价数
from
   ( select
       shop_id,
       shop_title,
       product_id,
       shop_c1,
       shop_c2
     from data.cc_dw_fs_products_shops
     where shop_id not in (18164,18335,17801,18628,18635,19141,19268,17791,18455,18470) 
   ) t1
   left join 
   ( select
       n1.shop_id,
       count(n2.ipv_7) as shop_ipv_7
  	 from 
   	    (select
           product_id,
           shop_id
         from data.cc_dw_fs_products_shops
         where shop_id not in (18164,18335,17801,18628,18635,19141,19268,17791,18455,18470) 
        ) n1
        left join 
   	    (select
           s1.product_id as product_id,
           count(s1.ds) as ipv_7
   	     from
            (select
               ds,
               product_id 
             from cc_ods_log_cctui_product_coupon_detail_hourly
             where ds>='${bizdate-7}' and ds<='${bizdate}' and detail_type='item'
             union all
             select
               ds,
               product_id 
             from cc_ods_log_gwapp_product_detail_hourly 
             where ds>='${bizdate-7}' and ds<='${bizdate}'
            ) s1
          group by s1.product_id  
        ) n2
        on n1.product_id=n2.product_id
        group by n1.shop_id
   	) t2
    on t1.shop_id-t2.shop_id
    left join 
    ( select
       n1.shop_id,
       count(n2.ipv_30) as shop_ipv_30
  	 from 
   	    (select
           product_id,
           shop_id
         from data.cc_dw_fs_products_shops
         where shop_id not in (18164,18335,17801,18628,18635,19141,19268,17791,18455,18470) 
        ) n1
        left join 
   	    (select
           s1.product_id as product_id,
           count(s1.ds) as ipv_30
   	     from
            (select
               ds,
               product_id 
             from cc_ods_log_cctui_product_coupon_detail_hourly
             where ds>='${bizdate-29}' and ds<='${bizdate}' and detail_type='item'
             union all
             select
               ds,
               product_id 
             from cc_ods_log_gwapp_product_detail_hourly 
             where ds>='${bizdate-29}' and ds<='${bizdate}'
            ) s1
          group by s1.product_id  
        ) n2
        on n1.product_id=n2.product_id
        group by n1.shop_id
   	) t3
   	on t1.shop_id-t3.shop_id
   	left join 
    (select
       n1.shop_id,
       sum(n2.product_order_cnt_7) as shop_order_cnt_7,--7日店铺订单数
       sum(n2.product_cck_commission_7) as shop_cck_commission_7,--7日店铺佣金数
       sum(n2.product_pay_fee_7) as shop_pay_fee_7--7日店铺支付金额
     from
        (select
           product_id,
           shop_id
         from data.cc_dw_fs_products_shops
         where shop_id not in (18164,18335,17801,18628,18635,19141,19268,17791,18455,18470) 
        ) n1
        left join 
        (select
           product_id,
           count(distinct third_tradeno) as product_order_cnt_7,--7日商品订单数
           sum(cck_commission/100) as product_cck_commission_7,--7日商品佣金数
           sum(item_price/100) as product_pay_fee_7--7日商品支付金额
         from cc_ods_dwxk_wk_sales_deal_ctime
         where ds>='${biztade-7}' and ds<='${bizdate}'
         group by product_id
        ) n2
        on n1.product_id=n2.product_id
        group by n1.shop_id
    ) t4
    on t1.shop_id=t4.shop_id
    left join 
    (select
       n1.shop_id,
       sum(n2.product_order_cnt_30) as shop_order_cnt_30,--30日店铺订单数
       sum(n2.product_pay_fee_30) as shop_pay_fee_30--30日店铺支付金额
     from
        (select
           product_id,
           shop_id
         from data.cc_dw_fs_products_shops
         where shop_id not in (18164,18335,17801,18628,18635,19141,19268,17791,18455,18470) 
        ) n1
        left join 
        (select
           product_id,
           count(distinct third_tradeno) as product_order_cnt_30,--30日商品订单数
           sum(item_price/100) as product_pay_fee_30--30日商品支付金额
         from cc_ods_dwxk_wk_sales_deal_ctime
         where ds>='${biztade-29}' and ds<='${bizdate}'
         group by product_id
        ) n2
        on n1.product_id=n2.product_id
        group by n1.shop_id
    ) t5
    on t1.shop_id=t5.shop_id
    left join
   （select
       n1.shop_id as shop_id,
       sum(n2.eva_rate_cnt_30) as shop_eva_rate_cnt_30,--30日店铺评价数
       sum(n2.bad_eva_rate_cnt_30) as shop_bad_eva_rate_cnt_30--30日店铺差评价数
     from
        (select
           product_id,
           shop_id
         from data.cc_dw_fs_products_shops
         where shop_id not in (18164,18335,17801,18628,18635,19141,19268,17791,18455,18470) 
        ) n1
        left join 
        (select
           s1.product_id as product_id,
           count(s1.star_num) as eva_rate_cnt_30,--30日商品评价数
           sum(if(s1.star_num=1,1,0)) as bad_eva_rate_cnt_30--30日商品差评价数
         from
            (select
               order_sn,
               product_id,
               star_num
             from cc_rate_star
             where ds>='${bizdate-29}' and ds<='${bizdate}' and rate_id>0 and order_sn!='170213194354LFo017564wk'
            ) s1	
            inner join 
            (select
               distinct third_tradeno as order_sn
             from cc_ods_dwxk_wk_sales_deal_ctime
             where ds>='${bizdate-60}' and ds<='${bizdate}'
            ) s2
            on s1.order_sn=s2.order_sn
            group by s1.product_id
        ) n2
        on n1.product_id=n2.product_id
        group by n1.shop_id
    ）t6
    on t1.shop_id=t6.shop_id




