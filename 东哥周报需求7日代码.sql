select
    p1.shop_id as shop_id,--店铺id
    p1.shop_title as shop_title,--店铺名称
    p1.shop_cname1 as shop_cname1,--一级类目
    (case   
    when p1.shop_id in (19903,20305,20322) then '百诺优品'
    when p1.shop_id = 18470 then '冰冰购'
    when p1.shop_id in (18532,19141,19268,19347,20471) then '代发'
    when p1.shop_id in (18635,18240) then '极便利'
    when p1.shop_id in (17791,18731) then '京东'
    when p1.shop_id in (18704,18636) then '每日优鲜'
    when p1.shop_id in (18730,18723,18542,17636,18482,19089,19667,20203,20314,20343,20065,20548,20738,19517) then '其他'
    when p1.shop_id in (18662,18729,18588,18740,18799,19319,19405,19402,20216,18965,20696) then '生鲜'
    when p1.shop_id in (18706,18586,18569,18262,19392,18606,15426,18314,19534,2873,19708,2369,9872,19871,19756,19755,19709,16851,20179,17691,20242,456,3559,13930,15907,20513,20652,20653,20725,20789) then '小团子'
    when p1.shop_id = 18455 then '严选'
    when p1.shop_id in (18838,19239,19505,19504,19486,19470,19404,19527,19521,19525,19542,19613,19609,19599,19580,19664,19701,19699,19683,19682,19678,19765,19742,19722,19753,20016,19906,19907,20063,20064,20178,20168,20236,20237,20202,20188,4086,20697,20737,19627,20748,18327,12902,11974,12334,15670,15912,14715,5649,16898,15729,2752,12375,4599,13706,15395,12461,19654,16293,4024,20353,17929,104,3037,19170,14948,1793,19207,4999,16137,3885,16671,18791,17210,5987,14956,1341,15499,1555,18381,16194,5107,16133,8670,2254,18253,3803,17773,13698,17576,14832,18565,739,9349,7693,14720,15044,13638,7200,4318,12033,12766,17639,13363,16305,15853,6163,11500,9806,4539,20818,17845,12523,13559,13991,1412,14823,15129,1655,17157,17397,1802,18057,1831,18812,18814,1937,7572,9621,20784) then '一亩田'
    when p1.shop_id in (18335,18164,17801) then '自营'
    when p1.shop_id in (19339,19468,19298,18491,19611,19435,18765,19870,20142,20332,18574,20392,20423,20543,20600,20770) then '总监店铺'
    else 'pop' end) as tab,--店铺类型
    p2.online_product_num as shop_online_product_num,--7日店铺维度在线商品数
    p2.new_product_num as shop_new_product_num,--7日店铺维度上新商品数
    p1.shop_ipv_7 as shop_ipv_7,--7日店铺维度ipv
    p1.shop_pay_fee_7 as shop_pay_fee_7,--7日店铺维度支付金额
    p1.shop_order_cnt_7 as shop_order_cnt_7,--7日店铺维度订单数
    (p1.shop_pay_fee_7/p1.shop_order_cnt_7) as price_per_order_7,--7日客单价
    (p1.shop_order_cnt_7/p1.shop_ipv_7) as order_rate_7,--7日店铺维度转化率
    p1.shop_eva_rate_cnt_7 as shop_eva_rate_cnt_7,--7日店铺维度有效评价数
    p1.shop_bad_eva_rate_cnt_7 as shop_bad_eva_rate_cnt_7,--7日店铺维度差评数
    (p1.shop_bad_eva_rate_cnt_7/p1.shop_eva_rate_cnt_7) as shop_bad_eva_rate,--7日店铺维度差评率
    p1.shop_refund_cnt_after_delivery as shop_refund_cnt_after_delivery,--7日店铺维度发货后退款数
    (p1.shop_refund_cnt_after_delivery/p1.shop_delivery_num) as shop_refund_rate,--7日店铺维度退款率
    p1.shop_delivery_num as shop_delivery_num,--7日店铺维度发货数量
    p1.shop_count_delivery_overtime_24 as shop_count_delivery_overtime_24,--7日店铺维度超时发货数
    p1.shop_count_delivery_overtime_48 as shop_count_delivery_overtime_48,--7日店铺维度超时发货数
    p1.shop_delivery_duration/p1.shop_delivery_num as shop_avg_delivery_duration,--7日店铺维度平均发货时长
    p1.shop_order_num_ship_success as shop_order_num_ship_success,--7日店铺维度签收订单数
    p1.shop_ship_duration/p1.shop_delivery_num as shop_avg_ship_duration,--7日店铺维度平均物流时长
    p3.shop_count_huihua as shop_count_huihua,--7日店铺维度会话数
    p3.shop_count_jiedai as shop_count_jiedai,--7日店铺维度接待数
    p3.shop_count_totalwaitetime as shop_count_totalwaitetime,--7日店铺维度等待时长即首次回复时长
    p4.count_task as shop_count_task,--店铺维度工单数
    p4.count_task_overtime as shop_count_task_overtime,--店铺超时工单数
    (p4.count_task_overtime/p4.count_task) as overtime_task_rate--超时工单率
from
(
    select
        t3.shop_id as shop_id,--店铺id
        t3.shop_title as shop_title,--店铺名称
        t3.shop_cname1 as shop_cname1,--一级类目   
        sum(t2.ipv_7) as shop_ipv_7,--7日店铺维度ipv  
        sum(t1.product_order_cnt_7) as shop_order_cnt_7,--7日店铺维度订单数
        sum(t1.product_pay_fee_7) as shop_pay_fee_7,--7日店铺维度支付金额
        sum(t4.product_eva_rate_cnt_7) as shop_eva_rate_cnt_7,--7日店铺维度评价数
        sum(t4.product_bad_eva_rate_cnt_7) as shop_bad_eva_rate_cnt_7,--7日店铺维度差评价数
        sum(t5.product_refund_cnt_after_delivery) as shop_refund_cnt_after_delivery,--7日店铺维度发货后退款数
        sum(t6.product_delivery_num) as shop_delivery_num,--7日店铺维度发货数量
        sum(t6.product_count_delivery_overtime) as shop_count_delivery_overtime_24,--7日店铺维度超时24发货数
        sum(t7.product_count_delivery_overtime) as shop_count_delivery_overtime_48,--7日店铺维度超时48发货数
        sum(t6.product_delivery_duration) as shop_delivery_duration,--7日店铺维度发货总时长
        sum(t8.product_order_num_ship_success) as shop_order_num_ship_success,--7日店铺维度签收订单数
        sum(t8.product_ship_duration) as shop_ship_duration--7日店铺维度物流总时长
    from
    (
        select
            s1.product_id,--商品id
            count(distinct s1.third_tradeno) as product_order_cnt_7,--7日商品维度订单数
            sum(s1.item_price/100) as product_pay_fee_7--7日商品维度支付金额
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
        inner join
            origin_common.cc_ods_dwxk_fs_wk_cck_user s2
        on 
            s1.cck_uid=s2.cck_uid
        where 
            s1.ds>='${begin_date}' 
        and 
            s1.ds<='${end_date}' 
        and 
            s2.platform =14 
        and 
            s2.ds='${end_date}'
        group by 
            s1.product_id
    ) t1
    left join 
    (
        select
            s1.product_id as product_id,--商品id
            count(s1.ds) as ipv_7--7日商品维度详情页浏览数
        from
        (
            select
                ds,
                product_id--商品id 
            from 
                origin_common.cc_ods_log_cctui_product_coupon_detail_hourly
            where 
                ds>='${begin_date}' 
            and 
                ds<='${end_date}' 
            and 
                detail_type='item'
            union all
            select
                ds,
                product_id--商品id 
            from 
                origin_common.cc_ods_log_gwapp_product_detail_hourly
            where 
                ds>='${begin_date}' 
            and 
                ds<='${end_date}'
        ) s1
        group by s1.product_id  
    ) t2
    on t1.product_id=t2.product_id
    left join 
    (
        select
            product_id,--商品id
            shop_id,--店铺id
            shop_title,--店铺名称
            shop_cname1,--一级类目
            shop_cname2--二级类目
        from data.cc_dw_fs_products_shops
    ) t3
    on t1.product_id=t3.product_id
    left join
    (
        select
            s1.product_id as product_id,--商品id
            count(s1.star_num) as product_eva_rate_cnt_7,--7日商品维度评价数
            sum(if(s1.star_num=1,1,0)) as product_bad_eva_rate_cnt_7--7日商品维度差评价数
        from
        (
            select
                distinct
                product_id,--商品id
                star_num,--评价分数
                order_sn--订单号
            from 
                origin_common.cc_rate_star
            where   
                ds>='${begin_date}' 
            and 
                ds<='${end_date}' 
            and 
                rate_id >0 
            and 
                order_sn!='170213194354LFo017564wk'
        ) s1	
        inner join 
        (
            select
                distinct m1.third_tradeno as order_sn--订单号
            from 
                origin_common.cc_ods_dwxk_wk_sales_deal_ctime m1
            inner join
                origin_common.cc_ods_dwxk_fs_wk_cck_user m2
            on 
                m1.cck_uid=m2.cck_uid
            where 
                m1.ds>='${begin_date}' 
            and 
                m1.ds<='${end_date}' 
            and 
                m2.platform =14 
            and 
                m2.ds='${end_date}'
        ) s2
        on s1.order_sn=s2.order_sn
        group by s1.product_id
    ) t4
    on t1.product_id=t4.product_id
    left join 
    (
        select
	        s2.product_id as product_id,
	        count(s1.order_sn) as product_refund_cnt_after_delivery--7日商品维度发货后退款数
	    from	
        (
            select
                distinct 
                m1.order_sn as order_sn
            from 
                origin_common.cc_ods_fs_refund_order m1
            inner join
                origin_common.cc_order_user_delivery_time m2
            on 
                m1.order_sn=m2.order_sn 
            where
                from_unixtime(m1.create_time,'yyyyMMdd')>='${begin_date}' 
            and
                from_unixtime(m1.create_time,'yyyyMMdd')<='${end_date}'
            and  
                m1.status=1
        ) s1
        inner join
        (
            select
                distinct 
                m1.third_tradeno as order_sn,--订单号
                m1.product_id  as product_id 
            from 
                origin_common.cc_ods_dwxk_wk_sales_deal_ctime m1
            inner join
                origin_common.cc_ods_dwxk_fs_wk_cck_user m2
            on 
                m1.cck_uid=m2.cck_uid
            where 
                m1.ds>='${begin_date}' 
            and 
                m1.ds<='${end_date}' 
            and 
                m2.platform =14 
            and 
                m2.ds='${end_date}'
        ) s2
        on s1.order_sn=s2.order_sn
        group by s2.product_id
    ) t5
    on t1.product_id=t5.product_id
    left join 
    (
        select
            s2.product_id as product_id,
            count(s1.order_sn) as product_delivery_num,--7日商品维度发货数量
            sum(if(s1.delivery_time-s2.create_time>86400,1,0)) as product_count_delivery_overtime,--7日商品维度超时发货数
            sum(s1.delivery_time-s2.create_time) as product_delivery_duration--7日日商品维度发货总时长
        from
        (
            select
                order_sn,
                delivery_time
            from 
                cc_order_user_delivery_time 
            where 
                ds>='${begin_date}' 
            and 
                ds<='${end_date}'
        ) s1 
        inner join 
        (
            select
                distinct 
                m1.third_tradeno as order_sn,--订单号
                m1.product_id as product_id,
                m1.create_time as create_time 
            from 
                origin_common.cc_ods_dwxk_wk_sales_deal_ctime m1
            inner join
                origin_common.cc_ods_dwxk_fs_wk_cck_user m2
            on 
                m1.cck_uid=m2.cck_uid
            where 
                m1.ds>='${begin_date}' 
            and 
                m1.ds<='${end_date}' 
            and 
                m2.platform =14 
            and 
                m2.ds='${end_date}'
        ) s2 
        on 
            s1.order_sn=s2.order_sn
        group by 
            s2.product_id
    ) t6
    on t1.product_id=t6.product_id
    left join 
    (
        select
            s2.product_id as product_id,
            count(s1.order_sn) as product_delivery_num,--7日商品维度发货数量
            sum(if(s1.delivery_time-s2.create_time>172800,1,0)) as product_count_delivery_overtime--7日商品维度超时发货数
        from
        (
            select
                order_sn,
                delivery_time
            from 
                origin_common.cc_order_user_delivery_time 
            where 
                ds>='${begin_date}' 
            and 
                ds<='${end_date}'
        ) s1 
        inner join 
        (
            select
                distinct 
                m1.third_tradeno as order_sn,--订单号
                m1.product_id as product_id,
                m1.create_time as create_time 
            from 
                origin_common.cc_ods_dwxk_wk_sales_deal_ctime m1
            inner join
                origin_common.cc_ods_dwxk_fs_wk_cck_user m2
            on 
                m1.cck_uid=m2.cck_uid
            where 
                m1.ds>='${begin_date}' 
            and 
                m1.ds<='${end_date}' 
            and 
                m2.platform =14 
            and 
                m2.ds='${end_date}'
        ) s2 
        on 
            s1.order_sn=s2.order_sn
        group by 
            s2.product_id
    ) t7
    on t1.product_id=t7.product_id
    left join
    (
        select  
            s2.product_id as product_id,
            count(s1.order_sn) as product_order_num_ship_success,--7日商品维度签收订单数
            sum(s1.ship_time)  as product_ship_duration--7日商品维度物流总时长
        from
        (
            select
                order_sn,
                (update_time-create_time) as ship_time--物流时长
            from 
                data.cc_cct_product_ship_info
            where 
                ds>='${begin_date}' 
            and 
                ds<='${end_date}'
        ) s1
        inner join
        (
            select
                distinct 
                m1.third_tradeno as order_sn,--订单号
                m1.product_id  as product_id
            from 
                cc_ods_dwxk_wk_sales_deal_ctime m1
            inner join
                cc_ods_dwxk_fs_wk_cck_user m2
            on 
                m1.cck_uid=m2.cck_uid
            where 
                m1.ds>='${begin_date}' 
            and 
                m1.ds<='${end_date}' 
            and 
                m2.platform =14 
            and 
                m2.ds='${end_date}'
        ) s2
        on s1.order_sn = s2.order_sn
        group by s2.product_id
    ) t8
    on t1.product_id=t8.product_id
    group by t3.shop_id,t3.shop_title,t3.shop_cname1
) p1
left join 
(
    select
        app_shop_id as shop_id,
        count(distinct ad_id)  as online_product_num,--7日在线商品数
        sum(if(start_time>=unix_timestamp('${begin_date}','yyyyMMdd') and start_time<=unix_timestamp('${end_date}','yyyyMMdd'),1,0)) as new_product_num--7日上新商品数
     from 
         origin_common.cc_ods_fs_dwxk_ad_items_daily
     where 
         audit_status=1 
     and 
         status>0 
     and 
         start_time<unix_timestamp('${end_date}','yyyyMMdd') 
     and 
         end_time>=unix_timestamp('${begin_date}','yyyyMMdd')
     group by app_shop_id
) p2
on p1.shop_id=p2.shop_id
left join 
(
    select
        shop_id,
        sum(mantakesessioncount) as shop_count_huihua,--7日店铺维度会话数
        sum(waiterreplaysessioncount) as shop_count_jiedai,--7日店铺维度接待数
        sum(totalwaitetime) as shop_count_totalwaitetime--7日店铺维度等待时长即首次回复时长
	from 
        report.cc_rpt_cctui_im_shop_stat
	where 
        ds>='${begin_date}' 
    and 
        ds<='${end_date}'
	group by shop_id
) p3
on p1.shop_id=p3.shop_id
left join
(
    select
        s1.shop_id,
        count(s1.id) as count_task,--工单数
        sum(s1.is_overtime) as count_task_overtime--超时工单数
    from
    (
        select
            shop_id,
            order_id,
            id,
            is_overtime
        from 
            cc_ods_fs_task
        where 
            from_unixtime(created_on,'yyyyMMdd')>='${begin_date}' 
        and 
            from_unixtime(created_on,'yyyyMMdd')<='${end_date}'
    ) s1 
    inner join
    (
        select
            distinct m1.third_tradeno as order_sn--订单号
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime m1
        inner join
            origin_common.cc_ods_dwxk_fs_wk_cck_user m2
        on 
            m1.cck_uid=m2.cck_uid
        where 
            m1.ds>='${begin_date}' 
        and 
            m1.ds<='${end_date}' 
        and 
            m2.platform =14 
        and 
            m2.ds='${end_date}'
    ) s2
    on s1.order_id=s2.order_sn
    group by s1.shop_id
) p4
on p1.shop_id=p4.shop_id





