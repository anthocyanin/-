select
	t1.product_id,--7日有销量商品的ID
    t2.product_cname1,--一级类目
    t2.product_cname2,--二级类目
    t2.shop_id,--店铺id
    t2.shop_title,--店铺名称
	(
	case	
	when t2.shop_id in (18532,19268,19347) then '代发'
	when t2.shop_id in (18606,15426,18314,19534,2873,19708,2369,19709,19755,19756,19871,19872,20179,13930,15907,20513,20652,20653,20725,20789,18706,18586,18569,18262) then '小团子'
	when t2.shop_id in (19470,19486,19505,19521,19527,19542,19525,19580,19599,19609,19613,19664,19678,19682,19683,19699,19701,19765,19742,19722,19753,20016,19906,19907,20063,20064,20168,20178,20188,20202,20237,20236,4086,20697,20737,19627,20748,18327,15729,2752,12375,4599,13706,15395,12461,19654,16293,4024,20353,17929,104,3037,19170,14948,1793,19207,4999,16137,3885,17210,5987,1341,18381,16194,16133,8670,2254,13698,18565,15853,16305,13363,17639,12766,12033,7200,14720,9349,20818,4539,9806,6163,12523,13559,13991,1412,15129,1655,17157,17397,1802,18057,1831,18812,18814,1937,7572,9621,17845) then '一亩田'
	when t2.shop_id = 18164 then '自营'
	else 'pop' end
	) as shop_type,--店铺类型
	t3.ipv_uv,--30日ipv_uv
	t5.pay_fee_30d,--30日订单数支付金额
	t5.cck_commission_30d,--30日佣金额
	t5.pay_count_30d,--30日订单数
	t5.refund_count_30d,--30日发货后退款数
	(t5.refund_count_30d/t5.count_delivery) as rate_refund,--30日发货退款率
	t5.eva_cnt,--30日有效评价数
	t5.bad_eva_cnt,--30日差评数
	(t5.bad_eva_cnt/t5.eva_cnt) as rate_bad,--30日差评率
	t5.count_delivery,--30日发货数
	t5.product_delivery_duration,--3日日商品维度发货总时长
	t5.count_delivery_overtime_24,--30日超时24h发货数
	t5.count_delivery_overtime_48,--30日超时48h发货数
	t5.count_delivery_overtime_72,--30日超时72h发货数
	t5.product_order_num_ship_success,--30日商品维度签收订单数
    t5.product_ship_duration,--30日商品维度物流总时长
    t5.count_task,--30工单数
    t5.count_task_overtime,--30超时工单数
    (t5.count_task_overtime/t5.count_task) as task_overtime_rate,--30日工单超时率
    t6.buyer_num,--30日购买人数
	t6.again_buyer_num,--30日二次购买人数
	(t6.again_buyer_num/t6.buyer_num) as again_buyer_rate--30日复购率
from
(
	select
		distinct
		s1.product_id
	from
	(
		select
			product_id,
			cck_uid
		from
			origin_common.cc_ods_dwxk_wk_sales_deal_ctime
		where
			ds >= '${begin_date_30d}'
		and
			ds <= '${end_date}' 
	)s1
	inner join
	(
		select
			cck_uid
		from
			origin_common.cc_ods_dwxk_fs_wk_cck_user 
		where
			ds = '${end_date}'
		and
			platform = 14
	)s2
	on s1.cck_uid=s2.cck_uid
)t1
left join
(
    select
    	distinct
        product_id,--商品id
        product_cname1,
        product_cname2,
        shop_id,--店铺id
        shop_title--店铺名称
    from 
	    data.cc_dw_fs_products_shops
)t2
on t1.product_id=t2.product_id
left join
(
	select
		a1.product_id,
		sum(a1.ipv_uv) as ipv_uv--30日ipv_uv
	from
	(
		select
			ds,
			product_id,
			count(distinct user_id) as ipv_uv
		from
			origin_common.cc_ods_log_cctui_product_coupon_detail_hourly
		where
			ds >= '${begin_date_30d}'
		and
			ds <= '${end_date}'
		and
			detail_type = 'item'
		group by
			ds,product_id
	)a1
	group by
		a1.product_id
)t3
on t1.product_id=t3.product_id
left join
(
	select
		a1.product_id,
		sum(a1.item_price/100) as pay_fee_30d,--30日订单数支付金额
		sum(a1.cck_commission/100) as cck_commission_30d,--30日佣金额
		count(a1.third_tradeno) as pay_count_30d,--30日订单数
		count(a2.order_sn) as refund_count_30d,--30日发货后退款数
		count(a3.rate_id) as eva_cnt,--30日评价数
		sum(if(a3.star_num=1,1,0)) as bad_eva_cnt,--30日差评数
		count(a4.order_sn) as count_delivery,--30日发货数
		sum(a4.delivery_time-a1.create_time) as product_delivery_duration,--3日日商品维度发货总时长
		sum(if(a4.delivery_time-a1.create_time>86400,1,0)) as count_delivery_overtime_24,--30日超时发货数
		sum(if(a4.delivery_time-a1.create_time>172800,1,0)) as count_delivery_overtime_48,--30日超时发货数
		sum(if(a4.delivery_time-a1.create_time>259200,1,0)) as count_delivery_overtime_72,--30日超时发货数
		count(a5.order_sn) as product_order_num_ship_success,--30日商品维度签收订单数
        sum(a5.ship_time) as product_ship_duration,--30日商品维度物流总时长
        count(a6.id) as count_task,--30工单数
        sum(a6.is_overtime) as count_task_overtime--30超时工单数
	from
	(
		select
			s1.product_id,
			s1.item_price,
			s1.cck_commission,
			s1.third_tradeno,
			s1.create_time
		from
		(
			select
				product_id,
				item_price,
				cck_commission,
				third_tradeno,
				create_time,
				cck_uid
			from
				origin_common.cc_ods_dwxk_wk_sales_deal_ctime
			where
				ds >= '${begin_date_30d}'
			and
				ds <= '${end_date}' 
		)s1
		inner join
		(
			select
				cck_uid
			from
				origin_common.cc_ods_dwxk_fs_wk_cck_user 
			where
				ds = '${end_date}'
			and
				platform = 14
		)s2
		on s1.cck_uid=s2.cck_uid
	) a1
	left join
	(
		select 
			distinct
		  	s1.order_sn
		from 
		(
			select 
			  	order_sn
			from
			  	origin_common.cc_ods_fs_refund_order 
			where
	            from_unixtime(create_time,'yyyyMMdd') >= '${begin_date_30d}' 
	        and
	            from_unixtime(create_time,'yyyyMMdd') <= '${end_date}'
			and
				status =1
			and 
				refund_reason != 31
        )s1
		inner join
		(
			select 
			  	order_sn
			from 
				origin_common.cc_order_user_delivery_time
			where
				ds >= '${begin_date_30d}'
			and
				ds <= '${end_date}' 
		)s2
		on s1.order_sn=s2.order_sn
	) a2
	on a1.third_tradeno=a2.order_sn
	left join
	(
		select 
			distinct
		    order_sn,
		    rate_id,
		    star_num
		from
		    origin_common.cc_rate_star
  		where
		  	ds >= '${begin_date_30d}'
		and 
            ds <= '${end_date}' 
		and
    		rate_id != 0
		and
			order_sn!='170213194354LFo017564wk'
	) a3
	on a1.third_tradeno=a3.order_sn
	left join
	(
		select
			distinct
	        order_sn,
	        delivery_time
   		from 
   			origin_common.cc_order_user_delivery_time
		where 
   			ds >= '${begin_date_30d}' 
        and 
		    ds <= '${end_date}'
	) a4
	on a1.third_tradeno=a4.order_sn
	left join
    (
        select
            order_sn,
            (update_time-create_time) as ship_time--物流时长
        from 
            data.cc_cct_product_ship_info
        where 
   			ds >= '${begin_date_30d}' 
        and 
		    ds <= '${end_date}'
    ) a5
    on a1.third_tradeno=a5.order_sn
	left join
    (
        select
            order_id,
            id,
            is_overtime
        from 
            origin_common.cc_ods_fs_task
        where 
            from_unixtime(created_on,'yyyyMMdd') >= '${begin_date_30d}'
        and 
            from_unixtime(created_on,'yyyyMMdd') <= '${end_date}'
    ) a6
    on a1.third_tradeno=a6.order_id
    group by a1.product_id
)t5
on t1.product_id=t5.product_id
left join
(
	select
		a1.product_id,
		count(a1.uid) as buyer_num,
		sum(if(a1.num>=2,1,0)) as again_buyer_num
	from
	(
    	select
			s1.product_id,
			s1.uid,
			count(s1.create_time) as num 
		from
		(
	    	select
				product_id,
				cck_uid,
				uid,
				create_time
			from
				origin_common.cc_ods_dwxk_wk_sales_deal_ctime
			where
				ds >= '${begin_date_30d}'
			and
				ds <= '${end_date}' 
		)s1
		inner join
		(
			select
				cck_uid
			from 	
				origin_common.cc_ods_dwxk_fs_wk_cck_user 
			where
				ds = '${end_date}'
			and
				platform = 14
		)s2
		on s1.cck_uid=s2.cck_uid
		group by 
			s1.product_id,s1.uid
	) a1
	group by a1.product_id
)t6
on t1.product_id=t6.product_id
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
每周开店数据
select
	s2.shop_id,
	s3.name as shop_cname1,
	s2.shop_name,
	s2.company_name,
	from_unixtime(s1.create_time,'yyyyMMdd HH:mm:ss') as open_time
from
(
	select
		owner_id,
		cn_name,
		create_time
	from
		origin_common.cc_ods_fs_shop
	where 
	    from_unixtime(create_time,'yyyyMMdd') >= '${begin_date}'
	and 
	    from_unixtime(create_time,'yyyyMMdd') <= '${end_date}'
	and
		platform = 1
) s1
left join 
(
	select
		uid,
		shop_id,
		shop_name,
		company_name,
		category1
	from
		origin_common.cc_ods_fs_business_basic
) s2
on s1.owner_id=s2.uid
left join 
(
    select
        s5.last_cid as last_cid,
        s6.name     as name,
        s5.c1       as c1
    from 
        origin_common.cc_category_cascade s5
    join 
        origin_common.cc_ods_fs_category s6
    on 
        s5.c1 = s6.cid
    where 
        s5.ds='${end_date}'
) s3
on s2.category1 = s3.last_cid
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
每周关店数据
select
	t1.shop_id,
	t3.name as shop_cname1,
	t2.shop_name,
	t2.company_name,
	t1.apply_reason,
	t1.source
from
(
	select
		shop_id,
		apply_reason,
		source
	from
		origin_common.cc_ods_fs_shop_close
	where 
	    from_unixtime(insert_date,'yyyyMMdd')>='${begin_date}'
	and 
	    from_unixtime(insert_date,'yyyyMMdd')<='${end_date}'
	and
		status = 'close' 
) t1
left join
(
	select
		shop_id,
		shop_name,
		company_name,
		category1
	from 
		origin_common.cc_ods_fs_business_basic
) t2
on t1.shop_id=t2.shop_id
left join 
(
    select
        s5.last_cid as last_cid,
        s6.name     as name,
        s5.c1       as c1
    from 
        origin_common.cc_category_cascade s5
    join 
        origin_common.cc_ods_fs_category s6
    on 
        s5.c1 = s6.cid
    where 
        s5.ds='${end_date}'
) t3
on t2.category1 = t3.last_cid
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
商品30日复购数据
select
    n1.product_id,
    n2.product_title,
    n2.product_cname1,
    n2.shop_id,
    n2.shop_title,
    n1.total_num,
    n1.total_buyer_num,
    (n1.one_time_buyer_num/n1.total_buyer_num) as one_buyer_rate,
    (n1.two_times_buyer_num/n1.total_buyer_num) as two_buyer_rate,
    (n1.three_times_buyer_num/n1.total_buyer_num) as three_buyer_rate,
    (n1.four_times_buyer_num/n1.total_buyer_num) as four_buyer_rate,
    (n1.five_times_buyer_num/n1.total_buyer_num) as five_buyer_rate,
    (n1.six_times_buyer_num/n1.total_buyer_num) as six_buyer_rate,
    (n1.seven_times_buyer_num/n1.total_buyer_num) as seven_buyer_rate,
    (n1.more_than_seven_times_buyer_num/n1.total_buyer_num) as more_than_seven_buyer_rate
from
(
    select
        a1.product_id         as product_id,
        sum(a1.num)			  as total_num,
        count(a1.uid)         as total_buyer_num,
        sum(if(a1.num=1,1,0)) as one_time_buyer_num,
        sum(if(a1.num=2,1,0)) as two_times_buyer_num,
        sum(if(a1.num=3,1,0)) as three_times_buyer_num,
        sum(if(a1.num=4,1,0)) as four_times_buyer_num,
        sum(if(a1.num=5,1,0)) as five_times_buyer_num,
        sum(if(a1.num=6,1,0)) as six_times_buyer_num,
        sum(if(a1.num=7,1,0)) as seven_times_buyer_num,
        sum(if(a1.num>7,1,0)) as more_than_seven_times_buyer_num
    from
    (
        select
            s1.product_id,
            s1.uid,
            count(s1.create_time) as num 
        from
        (
            select
                product_id,
                cck_uid,
                uid,
                create_time
            from
                origin_common.cc_ods_dwxk_wk_sales_deal_ctime
            where
                ds >= '${begin_date_30d}'
            and
                ds <= '${end_date}' 
        )s1
        inner join
        (
            select
                cck_uid
            from    
                origin_common.cc_ods_dwxk_fs_wk_cck_user 
            where
                ds = '${end_date}'
            and
                platform = 14
        )s2
        on s1.cck_uid=s2.cck_uid
        group by 
            s1.product_id,s1.uid
    ) a1
    group by a1.product_id
) n1
left join 
(
    select
    	distinct
        product_id,
        product_title,
        product_cname1,
        shop_id,
        shop_title
    from
        data.cc_dw_fs_products_shops
) n2
on n1.product_id = n2.product_id
//////////////////////////////////////////////////////////////////////////////////////////////
东哥 大微信客的启用的店铺 一段时间内新创建的商品信息及推广与否
select
    n1.shop_id,
    n2.shop_title,
    n1.product_id,
    n2.product_title,
    n2.product_cname1,
    n2.product_cname2,
    n2.product_cname3,
    n1.ctime,
    n3.product_id,
    n4.start_time
from 
(
    select
        shop_id,
        product_id,
        from_unixtime(datetime,'yyyyMMdd HH:mm:dd') as ctime
    from 
        origin_common.cc_product
    where
        ds =20190315
    and 
        from_unixtime(datetime,'yyyyMMdd') >= '${begin_date}'
    and
        from_unixtime(datetime,'yyyyMMdd') <= '${end_date}'
    and 
        shop_id in (
		18532,19268,24017,18112,19370,19412,18907,19561,19434,19700,19636,12964,19570,19766,19908,19697,7780,19884,20201,10064,19698,20294,20350,20401,20205,17516,20499,20500,20719,20754,20937,20939,20931,20860,22605,22361,13589,22452,22463,22747,22760,22799,23137,18860,18323,18686,18693,18692,18674,18398,18633,18472,18625,14661,2776,18516,18501,19127,9665,17757,18385,18133,11646,14502,16785,10639,168,14390,6621,12545,18174,18145,12889,9961,9691,17172,17152,17012,11237,7647,10671,9570,16650,3826,17337,965,17490,18035,11845,182,9342,19277,18288,19284,17691,22881,22607,18551,23179,20667,23384,23634,17115,23721,23643,23794,23783,23736,23632,23904,23928,23961,23809,20400,24028,23966,24062,24046,24063,24060,24086,24090,19347,19209,17300,18595,18576,17501,15519,10097,13805,6427,18294,18250,19767,17485,19675,20806,8935,22819,23024,18683,18649,18510,15801,18117,18197,17726,17947,9601,17692,18255,17884,18309,8839,11760,14515,11677,17428,1374,9241,5529,17944,16717,17896,17903,19641,23934,23924,18432,20443,20534,20614,18053,20940,18071,18608,18590,18533,18579,18546,18526,18243,14560,17114,16907,10338,18065,15279,18217,17815,13278,18224,17839,17697,18007,16439,17218,8036,17582,10668,17698,17699,17686,14359,17461,17888,18937,17931,22853,17803,18871,18927,17702,16531,3533,20243,18467,17200,16315,8032,14975,13352,16819,18304,11184,13896,16530,18142,4128,8106,5138,8089,5666,16510,9151,8481,7479,533,17690,9238,16270,16567,6521,18330,1796,17704,4121,17951,20242,18682,12704,20545,20923,20916,20558,17563,20688,22751,22451,13098,22800,22917,9565,18518,12502,17531,10078,15811,10141,18417,11548,17850,13773,10036,17851,17982,17597,8270,17405,18317,18238,17315,17950,2995,18382,17920,10878,9304,23633,23246,23708,22699,18479,23801,23599,23891,23931,15159,19995,20965,19402,18965,20696,18662,18729,18740,19319,19405,22458,24051,19330,20335,20399,20481,20493,20427,20492,19651,9974,20541,20411,18552,18744,20336,18512,20604,19338,19596,20676,20648,20812,20524,20751,22728,20777,14686,18722,18611,18676,18684,18494,18547,18492,16540,18338,16355,12846,9950,12854,7199,16741,11137,17167,16687,12924,17349,17981,17645,17707,16375,16581,8756,17768,17644,17137,17441,17522,17693,18080,17926,18123,11279,17483,17748,17489,19111,19371,16273,18606,15426,18314,19534,2873,19708,2369,19709,19755,19756,19871,19872,20179,13930,15907,20513,20652,20653,20725,20789,18706,18586,18569,18262,19470,19486,19505,19521,19527,19542,19525,19580,19599,19609,19613,19664,19678,19682,19683,19699,19701,19765,19742,19722,19753,20016,19906,19907,20063,20064,20168,20178,20188,20202,20237,20236,4086,20697,20737,19627,20748,18327,15729,2752,12375,4599,13706,15395,12461,19654,16293,4024,20353,17929,104,3037,19170,14948,1793,19207,4999,16137,3885,16671,18791,17210,5987,1341,18381,16194,16133,8670,2254,3803,13698,18565,15853,16305,13363,17639,12766,12033,7200,14720,9349,20818,4539,9806,6163,12523,13559,13991,1412,15129,1655,17157,17397,1802,18057,1831,18812,18814,1937,7572,9621,17845,18164,24110,18815,24116,24113,24072,20471,11852,24112,24124,23631,24126,24119,24144,24135,20814,24156,24155,24157,24158,24168,24167,24166,24169,24199,24205,24100,24208
	)
) n1
left join 
(
    select
        distinct
        product_id,
        product_title,
        product_cname1,
        product_cname2,
        product_cname3,
        shop_title
    from
        data.cc_dw_fs_products_shops 
) n2
on n1.product_id =n2.product_id 
left join
(
    select
        distinct
        t2.app_item_id as product_id
    from 
    (
        select
            distinct
            item_id
        from 
            origin_common.cc_ods_fs_dwxk_ad_items_daily
        where
            audit_status = 1
        and
            status>0
        and
            from_unixtime(end_time,'yyyyMMdd')>='${end_date}'
    ) t1
    inner join
    (
        select 
            item_id, 
            app_item_id
        from 
            origin_common.cc_ods_dwxk_fs_wk_items
    ) t2
     on t1.item_id = t2.item_id
) n3
on n1.product_id = n3.product_id
left join 
(
    select
        t2.app_item_id as product_id,
        from_unixtime(t1.start_time,'yyyyMMdd HH:mm:dd') as start_time
    from 
    (
        select
            item_id,
            min(start_time) as start_time
        from 
            origin_common.cc_ods_fs_dwxk_ad_items_daily
        where
            audit_status = 1
        group by 
        	item_id
    ) t1
    inner join
    (
        select
        	distinct 
            item_id, 
            app_item_id
        from 
            origin_common.cc_ods_dwxk_fs_wk_items
    ) t2
     on t1.item_id = t2.item_id
) n4
on n1.product_id = n4.product_id
/////////////////////////////////////////////////////////////////////////////////////////////
