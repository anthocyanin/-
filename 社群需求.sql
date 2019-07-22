小仙 2018.12.18-2018.12.25和2019.2.18-2019.2.25
服务经理id，姓名，战区，招募人数，支付金额，佣金
select
	p1.gm_uid 																			as gm_uid,
	p2.real_name 																		as real_name,
	p2.phone 																			as phone,
	(
    case
	    when gm_uid in(574693,997954,678663,351503,446253,1068772,684796,946772,1088215,581919,376987,263970,353276,353349,612648,614766,589882,769441,330186,919648,540504,828712) then '一战区'
	    when gm_uid in(710973,1018891,620072,1117720,537784,453987,360184,255478,419227,459877,1102429,346058,939962,575222,350755,240474,240561,242804,520269,577979,1229407,960479,254945,285408,289429,266283,958082,1017197,1008462,1031618,998298,924852) then '二战区'  
	    when gm_uid in(721563,720760,744474,1016400,1001493,770362,736736,1034247,930631,1035670,1175069,1175203,760397,1017792,1049908,1025605,1031405,1034136,1101858,747406,1151558,909658,763931,901929,815637,1053184,709705,1182618,1002362,1254291,1050353,1143126) then '四战区'
	    when gm_uid in(1199321,1199168,1197475,1202494,1210498,1200648,1204007,1240629,1199210,1209314,1199978,1199305,1199214,1199749,1201288,1205821,1201128,1240633,1199956,1199621,1199365,1204049,1241239,1200483,1252167,1204461,1199985,1199515,1199635,1257637,1227974,1200608,1419045,1242477,1201979,1224204,1200412,1240632,1241385,1276826,1201275,1256190) then '三战区'
	    when gm_uid in(493355,289477,402913,1317551,532027,1245487,285968,240087,443123,240461,293459,273330,718749,325059,1199257,958082,344364,475906,507451,1022549,278330,245919,243168,976562,547890,950800,1012885,244360,1276634,1407703,1317590,1446199,461541) then '五战区'
	    when gm_uid in(1307543,1342969,1368362,1318636,1406113,1307523,493355,877172) then '七战区'
	    when gm_uid in(328580,569834,455813,985271,980086,471845,989050,1133012,816449,472959,399316,239746,940677,1586109,787686,1068651,1216371,692018,497607,1113907,1004655,573196,942532,931097,664703,1225213,790020,831619,551805,416797,415062,501015,1157510,278849,283867,944769,1312806,961545,1140146,240863,261756,1022418,863183,375083,280411,995759,334274,949822,430297,227880,1166918,521741,283867,850095,1067330,1198132,777208,872252,1169977,1481021,1182176,1156866,989201,602463,232877,967935,355368,498739,1029483,1038882,497521,730130,581807,861298,623628,268750,268078,452886) then '九战区'
	    else '其它战区'
    end
    ) as team,
	p1.team_cck_count 																	as team_cck_count,
	p1.cck_count 																		as cck_count,
	p1.pay_count 																		as pay_count,
	p1.fee 																				as fee,
	p1.cck_commission 																	as cck_commission,
	p1.pro_cck_count 																	as pro_cck_count,
	p1.pro_pay_count 																	as pro_pay_count,
	p1.pro_fee 																	    	as pro_fee,
	p1.pro_cck_commission 												            	as pro_cck_commission,
	p1.invite_cck_count 														    	as invite_cck_count,
	p1.invite_count 															    	as invite_count,
	p1.share_cck_count 															    	as share_cck_count,
	p1.share_count 																   		as share_count
from
(
	select
	    t1.gm_uid 													as gm_uid,
	    count(t1.cck_uid) 											as team_cck_count,
	    count(t2.cck_uid) 											as cck_count,
		sum(coalesce(t2.pay_count,0)) 		 						as pay_count,
		sum(coalesce(t2.fee,0)) 									as fee,
		sum(coalesce(t2.cck_commission,0)) 							as cck_commission,
	    count(t3.cck_uid) 											as pro_cck_count,
		sum(coalesce(t3.pay_count,0)) 								as pro_pay_count,
		sum(coalesce(t3.fee,0))										as pro_fee,
		sum(coalesce(t3.cck_commission,0)) 							as pro_cck_commission,		
		count(t4.cck_uid)		   								    as invite_cck_count,
		sum(coalesce(t4.invite_count,0)) 							as invite_count,
		count(t5.cck_uid) 											as share_cck_count,
		sum(coalesce(t5.share_count,0))								as share_count
	from
	(
		select
			distinct
			gm_uid,
			cck_uid
		from
			origin_common.cc_ods_fs_wk_cct_layer_info
		where
			gm_uid !=0
	        and
	        platform=14
		union all
		select
			distinct
			gm_uid,
			gm_uid as cck_uid
		from
			origin_common.cc_ods_fs_wk_cct_layer_info
		where
			gm_uid !=0
	        and
	        platform=14
	)t1
	left join
	(
	--->总销售
		select
			h1.cck_uid as cck_uid,
			count(distinct h1.third_tradeno) as pay_count,
			sum(h1.item_price/100) as fee,
			sum(h1.cck_commission/100) as cck_commission
		from
		(
			select
				cck_uid,
				third_tradeno,
				item_price,
				cck_commission
			from
				origin_common.cc_ods_dwxk_wk_sales_deal_ctime
			where
				ds>='${begin_date}' 
				and
				ds<='${end_date}' 
		)h1
		join
		(
			select
				cck_uid
			from
				origin_common.cc_ods_dwxk_fs_wk_cck_user
			where
				ds='${end_date}' 
				and
				platform=14
		)h2
		on h1.cck_uid=h2.cck_uid
		group by h1.cck_uid
	)t2
	on t1.cck_uid=t2.cck_uid
	left join
	(
	--->推广销售
		select
			h1.cck_uid as cck_uid,
			count(distinct h1.third_tradeno) as pay_count,
			sum(h1.item_price/100) as fee,
			sum(h1.cck_commission/100) as cck_commission
		from
		(
			select
				cck_uid,
				third_tradeno,
				item_price,
				cck_commission
			from
				origin_common.cc_ods_dwxk_wk_sales_deal_ctime
			where
				ds>='${begin_date}' 
				and
				ds<='${end_date}' 
		)h1
		join
		(
			select
				cck_uid
			from
				origin_common.cc_ods_dwxk_fs_wk_cck_user
			where
				ds='${end_date}' 
				and
				platform=14
		)h2
		on h1.cck_uid=h2.cck_uid
		left join
		(
			select
				order_sn,
				source
			from
				origin_common.cc_ods_log_gwapp_order_track_hourly
			where
				ds>='${begin_date}' 
				and
				ds<='${end_date}' 
		)h3
		on h1.third_tradeno=h3.order_sn
		where h3.source != 'cctui'
		group by h1.cck_uid
	)t3
	on t1.cck_uid=t3.cck_uid
	left join
	(
	--->招募
		select
			h1.invite_uid as cck_uid,
			count(h1.create_time) as invite_count
		from
		(
			select
				invite_uid,
				cck_uid,
				create_time
			from
				origin_common.cc_ods_fs_wk_cct_layer_info
			where
				from_unixtime(create_time,'yyyyMMdd')>='${begin_date}' 
				and
				from_unixtime(create_time,'yyyyMMdd')<='${end_date}' 
				and
				platform=14
				and
				status = 1
		)h1
		inner join
		(
			select
				cck_uid
			from
				origin_common.cc_ods_dwxk_fs_wk_business_info
			where
				ds='${end_date}' 
				and
				pay_price in(39900,49900,9900,19900)
		)h2
		on h1.cck_uid=h2.cck_uid
		group by h1.invite_uid
	)t4
	on t1.cck_uid=t4.cck_uid
	left join
	(
	--->分享
		select
			h2.cck_uid as cck_uid,
		    count(h1.user_id) as share_count
	    from
	    (
	    	select
		        user_id
		    from 
		        origin_common.cc_ods_log_cctapp_click_hourly
		    where 
		        ds>='${begin_date}' 
		        and
		        ds<='${end_date}' 
				and 
				module = 'detail_material' 
				and 
				zone in ('line','small_routine','pQrCode','promotion')
				and	
				source in ('cct','cctui')
		)h1
		left join
		(
			select
				cck_uid,
				cct_uid
			from
				origin_common.cc_ods_dwxk_fs_wk_cck_user
			where
				ds='${end_date}' 
				and
				platform=14
		)h2
		on h1.user_id=h2.cct_uid
		group by h2.cck_uid
	)t5
	on t1.cck_uid=t5.cck_uid
	group by
		t1.gm_uid
)p1
left join
(
	select
		cck_uid,
		real_name,
		phone
	from
		origin_common.cc_ods_dwxk_fs_wk_business_info
	where
		ds='${end_date}' 
)p2
on p1.gm_uid=p2.cck_uid

/////////////////////////////////////////////////////////////////////
马竞译
总监id、总监姓名、所属上级、总监团队销售额、总监个人销售额，其中销售额去除退款；
数据维度：11月、12月、1月、2月1日~2月24日

select
	p1.leader_uid 		as leader_uid,
	p2.real_name 		as real_name,
	p2.phone 			as phone,
	p1.gm_uid			as gm_uid,
	p1.team_cck_count 	as team_cck_count,
	p1.cck_count 		as cck_count,
	COALESCE(p1.team_pay_count,0) 		as team_pay_count,
	COALESCE(p1.team_fee,0) 			as team_fee,
	COALESCE(p1.team_cck_commission,0) 	as team_cck_commission,
	COALESCE(p1.self_pay_count,0)   	as self_pay_count,
	COALESCE(p1.self_fee,0) 			as self_fee,
	COALESCE(p1.self_cck_commission,0)  as self_cck_commission
from
(
	select
	    t1.leader_uid 						as leader_uid,
	    max(t1.gm_uid) 						as gm_uid,
	    count(t1.cck_uid) 					as team_cck_count,
	    count(t2.cck_uid) 					as cck_count,
		sum(t2.pay_count) 		 			as team_pay_count,
		sum(t2.fee) 						as team_fee,
		sum(t2.cck_commission) 			    as team_cck_commission,
		sum(if(t1.type=1,t2.pay_count,0)) 	   as self_pay_count,
		sum(if(t1.type=1,t2.fee,0))      	   as self_fee,
		sum(if(t1.type=1,t2.cck_commission,0)) as self_cck_commission
	from
	(
		select
			distinct
			leader_uid,
			gm_uid,
			cck_uid,
			0 as type
		from
			origin_common.cc_ods_fs_wk_cct_layer_info
		where
			gm_uid in (263970,769441,384542,351503,612648,574693,1068772,946772,353276,446253,589882,684796,353349,985037,384261,1088215,541262,376987,614766,997954,330186,919648,1107104,446707,678663,1086499,540504,850095,537969,411164)
	        and
	        leader_uid !=0
	        and
	        platform=14
		union all
		select
			distinct
			leader_uid,
			gm_uid,
			leader_uid as cck_uid,
			1 as type
		from
			origin_common.cc_ods_fs_wk_cct_layer_info
		where
			gm_uid in (263970,769441,384542,351503,612648,574693,1068772,946772,353276,446253,589882,684796,353349,985037,384261,1088215,541262,376987,614766,997954,330186,919648,1107104,446707,678663,1086499,540504,850095,537969,411164)
	        and
	        leader_uid !=0
	        and
	        platform=14
	)t1
	left join
	(
	--->总销售
		select
			h1.cck_uid as cck_uid,
			count(distinct h1.third_tradeno) as pay_count,
			sum(h1.item_price/100) as fee,
			sum(h1.cck_commission/100) as cck_commission
		from
		(
			select
				cck_uid,
				third_tradeno,
				item_price,
				cck_commission
			from
				origin_common.cc_ods_dwxk_wk_sales_deal_ctime
			where
				ds>='${begin_date}' 
				and
				ds<='${end_date}' 
		)h1
		inner join
		(
			select
				cck_uid
			from
				origin_common.cc_ods_dwxk_fs_wk_cck_user
			where
				ds='${end_date}' 
				and
				platform=14
		)h2
		on h1.cck_uid=h2.cck_uid
		left join 
		(
	        select
	            order_sn
	        from
	            origin_common.cc_ods_fs_refund_order
	        where 
	            from_unixtime(create_time,'yyyyMMdd') >= 20181101
	        and
	            status = 1
		)h3
		on h1.third_tradeno=h3.order_sn
		where h3.order_sn is null
		group by h1.cck_uid
	)t2
	on t1.cck_uid=t2.cck_uid
	group by
		t1.leader_uid
)p1
left join
(
	select
		cck_uid,
		real_name,
		phone
	from
		origin_common.cc_ods_dwxk_fs_wk_business_info
	where
		ds=20190225
)p2
on p1.leader_uid=p2.cck_uid

////////////////////////////////////////////////////////////////////////////////////
小仙 12月和2月整体及各战区情况
销售、招募、商品分享次数、裂变率（除去1-3号进来的人，在12月和2月的新增VIP的裂变数/率）、开单率（除去1-3号进来的人，在12月和2月的开单数/率）
select
	p1.gm_uid 																			as gm_uid,
	p2.real_name 																		as real_name,
	p2.phone 																			as phone,
	(
	    case
		    when gm_uid in(376987,614766,1068772,684796,946772,612648,574693,330186,540504,769441,541262,678663,353276,589882,997954,446253,351503,263970,353349,919648,1088215,384542,985037,384261,1107104,446707,1086499,850095,537969,411164) then '一战区'
		    when gm_uid in(939962,459877,1117720,575222,255478,453987,710973,537784,240474,350755,520269,360184,240561,577979,620072,1018891,242804,419227,346058,1102429,1229407,960479,254945,285408,289429,266283,958082,1017197,1008462,1031618,998298,924852) then '二战区'  
		    when gm_uid in(815637,770362,1034247,1035670,1151558,760397,721563,1016400,1017792,1049908,744474,720760,930631,901929,1175069,1034136,1001493,1175203,736736,909658,747406,1053184,763931,1025605,1101858,1031405,1002362,709705,1050353,1182618,1254291,1143126) then '四战区'
		    when gm_uid in(1241239,1276826,1256190,1201128,1199956,1202494,1199210,1419045,1200412,1257213,1211271,1209138,1201288,1204461,1240632,1242477,1252167,1199321,1240633,1201979,1199214,1241385,1209314,1257637,1199305,1204049,1200483,1205821,1199515,1204007,1199985,1210498,1197475,1199621,1200648,1200608,1199749,1224204,1227974,1199168,1199365,1199978,1240629,1201275,1199635,1350622,1202058,1224242) then '三战区'
		    when gm_uid in(289477,1245487,1199257,443123,493355,976562,245919,325059,278330,958082,532027,547890,718749,293459,950800,240461,507451,1022549,243168,402913,285968,344364,240087,1317551,1012885,273330,244360,1276634,1407703,1317590,1446199,461541,475906) then '五战区'
		    when gm_uid in(493355,877172,1406113,1342969,1307523,1368362,1318636,1307543,1481982,1052413) then '七战区'
		    when gm_uid in(261756,239746,989050,227880,942532,787686,283867,1216371,949822,399316,375083,1312806,995759,1586109,961545,1140146,551805,980086,940677,430297,280411,501015,985271,471845,1068651,416797,1225213,931097,664703,944769,1113907,334274,569834,692018,573196,497607,1022418,1133012,1004655,415062,240863,1156866,790020,455813,472959,278849,831619,816449,1157510,328580,863183,1166918,521741,850095,1067330,1198132,777208,872252,1169977,1481021,1182176,989201,602463,232877,967935,355368,498739,1029483,1038882,497521,730130,581807,861298,623628,268750,268078,452886) then '九战区'
		    else '其它战区'
	    end
    ) as team,
	p1.team_cck_count 																	as team_cck_count,
	p1.cck_count 																		as cck_count,
	p1.pay_count 																		as pay_count,
	p1.fee 																				as fee,
	p1.cck_commission 																	as cck_commission,
	p1.invite_cck_count 														    	as invite_cck_count,
	p1.invite_count 															    	as invite_count,
	p1.share_cck_count 															    	as share_cck_count,
	p1.share_count 																   		as share_count,
	p1.new_cck_invite_count,
	p1.new_cck_invite_num,
	p1.new_cck_buy_count
from
(
	select
	    t1.gm_uid 													as gm_uid,
	    count(t1.cck_uid) 											as team_cck_count,
	    count(t2.cck_uid) 											as cck_count,
		sum(coalesce(t2.pay_count,0)) 		 						as pay_count,
		sum(coalesce(t2.fee,0)) 									as fee,
		sum(coalesce(t2.cck_commission,0)) 							as cck_commission,		
		count(t4.cck_uid)		   								    as invite_cck_count,
		sum(coalesce(t4.invite_count,0)) 							as invite_count,--新入住人数
		count(t5.cck_uid) 											as share_cck_count,
		sum(coalesce(t5.share_count,0))								as share_count,
		count(t7.cck_uid) as new_cck_invite_count,--新人在邀人数
		sum(t7.new_cck_invite_count) as new_cck_invite_num,--新人邀请总数
		count(t6.cck_uid) as new_cck_buy_count--新人开单人数
	from
	(
		select
			distinct
			gm_uid,
			cck_uid
		from
			origin_common.cc_ods_fs_wk_cct_layer_info
		where
			gm_uid !=0
	        and
	        platform=14
		union all
		select
			distinct
			gm_uid,
			gm_uid as cck_uid
		from
			origin_common.cc_ods_fs_wk_cct_layer_info
		where
			gm_uid !=0
	        and
	        platform=14
	)t1
	left join
	(
	--->总销售
		select
			h1.cck_uid as cck_uid,
			count(distinct h1.third_tradeno) as pay_count,
			sum(h1.item_price/100) as fee,
			sum(h1.cck_commission/100) as cck_commission
		from
		(
			select
				cck_uid,
				third_tradeno,
				item_price,
				cck_commission
			from
				origin_common.cc_ods_dwxk_wk_sales_deal_ctime
			where
				ds>='${begin_date}' 
				and
				ds<='${end_date}' 
		)h1
		inner join
		(
			select
				cck_uid
			from
				origin_common.cc_ods_dwxk_fs_wk_cck_user
			where
				ds='${end_date}' 
				and
				platform=14
		)h2
		on h1.cck_uid=h2.cck_uid
		group by h1.cck_uid
	)t2
	on t1.cck_uid=t2.cck_uid
	left join
	(
	--->招募
		select
			h1.invite_uid as cck_uid,
			count(h1.create_time) as invite_count
		from
		(
			select
				invite_uid,
				cck_uid,
				create_time
			from
				origin_common.cc_ods_fs_wk_cct_layer_info
			where
				from_unixtime(create_time,'yyyyMMdd')>='${begin_date}' 
				and
				from_unixtime(create_time,'yyyyMMdd')<='${end_date}' 
				and
				platform=14
				and
				status = 1
		)h1
		inner join
		(
			select
				cck_uid
			from
				origin_common.cc_ods_dwxk_fs_wk_business_info
			where
				ds='${end_date}' 
				and
				pay_price in(39900,49900,9900,19900)
		)h2
		on h1.cck_uid=h2.cck_uid
		group by h1.invite_uid
	)t4
	on t1.cck_uid=t4.cck_uid
	left join
	(
	--->招募裂变
		select
			h1.invite_uid as cck_uid,
			count(h1.create_time) as new_cck_invite_count
		from
		(
			select
				invite_uid,
				cck_uid,
				create_time
			from
				origin_common.cc_ods_fs_wk_cct_layer_info
			where
				from_unixtime(create_time,'yyyyMMdd')>='${begin_date}' 
				and
				from_unixtime(create_time,'yyyyMMdd')<='${end_date}' 
				and
				platform=14
				and
				status = 1
		)h1
		inner join
		(
			select
				cck_uid
			from
				origin_common.cc_ods_fs_wk_cct_layer_info
			where
				from_unixtime(create_time,'yyyyMMdd')>='${begin_date}' 
				and
				from_unixtime(create_time,'yyyyMMdd')<='${end_date}' 
				and
				platform=14
				and
				status = 1
		) h2
		on h1.invite_uid = h2.cck_uid
		inner join
		(
			select
				cck_uid
			from
				origin_common.cc_ods_dwxk_fs_wk_business_info
			where
				ds='${end_date}' 
				and
				pay_price in(39900,49900,9900,19900)
		)h3
		on h1.cck_uid=h3.cck_uid
		group by h1.invite_uid
	)t7
	on t1.cck_uid=t7.cck_uid
	left join
	(
	--->分享
		select
			h2.cck_uid as cck_uid,
		    count(h1.user_id) as share_count
	    from
	    (
	    	select
		        user_id
		    from 
		        origin_common.cc_ods_log_cctapp_click_hourly
		    where 
		        ds>='${begin_date}' 
		        and
		        ds<='${end_date}' 
				and 
				module = 'detail_material' 
				and 
				zone in ('line','small_routine','pQrCode','promotion')
				and	
				source in ('cct','cctui')
		)h1
		left join
		(
			select
				cck_uid,
				cct_uid
			from
				origin_common.cc_ods_dwxk_fs_wk_cck_user
			where
				ds='${end_date}' 
				and
				platform=14
		)h2
		on h1.user_id=h2.cct_uid
		group by h2.cck_uid
	)t5
	on t1.cck_uid=t5.cck_uid
	left join 
	(
	--->新人开单
		select
			h1.cck_uid
		from 
		(
			select
				h1.cck_uid as cck_uid
			from
			(
				select
					cck_uid
				from
					origin_common.cc_ods_fs_wk_cct_layer_info
				where
					from_unixtime(create_time,'yyyyMMdd')>='${begin_date}' 
					and
					from_unixtime(create_time,'yyyyMMdd')<='${end_date}' 
					and
					platform=14
					and
					status = 1
			)h1
			inner join
			(
				select
					cck_uid
				from
					origin_common.cc_ods_dwxk_fs_wk_business_info
				where
					ds='${end_date}' 
					and
					pay_price in(39900,49900,9900,19900)
			)h3
			on h1.cck_uid=h3.cck_uid
		)h1
		inner join
		(
			select
				distinct
				h1.cck_uid as cck_uid
			from
			(
				select
					cck_uid,
					third_tradeno,
					item_price,
					cck_commission
				from
					origin_common.cc_ods_dwxk_wk_sales_deal_ctime
				where
					ds>='${begin_date}' 
					and
					ds<='${end_date}' 
			)h1
			inner join
			(
				select
					cck_uid
				from
					origin_common.cc_ods_dwxk_fs_wk_cck_user
				where
					ds='${end_date}' 
					and
					platform=14
			)h2
			on h1.cck_uid=h2.cck_uid
		)h2
		on h1.cck_uid=h2.cck_uid
	)t6
	on t1.cck_uid=t6.cck_uid
	group by
		t1.gm_uid
)p1
left join
(
	select
		cck_uid,
		real_name,
		phone
	from
		origin_common.cc_ods_dwxk_fs_wk_business_info
	where
		ds='${end_date}' 
)p2
on p1.gm_uid=p2.cck_uid
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
-- 小仙
-- 其他战区-服务经理下的销售经理id  销售经理团队的人数、销售、裂变 12、1、2月
select
	p1.gm_uid			as gm_uid,
	p1.leader_uid 		as leader_uid,
	p2.real_name 		as real_name,
	p2.phone 			as phone,
	p1.team_cck_count 	as team_cck_count,
	p1.cck_count 		as cck_count,
	COALESCE(p1.team_pay_count,0) 		as team_pay_count,
	COALESCE(p1.team_fee,0) 			as team_fee,
	COALESCE(p1.team_cck_commission,0) 	as team_cck_commission,
	COALESCE(p1.invite_count,0)   		as invite_count,
	COALESCE(p1.new_cck_invite_count,0) as new_cck_invite_count
from
(
	select
	    max(t1.gm_uid) 			as gm_uid,
	    t1.leader_uid 			as leader_uid,
	    count(t1.cck_uid) 		as team_cck_count,--销售经理团队人数
	    count(t2.cck_uid) 		as cck_count,--团队有销售人数
		sum(t2.pay_count) 		as team_pay_count,--订单
		sum(t2.fee) 			as team_fee,--支付金额
		sum(t2.cck_commission)  as team_cck_commission,--佣金
		sum(t4.invite_count)    as invite_count,--招募人数
		count(t7.cck_uid)       as new_cck_invite_count--新人中又有招募的人数
	from
	(
		select
			distinct
			gm_uid,
			leader_uid,
			cck_uid,
			0 as type
		from
			origin_common.cc_ods_fs_wk_cct_layer_info
		where
			gm_uid in (1548755,1165517,1956450,1165511,991237,288208,257958,486209,255274,1199349,288739,1343854,311118,1137220,423402,373219,1251821)
	        and
	        leader_uid !=0
	        and
	        platform=14
		union all
		select
			distinct
			gm_uid,
			leader_uid,
			leader_uid as cck_uid,
			1 as type
		from
			origin_common.cc_ods_fs_wk_cct_layer_info
		where
			gm_uid in (1548755,1165517,1956450,1165511,991237,288208,257958,486209,255274,1199349,288739,1343854,311118,1137220,423402,373219,1251821)
	        and
	        leader_uid !=0
	        and
	        platform=14
	)t1
	left join
	(
	--->总销售
		select
			h1.cck_uid as cck_uid,
			count(distinct h1.third_tradeno) as pay_count,
			sum(h1.item_price/100) as fee,
			sum(h1.cck_commission/100) as cck_commission
		from
		(
			select
				cck_uid,
				third_tradeno,
				item_price,
				cck_commission
			from
				origin_common.cc_ods_dwxk_wk_sales_deal_ctime
			where
				ds>='${begin_date}' 
			and
				ds<='${end_date}' 
		)h1
		inner join
		(
			select
				cck_uid
			from
				origin_common.cc_ods_dwxk_fs_wk_cck_user
			where
				ds='${end_date}' 
			and
				platform=14
		)h2
		on h1.cck_uid=h2.cck_uid
		group by h1.cck_uid
	)t2
	on t1.cck_uid=t2.cck_uid
	left join
	(
	--->招募
		select
			h1.invite_uid as cck_uid,
			count(h1.create_time) as invite_count
		from
		(
			select
				invite_uid,
				cck_uid,
				create_time
			from
				origin_common.cc_ods_fs_wk_cct_layer_info
			where
				from_unixtime(create_time,'yyyyMMdd')>='${begin_date}' 
				and
				from_unixtime(create_time,'yyyyMMdd')<='${end_date}' 
				and
				platform=14
				and
				status = 1
		)h1
		inner join
		(
			select
				cck_uid
			from
				origin_common.cc_ods_dwxk_fs_wk_business_info
			where
				ds='${end_date}' 
				and
				pay_price in(39900,49900,9900,19900)
		)h2
		on h1.cck_uid=h2.cck_uid
		group by h1.invite_uid
	)t4
	on t1.cck_uid=t4.cck_uid
	left join 
	(
	--->招募裂变
		select
			h1.invite_uid as cck_uid,--新人中又有招募的人
			count(h1.create_time) as new_cck_invite_count
		from
		(
			select
				invite_uid,
				cck_uid,
				create_time
			from
				origin_common.cc_ods_fs_wk_cct_layer_info
			where
				from_unixtime(create_time,'yyyyMMdd')>='${begin_date}' 
				and
				from_unixtime(create_time,'yyyyMMdd')<='${end_date}' 
				and
				platform=14
				and
				status = 1
		)h1
		inner join
		(
			select
				cck_uid
			from
				origin_common.cc_ods_fs_wk_cct_layer_info
			where
				from_unixtime(create_time,'yyyyMMdd')>='${begin_date}' 
				and
				from_unixtime(create_time,'yyyyMMdd')<='${end_date}' 
				and
				platform=14
				and
				status = 1
		) h2
		on h1.invite_uid = h2.cck_uid
		inner join
		(
			select
				cck_uid
			from
				origin_common.cc_ods_dwxk_fs_wk_business_info
			where
				ds='${end_date}' 
				and
				pay_price in(39900,49900,9900,19900)
		)h3
		on h1.cck_uid=h3.cck_uid
		group by h1.invite_uid
	)t7
	on t1.cck_uid=t7.cck_uid
	group by
		t1.leader_uid
)p1
left join
(
	select
		cck_uid,
		real_name,
		phone
	from
		origin_common.cc_ods_dwxk_fs_wk_business_info
	where
		ds=20190306
)p2
on p1.leader_uid=p2.cck_uid
////////////////////////////////////////////////////////////////
金泞佳 爆款奖励数据
select
    n1.product_id, 
    n1.cck_uid,
    n3.real_name,
    n3.phone,
    n1.third_tradeno,
    n1.sale_num,
    n1.cck_commission,
    n1.item_price,
    from_unixtime(n1.create_time,'yyyyMMdd HH:mm:ss') as create_time  
from
(
	select
	    t1.product_id, 
	    t1.cck_uid,
	    t1.third_tradeno,
	    t1.sale_num,
	    t1.cck_commission,
	    t1.item_price,
	    t1.create_time
    from
	(
	    select
	        s1.product_id as product_id, 
	        s1.cck_uid,
	        s1.third_tradeno,
	        s1.sale_num,
	        (s1.cck_commission/100) as cck_commission,
	        (s1.item_price/100) as item_price,
	        s1.create_time
	    from
	        origin_common.cc_ods_dwxk_wk_sales_deal_ctime s1
	    inner join 
	        origin_common.cc_ods_dwxk_fs_wk_cck_user s2
	    on 
	        s1.cck_uid = s2.cck_uid
	    where 
	        s1.ds='${begin_date}'
	    and
	    	s1.product_id = 
	    and
	        s2.ds='${begin_date}'
	    and
	        s2.platform = 14
	) t1
    left join 
    (
        select
            distinct
            order_sn
        from
            origin_common.cc_ods_fs_refund_order
        where 
            from_unixtime(create_time,'yyyyMMdd')>=20190223
        and
            status=1
    ) t2
    on t1.third_tradeno=t2.order_sn
    where 
        t2.order_sn is null 
) n1
left join 
(
    select
        distinct
        cck_uid,
        real_name,
        phone
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = 20190303
) n3
on n1.cck_uid = n3.cck_uid
////////////////////////////////////////////////////////////////////////////////////////////////
小仙 12.1-12.3 进来的VIP，
在12月、1和2月整月分别 登录APP唤醒率、有销售行为的、裂变率、开单率
select
	count(t1.cck_uid) as one_to_three_new_vip,
	count(t3.cck_uid) as login_num,
	count(t3.cck_uid)/count(t1.cck_uid) as wake_rate,
	count(t2.cck_uid) as saled_vip_num,
	count(t2.cck_uid)/count(t1.cck_uid) as sale_rate,
	count(t7.cck_uid) as fission_vip_num,
	count(t7.cck_uid)/count(t1.cck_uid) as ission_rate
from
(--1-3号新入住vip
	select
		distinct
		cck_uid
	from
		origin_common.cc_ods_fs_wk_cct_layer_info
	where
		from_unixtime(create_time,'yyyyMMdd')>='${begin_date1}' 
		and
		from_unixtime(create_time,'yyyyMMdd')<='${end_date1}' 
		and
		platform=14
		and
		status = 1
)t1
left join 
(--1-31号登陆app的所有vip
    select
        distinct 
        n2.cck_uid as cck_uid
    from 
    (
        select 
            cct_uid
        from 
            origin_common.cc_ods_log_gwapp_pv_hourly  
        where 
            ds >= '${begin_date2}' 
        and 
        	ds <= '${end_date2}'
        and
            module='https://app-h5.daweixinke.com/chuchutui/index.html' 
        and 
            cct_uid is not null 
        and 
            app_partner_id = 14
    )n1
    left join 
    (
        select
            cck_uid,
            cct_uid
        from 
            origin_common.cc_ods_fs_tui_relation
    )n2
    on n1.cct_uid = n2.cct_uid
) t3
on t1.cck_uid = t3.cck_uid
left join
(
--->1-31有销售(推广和自购)的vip
	select
        distinct 
		h1.cck_uid as cck_uid
	from
	(
		select
			cck_uid,
			third_tradeno,
			item_price,
			cck_commission
		from
			origin_common.cc_ods_dwxk_wk_sales_deal_ctime
		where
			ds>='${begin_date2}' 
		and
			ds<='${end_date2}' 
	)h1
	inner join
	(
		select
			cck_uid
		from
			origin_common.cc_ods_dwxk_fs_wk_cck_user
		where
			ds='${end_date2}' 
		and
			platform=14
	)h2
	on h1.cck_uid=h2.cck_uid
)t2
on t1.cck_uid=t2.cck_uid
left join
(
--->有招募裂变的新人vip
	select
		distinct
		h1.cck_uid as cck_uid
	from
	(
		select
			cck_uid
		from
			origin_common.cc_ods_fs_wk_cct_layer_info
		where
			from_unixtime(create_time,'yyyyMMdd')>='${begin_date1}' 
		and
			from_unixtime(create_time,'yyyyMMdd')<='${end_date1}' 
		and
			platform=14
		and
			status = 1
	)h1
	inner join
	(
		select
			invite_uid,
			cck_uid,
			create_time
		from
			origin_common.cc_ods_fs_wk_cct_layer_info
		where
			from_unixtime(create_time,'yyyyMMdd')>='${begin_date2}' 
		and
			from_unixtime(create_time,'yyyyMMdd')<='${end_date2}' 
		and
			platform=14
		and
			status = 1
	) h2
	on h1.cck_uid = h2.invite_uid
	inner join
	(
		select
			cck_uid
		from
			origin_common.cc_ods_dwxk_fs_wk_business_info
		where
			ds='${end_date2}' 
		and
			pay_price in(39900,49900,9900,19900)
	)h3
	on h2.cck_uid=h3.cck_uid
)t7
on t1.cck_uid=t7.cck_uid

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
王军军 2月销售经理数据
select
	p1.leader_uid 																		as leader_uid,
	p2.real_name 																		as real_name,
	p2.phone 																			as phone,
	p4.gm_uid 																			as gm_uid,
	p4.real_name 																		as real_name,
	p1.team_cck_count 																	as team_cck_count,
	p1.cck_count 																		as cck_count,
	p1.pay_count 																		as pay_count,
	p1.fee 																				as fee,
	p1.cck_commission 																	as cck_commission
from
(
	select
	    t1.leader_uid 												as leader_uid,
	    count(t1.cck_uid) 											as team_cck_count,
	    count(t2.cck_uid) 											as cck_count,
		sum(coalesce(t2.pay_count,0)) 		 						as pay_count,
		sum(coalesce(t2.fee,0)) 									as fee,
		sum(coalesce(t2.cck_commission,0)) 							as cck_commission
	from
	(
		select
			distinct
			leader_uid,
			cck_uid
		from
			origin_common.cc_ods_fs_wk_cct_layer_info
		where
			leader_uid !=0
	        and
	        platform=14
		union all
		select
			distinct
			leader_uid,
			leader_uid as cck_uid
		from
			origin_common.cc_ods_fs_wk_cct_layer_info
		where
			leader_uid !=0
	        and
	        platform=14
	)t1
	left join
	(
	--->总销售
		select
			h1.cck_uid as cck_uid,
			count(distinct h1.third_tradeno) as pay_count,
			sum(h1.item_price/100) as fee,
			sum(h1.cck_commission/100) as cck_commission
		from
		(
			select
				cck_uid,
				third_tradeno,
				item_price,
				cck_commission
			from
				origin_common.cc_ods_dwxk_wk_sales_deal_ctime
			where
				ds>='${begin_date}' 
				and
				ds<='${end_date}' 
		)h1
		join
		(
			select
				cck_uid
			from
				origin_common.cc_ods_dwxk_fs_wk_cck_user
			where
				ds='${end_date}' 
				and
				platform=14
		)h2
		on h1.cck_uid=h2.cck_uid
		group by h1.cck_uid
	)t2
	on t1.cck_uid=t2.cck_uid
	group by
		t1.leader_uid
)p1
left join
(
	select
		cck_uid,
		real_name,
		phone
	from
		origin_common.cc_ods_dwxk_fs_wk_business_info
	where
		ds='${end_date}' 
)p2
on p1.leader_uid=p2.cck_uid
left join
(
	select
		h1.cck_uid,
		h1.gm_uid,
		h2.real_name,
		h2.phone
	from
	(
	    select
	    	cck_uid,
	        gm_uid
	    from
	        origin_common.cc_ods_fs_wk_cct_layer_info
	    where
	        platform=14
	)h1
	left join
	(
	    select
	        cck_uid,
	        real_name,
	        phone
	    from
	        origin_common.cc_ods_dwxk_fs_wk_business_info
	    where
	        ds='${end_date}' 
	)h2
	on h1.gm_uid=h2.cck_uid
)p4
on p1.leader_uid=p4.cck_uid


//////////////////////////////////////////////////////////////////////////////
-- 张慧如
-- 活动名称：精选商品销售奖励
-- 活动对象：楚楚推所有VIP
-- 活动时间：2019年2月1日0:00—2月28日24:00
-- 活动奖励：销售五个精选商品    奖励100元
--         销售十个精选商品    奖励300元
--         销售二十个精选商品  奖励1000元
-- 活动商品：399元精选商品专区所有商品
select
	t1.invite_uidss as cck_uid,
	t3.cct_uid,
	t2.real_name,
	t2.phone,
	t1.invite_count
from 
(
	select
		invite_uidss,
	    count(distinct cck_uid) as invite_count
	from 
	    origin_common.cc_ods_fs_wk_cck_gifts 
	where 
	   from_unixtime(pay_time,'yyyyMMdd') >= 20190201
	and
	   from_unixtime(pay_time,'yyyyMMdd') <= 20190228
	and 
	    pay_status = 1
	and 
	    platform = 14
	and 
		type = 0
	and
		total_price = 39900
	group by
		invite_uidss
) t1
left join 
(
    select
        distinct
        cck_uid,
        real_name,
        phone
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = 20190305
)t2
on t1.invite_uidss=t2.cck_uid
left join 
(
    select
        distinct
        cck_uid,
        cct_uid
    from 
        origin_common.cc_ods_fs_tui_relation
)t3
on t1.invite_uidss = t3.cck_uid
where t1.invite_count>=5

//////////////////////////////////////////////////////////////////////////////
-- 张慧如
-- 商品名称：【直降299元！499得2套！】欧诗漫 珍珠水氧尊享套装
-- 商品ID：110020065126
-- 活动日期：2019年2月27日0:00——2月28日24:00
-- 奖励机制：(包含自购和推广)销量排序，销量相同，则到达5件时间越早，排在前面。
select
	t1.invite_uid as cck_uid,
	t3.cct_uid,
	t2.real_name,
	t2.phone,
	t1.sale_num,
	t1.fifth_order_pay_time
from 
(
	select
		s1.invite_uid,
		max(s1.sale_num) as sale_num,
		max(s1.fifth_order_pay_time) as fifth_order_pay_time
	from
	(
		select
			p1.invite_uid,
			p1.num,
			p1.pay_time,
			sum(p1.num) over(partition by p1.invite_uid order by p1.pay_time) as sale_num,
			sum(p1.fifth_order_pay_time) over(partition by p1.invite_uid order by p1.pay_time) as fifth_order_pay_time
		from
		(
			select
				h1.invite_uid,
				h1.num,
				h1.pay_time,
				if(h1.rank_num=5,h1.pay_time,0) as fifth_order_pay_time
			from 
			(
				select
					m1.invite_uid,
					m1.num,
					m1.pay_time,
					rank() over(partition by m1.invite_uid order by m1.pay_time) as rank_num
				from
				(
					select
						if(n1.type=0,n2.invite_uid,n1.cck_uid) as invite_uid,
						n1.num,
						n1.pay_time,
						n1.type
					from
					(
						select
						    cck_uid,
						    pay_time,
						    num,
						    type
						from 
						    origin_common.cc_ods_fs_wk_cck_gifts 
						where 
						   from_unixtime(pay_time,'yyyyMMdd')>=20190227
						and
						   from_unixtime(pay_time,'yyyyMMdd')<=20190228
						and 
						    pay_status = 1
						and 
						    platform = 14
						and 
							product_id=110020065126
						and
							type in (0,1)
					)n1
					left join 
					(
						select
							cck_uid,
							invite_uid
						from
							origin_common.cc_ods_fs_wk_cct_layer_info
					)n2
					on n1.cck_uid=n2.cck_uid
				)m1
			)h1
		) p1
	)s1
	group by s1.invite_uid
	having sale_num>=5
)t1
left join 
(
    select
        distinct
        cck_uid,
        real_name,
        phone
    from 
        origin_common.cc_ods_dwxk_fs_wk_business_info
    where 
        ds = 20190305
)t2
on t1.invite_uid=t2.cck_uid
left join 
(
    select
        distinct
        cck_uid,
        cct_uid
    from 
        origin_common.cc_ods_fs_tui_relation
)t3
on t1.invite_uid=t3.cck_uid

//////////////////////////////////////////////////////////////////////////////////
0117 礼包销售new(cc_ods_fs_wk_cck_gifts,type=0-入驻礼包,type=1-复购礼包,type=2-兑换礼包)
select
	distinct
	p1.invite_uidss,
    p2.real_name as real_name,
    p2.phone as phone,
    p4.delivery_address
from
(
    select
        distinct
		invite_uidss
    from 
        origin_common.cc_ods_fs_wk_cck_gifts 
    where 
        from_unixtime(create_time,'yyyyMMdd') >= 20190215
    	and
        from_unixtime(create_time,'yyyyMMdd') <= 20190217
    	and 
    	type=0
    	and
        pay_status = 1
    	and 
        platform = 14
)p1
left join
(
	select
		cck_uid,
		real_name,
		phone,
		third_pay_sn
	from
		origin_common.cc_ods_dwxk_fs_wk_business_info
	where
		ds=20190218
)p2
on p1.invite_uidss=p2.cck_uid
left join 
(
	select
		distinct
		trade_no,
		order_sn
	from
		data.cc_dm_paysystem_pay_order_log_apart
)p3
on p2.third_pay_sn=p3.trade_no
left join 
(
	select
		distinct
		order_sn,
		delivery_address
	from
		origin_common.cc_order_user_delivery_time
)p4
on p3.order_sn=p4.order_sn

//////////////////////////////////////////////////////////////////////////////////
董小仙 现有一个社群需求是 0215-0217的所有礼包订单 区分出自买 推广
select
    p1.cck_uid,
    p1.type,
	p1.num,
	p1.third_pay_sn,
	p2.order_sn
from 
(
	select
		distinct
	    cck_uid,
	    type,
	    num,
	    third_pay_sn
	from 
	    origin_common.cc_ods_fs_wk_cck_gifts 
	where 
	    from_unixtime(create_time,'yyyyMMdd') >= 20190215
		and
	    from_unixtime(create_time,'yyyyMMdd') <= 20190217
		and 
	    pay_status = 1
		and 
	    platform = 14
)p1
left join 
(
	select
		distinct
		trade_no,
		order_sn
	from
		data.cc_dm_paysystem_pay_order_log_apart
	where
		ds>=20190210
	and
		ds<=20190218 
)p2
on p1.third_pay_sn=p2.order_sn
where p1.type=0
////////////////////////////////////////////////////////////////////////////
金泞佳 种草某心得ID user_id phone
select
	*
from 
	co_inspection_result --线上4045表
where
	from_unixtime(created_at,'yyyyMMdd') = 20190307
and
	id in (45955,45759)	
/////////////////////////////////////////////////////////
###社群周数据
###1209 服务经理，各战区销售、推广、招募数据，名单已更新0123
select
	p1.gm_uid 																			as gm_uid,
	p2.real_name 																		as real_name,
	p2.phone 																			as phone,
	p3.hatch_uid																		as hatch_uid,
	(
	    case
		    when gm_uid in(376987,614766,1068772,684796,946772,612648,574693,330186,540504,769441,541262,678663,353276,589882,997954,446253,351503,263970,353349,919648,1088215,384542,985037,384261,1107104,446707,1086499,850095,537969,411164) then '一战区'
		    when gm_uid in(939962,459877,1117720,575222,255478,453987,710973,537784,240474,350755,520269,360184,240561,577979,620072,1018891,242804,419227,346058,1102429,1229407,960479,254945,285408,289429,266283,958082,1017197,1008462,1031618,998298,924852) then '二战区'  
		    when gm_uid in(815637,770362,1034247,1035670,1151558,760397,721563,1016400,1017792,1049908,744474,720760,930631,901929,1175069,1034136,1001493,1175203,736736,909658,747406,1053184,763931,1025605,1101858,1031405,1002362,709705,1050353,1182618,1254291,1143126) then '四战区'
		    when gm_uid in(1241239,1276826,1256190,1201128,1199956,1202494,1199210,1419045,1200412,1257213,1211271,1209138,1201288,1204461,1240632,1242477,1252167,1199321,1240633,1201979,1199214,1241385,1209314,1257637,1199305,1204049,1200483,1205821,1199515,1204007,1199985,1210498,1197475,1199621,1200648,1200608,1199749,1224204,1227974,1199168,1199365,1199978,1240629,1201275,1199635,1350622,1202058,1224242) then '三战区'
		    when gm_uid in(289477,1245487,1199257,443123,493355,976562,245919,325059,278330,958082,532027,547890,718749,293459,950800,240461,507451,1022549,243168,402913,285968,344364,240087,1317551,1012885,273330,244360,1276634,1407703,1317590,1446199,461541,475906) then '五战区'
		    when gm_uid in(493355,877172,1406113,1342969,1307523,1368362,1318636,1307543,1481982,1052413) then '七战区'
		    when gm_uid in(261756,239746,989050,227880,942532,787686,283867,1216371,949822,399316,375083,1312806,995759,1586109,961545,1140146,551805,980086,940677,430297,280411,501015,985271,471845,1068651,416797,1225213,931097,664703,944769,1113907,334274,569834,692018,573196,497607,1022418,1133012,1004655,415062,240863,1156866,790020,455813,472959,278849,831619,816449,1157510,328580,863183,1166918,521741,850095,1067330,1198132,777208,872252,1169977,1481021,1182176,989201,602463,232877,967935,355368,498739,1029483,1038882,497521,730130,581807,861298,623628,268750,268078,452886) then '九战区'
		    else '其它战区'
	    end
    ) as team,
	p1.team_cck_count 																	as team_cck_count,--团队人数
	p1.cck_count 																		as cck_count,--有销售人数
	p1.pay_count 																		as pay_count,
	p1.fee 																				as fee,
	p1.cck_commission 																	as cck_commission,
	p1.pro_cck_count 																	as pro_cck_count,--有推广成交的人数
	p1.pro_pay_count 																	as pro_pay_count,
	p1.pro_fee 																	    	as pro_fee,
	p1.pro_cck_commission 												            	as pro_cck_commission,
	p1.invite_cck_count 														    	as invite_cck_count,
	p1.invite_count 															    	as invite_count,
	p1.share_cck_count 															    	as share_cck_count,--有分享的人数
	p1.share_count 																   		as share_count,
	if(p1.share_cck_count=0,0,p1.share_count/p1.share_cck_count) as ave_share_count,--平均每人分享次数
	(p1.share_cck_count/p1.team_cck_count*100) as share_cck_per,--团队有分享占比
	(p1.pro_cck_count/p1.team_cck_count*100) as pro_cck_count_per, --团队有推广占比
	(p1.invite_cck_count/p1.team_cck_count*100) as invite_cck_per,--团队有招募占比
	p1.new_cck_invite_count as new_cck_invite_count,--新人中又有招募的人数
	if(p1.invite_count=0,0,p1.new_cck_invite_count/p1.invite_count*100) as division_rate,--新人裂变率
	p1.new_cck_buy_count as new_cck_buy_count,--新人开单人数
	if(p1.invite_count=0,0,p1.new_cck_buy_count/p1.invite_count*100) as new_cck_buyer_rate--新人开单率
from
(
	select
	    t1.gm_uid 													as gm_uid,
	    count(t1.cck_uid) 											as team_cck_count,
	    count(t2.cck_uid) 											as cck_count,
		sum(coalesce(t2.pay_count,0)) 		 						as pay_count,
		sum(coalesce(t2.fee,0)) 									as fee,
		sum(coalesce(t2.cck_commission,0)) 							as cck_commission,
	    count(t3.cck_uid) 											as pro_cck_count,
		sum(coalesce(t3.pay_count,0)) 								as pro_pay_count,
		sum(coalesce(t3.fee,0))										as pro_fee,
		sum(coalesce(t3.cck_commission,0)) 							as pro_cck_commission,		
		count(t4.cck_uid)		   								    as invite_cck_count,--团队有招募人数
		sum(coalesce(t4.invite_count,0)) 							as invite_count,--团队招募新人书
		count(t5.cck_uid) 											as share_cck_count,--团队有分享人数
		sum(coalesce(t5.share_count,0))								as share_count,--团队分享次数
		count(t6.cck_uid) as new_cck_invite_count,--新人中又有招募的人数
		count(t7.cck_uid) as new_cck_buy_count--新人开单人数
	from
	(
		select
			distinct
			gm_uid,
			cck_uid
		from
			origin_common.cc_ods_fs_wk_cct_layer_info
		where
			gm_uid !=0
	        and
	        platform=14
		union all
		select
			distinct
			gm_uid,
			gm_uid as cck_uid
		from
			origin_common.cc_ods_fs_wk_cct_layer_info
		where
			gm_uid !=0
	        and
	        platform=14
	)t1
	left join
	(
	--->总销售
		select
			h1.cck_uid as cck_uid,
			count(distinct h1.third_tradeno) as pay_count,
			sum(h1.item_price/100) as fee,
			sum(h1.cck_commission/100) as cck_commission
		from
		(
			select
				cck_uid,
				third_tradeno,
				item_price,
				cck_commission
			from
				origin_common.cc_ods_dwxk_wk_sales_deal_ctime
			where
				ds>='${begin_date}' 
				and
				ds<='${end_date}' 
		)h1
		join
		(
			select
				cck_uid
			from
				origin_common.cc_ods_dwxk_fs_wk_cck_user
			where
				ds='${end_date}' 
				and
				platform=14
		)h2
		on h1.cck_uid=h2.cck_uid
		group by h1.cck_uid
	)t2
	on t1.cck_uid=t2.cck_uid
	left join
	(
	--->推广销售
		select
			h1.cck_uid as cck_uid,
			count(distinct h1.third_tradeno) as pay_count,
			sum(h1.item_price/100) as fee,
			sum(h1.cck_commission/100) as cck_commission
		from
		(
			select
				cck_uid,
				third_tradeno,
				item_price,
				cck_commission
			from
				origin_common.cc_ods_dwxk_wk_sales_deal_ctime
			where
				ds>='${begin_date}' 
				and
				ds<='${end_date}' 
		)h1
		join
		(
			select
				cck_uid
			from
				origin_common.cc_ods_dwxk_fs_wk_cck_user
			where
				ds='${end_date}' 
				and
				platform=14
		)h2
		on h1.cck_uid=h2.cck_uid
		left join
		(
			select
				order_sn,
				source
			from
				origin_common.cc_ods_log_gwapp_order_track_hourly
			where
				ds>='${begin_date}' 
				and
				ds<='${end_date}' 
		)h3
		on h1.third_tradeno=h3.order_sn
		where h3.source != 'cctui'
		group by h1.cck_uid
	)t3
	on t1.cck_uid=t3.cck_uid
	left join
	(
	--->招募
		select
			h1.invite_uid as cck_uid,
			count(h1.create_time) as invite_count
		from
		(
			select
				invite_uid,
				cck_uid,
				create_time
			from
				origin_common.cc_ods_fs_wk_cct_layer_info
			where
				from_unixtime(create_time,'yyyyMMdd')>='${begin_date}' 
				and
				from_unixtime(create_time,'yyyyMMdd')<='${end_date}' 
				and
				platform=14
		)h1
		inner join
		(
			select
				cck_uid
			from
				origin_common.cc_ods_dwxk_fs_wk_business_info
			where
				ds='${end_date}' 
				and
				pay_price in(39900,49900,9900,19900)
		)h2
		on h1.cck_uid=h2.cck_uid
		group by h1.invite_uid
	)t4
	on t1.cck_uid=t4.cck_uid
	left join
	(
	--->分享
		select
			h2.cck_uid as cck_uid,
		    count(h1.user_id) as share_count
	    from
	    (
	    	select
		        user_id
		    from 
		        origin_common.cc_ods_log_cctapp_click_hourly
		    where 
		        ds>='${begin_date}' 
		        and
		        ds<='${end_date}' 
				and 
				module = 'detail_material' 
				and 
				zone in ('line','small_routine','pQrCode','promotion')
				and	
				source in ('cct','cctui')
		)h1
		left join
		(
			select
				cck_uid,
				cct_uid
			from
				origin_common.cc_ods_dwxk_fs_wk_cck_user
			where
				ds='${end_date}' 
				and
				platform=14
		)h2
		on h1.user_id=h2.cct_uid
		group by h2.cck_uid
	)t5
	on t1.cck_uid=t5.cck_uid
	left join
	(
	--->招募裂变
		select
			h1.invite_uid as cck_uid,
			count(h1.create_time) as new_cck_invite_num
		from
		(
			select
				invite_uid,
				cck_uid,
				create_time
			from
				origin_common.cc_ods_fs_wk_cct_layer_info
			where
				from_unixtime(create_time,'yyyyMMdd')>='${begin_date}' 
				and
				from_unixtime(create_time,'yyyyMMdd')<='${end_date}' 
				and
				platform=14
		)h1
		inner join
		(
			select
				cck_uid
			from
				origin_common.cc_ods_fs_wk_cct_layer_info
			where
				from_unixtime(create_time,'yyyyMMdd')>='${begin_date}' 
				and
				from_unixtime(create_time,'yyyyMMdd')<='${end_date}' 
				and
				platform=14
		) h2
		on h1.invite_uid = h2.cck_uid
		inner join
		(
			select
				cck_uid
			from
				origin_common.cc_ods_dwxk_fs_wk_business_info
			where
				ds='${end_date}' 
				and
				pay_price in(39900,49900,9900,19900)
		)h3
		on h1.cck_uid=h3.cck_uid
		group by h1.invite_uid
	)t6
	on t1.cck_uid=t6.cck_uid
	left join
	(
	--->新人开单
		select
			h1.cck_uid
		from 
		(
			select
				h1.cck_uid as cck_uid
			from
			(
				select
					cck_uid
				from
					origin_common.cc_ods_fs_wk_cct_layer_info
				where
					from_unixtime(create_time,'yyyyMMdd')>='${begin_date}' 
					and
					from_unixtime(create_time,'yyyyMMdd')<='${end_date}' 
					and
					platform=14
			)h1
			inner join
			(
				select
					cck_uid
				from
					origin_common.cc_ods_dwxk_fs_wk_business_info
				where
					ds='${end_date}' 
					and
					pay_price in(39900,49900,9900,19900)
			)h3
			on h1.cck_uid=h3.cck_uid
		)h1
		inner join
		(
			select
				distinct
				h1.cck_uid as cck_uid
			from
			(
				select
					cck_uid,
					third_tradeno,
					item_price,
					cck_commission
				from
					origin_common.cc_ods_dwxk_wk_sales_deal_ctime
				where
					ds>='${begin_date}' 
					and
					ds<='${end_date}' 
			)h1
			inner join
			(
				select
					cck_uid
				from
					origin_common.cc_ods_dwxk_fs_wk_cck_user
				where
					ds='${end_date}' 
					and
					platform=14
			)h2
			on h1.cck_uid=h2.cck_uid
		)h2
		on h1.cck_uid=h2.cck_uid
	)t7
	on t1.cck_uid=t7.cck_uid
	group by
		t1.gm_uid
)p1
left join
(
	select
		cck_uid,
		real_name,
		phone
	from
		origin_common.cc_ods_dwxk_fs_wk_business_info
	where
		ds='${end_date}' 
)p2
on p1.gm_uid=p2.cck_uid
left join
(
    select
        cck_uid,
        hatch_uid
    from
        origin_common.cc_ods_fs_wk_cct_layer_info
    where
        platform=14
)p3
on p1.gm_uid=p3.cck_uid

////////////////////////////////////////////////////////////////////////////////////////////////
-- 董小仙 近三个月有订单的vip
-- 需要的字段:id,姓名，手机号 前80000
select
	p1.cck_uid,
	p2.real_name,
	p2.phone,
	p1.pay_count as pay_count,
	p1.fee as fee,
	p1.cck_commission as cck_commission
from
(
	select
		h1.cck_uid as cck_uid,
		count(distinct h1.third_tradeno) as pay_count,
		sum(h1.item_price/100) as fee,
		sum(h1.cck_commission/100) as cck_commission
	from
	(
		select
			cck_uid,
			third_tradeno,
			item_price,
			cck_commission
		from
			origin_common.cc_ods_dwxk_wk_sales_deal_ctime
		where
			ds>='${begin_date}' 
			and
			ds<='${end_date}' 
	)h1
	inner join
	(
		select
			cck_uid
		from
			origin_common.cc_ods_dwxk_fs_wk_cck_user
		where
			ds='${end_date}' 
			and
			platform=14
	)h2
	on h1.cck_uid=h2.cck_uid
	group by h1.cck_uid
)p1
left join 
(
	select
		cck_uid,
		real_name,
		phone
	from
		origin_common.cc_ods_dwxk_fs_wk_business_info
	where
		ds='${end_date}' 
)p2
on p1.cck_uid=p2.cck_uid
order by p1.cck_uid 
limit 80000
////////////////////////////////////////////////////////////////////
-- 后 48776
select
	p1.cck_uid,
	p2.real_name,
	p2.phone,
	p1.pay_count as pay_count,
	p1.fee as fee,
	p1.cck_commission as cck_commission
from
(
	select
		h1.cck_uid as cck_uid,
		count(distinct h1.third_tradeno) as pay_count,
		sum(h1.item_price/100) as fee,
		sum(h1.cck_commission/100) as cck_commission
	from
	(
		select
			cck_uid,
			third_tradeno,
			item_price,
			cck_commission
		from
			origin_common.cc_ods_dwxk_wk_sales_deal_ctime
		where
			ds>='${begin_date}' 
			and
			ds<='${end_date}' 
	)h1
	inner join
	(
		select
			cck_uid
		from
			origin_common.cc_ods_dwxk_fs_wk_cck_user
		where
			ds='${end_date}' 
			and
			platform=14
	)h2
	on h1.cck_uid=h2.cck_uid
	group by h1.cck_uid
)p1
left join 
(
	select
		cck_uid,
		real_name,
		phone
	from
		origin_common.cc_ods_dwxk_fs_wk_business_info
	where
		ds='${end_date}' 
)p2
on p1.cck_uid=p2.cck_uid
order by p1.cck_uid desc 
limit 48776
//////////////////////////////////////////////////////////////////////////////////////////////////////
线上查询不出来
select
	t1.cck_uid,
	t3.real_name,
	t2.phone_number,
	t1.pay_count,
	t1.fee
from
(
	select
		cck_uid,
		count(distinct third_tradeno) as pay_count,
		sum(item_price/100) as fee
	from
		wk_sales_deal
	where
		create_time>=1544630400
	and
		create_time<=1552406400
	group by
		cck_uid
)t1
join
(
	select
		cck_uid,
		cct_uid,
		phone_number
	from
		wk_cck_user
	where
		platform=14
)t2
on t1.cck_uid =t2.cck_uid
left join
(
	select
		cck_uid,
		real_name
	from
		wk_business_info
	union all
	select
		cck_uid,
		real_name
	from
		wk_region_user
)t3
on t1.cck_uid=t3.cck_uid
////////////////////////////////////////////////////////////////////////////////////////////////////////////
-- 董小仙
-- A，截止2019年1月底，育成5个服务经理
-- B，2018年团队销售额突破400W
-- 满足以上两个条件的服务经理
select
    t1.gm_uid 													as gm_uid,
	sum(coalesce(t2.pay_count,0)) 		 						as pay_count,
	sum(coalesce(t2.fee,0)) 									as fee,
	sum(coalesce(t2.cck_commission,0)) 							as cck_commission
from
(
	select
		distinct
		gm_uid,
		cck_uid
	from
		origin_common.cc_ods_fs_wk_cct_layer_info
	where
		gm_uid !=0
        and
        platform=14
	union all
	select
		distinct
		gm_uid,
		gm_uid as cck_uid
	from
		origin_common.cc_ods_fs_wk_cct_layer_info
	where
		gm_uid !=0
        and
        platform=14
)t1
left join
(
--->总销售
	select
		h1.cck_uid as cck_uid,
		count(distinct h1.third_tradeno) as pay_count,
		sum(h1.item_price/100) as fee,
		sum(h1.cck_commission/100) as cck_commission
	from
	(
		select
			cck_uid,
			third_tradeno,
			item_price,
			cck_commission
		from
			origin_common.cc_ods_dwxk_wk_sales_deal_ctime
		where
			ds>='${begin_date}' 
		and
			ds<='${end_date}' 
	)h1
	inner join
	(
		select
			cck_uid
		from
			origin_common.cc_ods_dwxk_fs_wk_cck_user
		where
			ds='${end_date}' 
		and
			platform=14
	)h2
	on h1.cck_uid=h2.cck_uid
	group by h1.cck_uid
)t2
on t1.cck_uid=t2.cck_uid
group by t1.gm_uid
having fee>=4000000
//////////////////////////////////////////////////////
孵化数据
select
	hatch_uid,
	count(distinct cck_uid) as num 
from
	origin_common.cc_ods_fs_wk_cct_layer_info
where
    platform=14
and
	type = 2
and
	create_time<=1548864000
and 
	hatch_uid in (240474,263970,240461,710973,430297,863183,1406113,1199168,532027)
group by 
	hatch_uid
/////////////////////////////////////////////////////////////
马竞译 查看1696816她的邀请人数量
select
	count(distinct cck_uid) as num 
from
	origin_common.cc_ods_fs_wk_cct_layer_info
where
    platform=14
and
	invite_uid = 1696816
/////////////////////////////////////////////////////////////
马竞译 查看1696816她的自购数据
-- 注意这里发现 她的入驻时购买的礼包 type=3,支付金额=99元
--另外她11.14自购一次399礼包

select
	*
from 
    origin_common.cc_ods_fs_wk_cck_gifts 
where 
    pay_status = 1
and 
    platform = 14
and
	cck_uid = 1696816
////////////////////////////////////////////////////////////////////
马竞译 查看1696816她的邀请数据
select
	t1.invite_uidss,
	t3.real_name,
	t3.phone,
	t1.cck_uid,
	t2.real_name,
	t1.product_id,
	t1.total_price,
	t1.pay_time
from
(
	select
		invite_uidss,
		cck_uid,
		product_id,
		total_price,
		pay_time
	from 
	    origin_common.cc_ods_fs_wk_cck_gifts 
	where 
	    pay_status = 1
	and 
	    platform = 14
	and
		invite_uidss = 1696816
)t1
left join 
(
	select
		cck_uid,
		real_name,
		phone
	from
		origin_common.cc_ods_dwxk_fs_wk_business_info
	where
		ds='${end_date}' 

)t2
on t1.cck_uid=t2.cck_uid
left join
(
	select
		cck_uid,
		real_name,
		phone
	from
		origin_common.cc_ods_dwxk_fs_wk_business_info
	where
		ds='${end_date}' 
)t3
on t1.invite_uidss=t3.cck_uid
////////////////////////////////////////////////////////////////////
马竞译 查看1696816她团队的邀请数据
select
	*
from 
    origin_common.cc_ods_fs_wk_cck_gifts 
where 
	invite_uidss in (1919426,1723096,1908560,1855896,1922158,1956572,1928630,1917976,1935870,1945596,1942278,1946814,1961322,1989070,1987276,1858080,1988532,1934088,1983490,1983516,1933982,1922504,1937928,1942432,1935672,1908608,1932694,1935776,1943754,1985420,1909510,1935210,1953272,1902308,1983544,1734538,1957174,1945700,1995170,1927426,1906024,1898948,1929902,2111608,1918080,1926694,1901258,1936082,1929140,1931622,1928732,2054356,1956842,1999946,1881784,1722646,1907320,1911346,1743396,1904534,1922072,1952248,1939186,1926072,1951658,1936338,1846444,1898644,1849110,1935750,1906898,1929386,1817908,1916306,1733958,1928224,1944718,1849776,1885160,1910298,1907004,1947260,1923254,1929034,1850532,1930246,1950844,1940440,1916663,1843947,2105947,1905989,1959303,1909641,1917879,1915907,1987891,1732879,1918405,1885165,1857407,1854177,1905405,1904117,1994931,1852365,2112151,1918367,1936705,1847329,1862663,1935719,1929233,1842721,1962029,1909275,1932543,1732947,1909695,1940923,1949053,1914529,1934535,1828855,1936059,1915929,1935677,1989301,1905635,1734563,1907235,2104179,2111981,2003737,1987981,1991419,2106555,1938147,1938739,1882499,1961117,1942315,1936261,1924369,1920417,1834257,2000245,1981959,1925647,1925605,1828699,1935707,1835627,1929965,1932249,1696816)
or 
	cck_uid in (1919426,1723096,1908560,1855896,1922158,1956572,1928630,1917976,1935870,1945596,1942278,1946814,1961322,1989070,1987276,1858080,1988532,1934088,1983490,1983516,1933982,1922504,1937928,1942432,1935672,1908608,1932694,1935776,1943754,1985420,1909510,1935210,1953272,1902308,1983544,1734538,1957174,1945700,1995170,1927426,1906024,1898948,1929902,2111608,1918080,1926694,1901258,1936082,1929140,1931622,1928732,2054356,1956842,1999946,1881784,1722646,1907320,1911346,1743396,1904534,1922072,1952248,1939186,1926072,1951658,1936338,1846444,1898644,1849110,1935750,1906898,1929386,1817908,1916306,1733958,1928224,1944718,1849776,1885160,1910298,1907004,1947260,1923254,1929034,1850532,1930246,1950844,1940440,1916663,1843947,2105947,1905989,1959303,1909641,1917879,1915907,1987891,1732879,1918405,1885165,1857407,1854177,1905405,1904117,1994931,1852365,2112151,1918367,1936705,1847329,1862663,1935719,1929233,1842721,1962029,1909275,1932543,1732947,1909695,1940923,1949053,1914529,1934535,1828855,1936059,1915929,1935677,1989301,1905635,1734563,1907235,2104179,2111981,2003737,1987981,1991419,2106555,1938147,1938739,1882499,1961117,1942315,1936261,1924369,1920417,1834257,2000245,1981959,1925647,1925605,1828699,1935707,1835627,1929965,1932249,1696816)

//////////////////////////////////////////////////////




























