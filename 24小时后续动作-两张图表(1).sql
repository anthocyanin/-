///////////////////////////////////////////
后续购买
(select 
	s1.pay_id 
from
	(
        select 
			product_id,
			uid as pay_id,
			create_time
		from
			origin_common.cc_ods_dwxk_wk_sales_deal_ctime
		where 
			product_id in (110019405576,110019405615,110019405627,110019405628,110019405632)
		and 
			from_unixtime(create_time,'yyyyMMdd') >='${begin_date}'
		and 
		    from_unixtime(create_time,'yyyyMMdd') <='${end_date}'
	) s1
inner join
	(
				select 
					uid,
					create_time
				from 
					origin_common.cc_ods_dwxk_wk_sales_deal_ctime
	)s2
on 
	s1.pay_id=s2.uid 
where 
	s2.create_time-s1.create_time<=86400
);
//////////////////////////////////////////////////////////////////////////////
后续分享
(select 
	s1.pay_id,
	count(distinct pay_id) as houxu_pay
from
	(
        select 
			product_id,
			uid as pay_id,
			create_time
		from
			origin_common.cc_ods_dwxk_wk_sales_deal_ctime
		where 
			product_id in (110019405576,110019405615,110019405627,110019405628,110019405632)
		and 
			from_unixtime(create_time,'yyyyMMdd') >='${begin_date}'
		and 
		    from_unixtime(create_time,'yyyyMMdd') <='${end_date}'
	) s1
inner join
    (
    	
	    select
			user_id as share_id,
			ad_id,
		    timestamp
		from 
			origin_common.cc_ods_log_cctapp_click_hourly
		where 
			module in
			(
				'login','index_new_share','red_packet','help_farmers','self-support','Netease-recommend','Netease-home','Netease-

				diet','Netease-baby','Netease-electric','insurance','insurance_detail','vip-materia','vip-materia-circle-friend','materia-circle-

				friend-goods','materia-circle-friend-material','materia-circle-friend-new','vip-invite-download','vip-

				invite','vip_shop','coin_share','coin_share_complete','detail_app','detail_material','command_detail',
				'command_circle_of_friends','command_activity','task-share','task-first-hotgeneralize','task-firstshare-friendcircle','task-first-

				generalize-success','task-firstfriend-download','task-firstfriend-VIP','task-invite-friend','task-hotgeneralize','task-share-

				friendcircle','task-call-friends','task-friend-download'
			)
		and 
			zone in 
			(
			'successfully','goods_share','share','share_float','friend','circle-friend','copy','spread','promotion','paste','share-

			immediately','get-currency','share_friend','circlefriend'
			)
		
	) as s2
on 
	s1.pay_id=s2.share_id
where 
s2.timestamp-s1.create_time <=86400) n3
;
////////////////////////////////////////////////////
购买者
select 
	uid as pay_id
from
	origin_common.cc_ods_dwxk_wk_sales_deal_ctime
where 
	product_id in (110019405576,110019405615,110019405627,110019405628,110019405632)
and 
	from_unixtime(create_time,'yyyyMMdd') >='${begin_date}'
and 
	from_unixtime(create_time,'yyyyMMdd') <='${end_date}'
/////////////////////////////////////////////////////////////////////////////////////
购买者极其24小时内的后续动作组合-------------------普通用户 （userID） 改成楚客id
select 
	n1.pay_id,
	count(n2.houxu_pay) as houxu_pay_num,
	count(n3.houxu_share_id) as houxu_share_num
from 
(
	select 
		distinct 
		uid as pay_id
	from
		origin_common.cc_ods_dwxk_wk_sales_deal_ctime
	where 
		product_id in (110019405576,110019405615,110019405627,110019405628,110019405632)
	and 
		from_unixtime(create_time,'yyyyMMdd') >='${begin_date}'
	and 
		from_unixtime(create_time,'yyyyMMdd') <='${end_date}'
) as n1
left join
(
   	select 
		s1.pay_id as houxu_pay
	from
	(
        select 
			uid as pay_id,
			min(create_time) as first_time
		from
			origin_common.cc_ods_dwxk_wk_sales_deal_ctime
		where 
			product_id in (110019405576,110019405615,110019405627,110019405628,110019405632)
		and 
			from_unixtime(create_time,'yyyyMMdd') >='${begin_date}'
		and 
		    from_unixtime(create_time,'yyyyMMdd') <='${end_date}'
		group by 
			uid
	) s1
	inner join
	(
		select 
			uid,
			create_time
		from 
			origin_common.cc_ods_dwxk_wk_sales_deal_ctime
	)s2
	on 
		s1.pay_id=s2.uid 
	where 
		(s2.create_time-s1.create_time)>0 
	and 
		s2.create_time-s1.create_time>=0 and s2.create_time-s1.create_time<=86400
) as n2
on n1.pay_id = n2.houxu_pay
left join
(
	select 
		s1.pay_id as houxu_share_id
	from
	(
        select 
			product_id,
			uid as pay_id,
			create_time
		from
			origin_common.cc_ods_dwxk_wk_sales_deal_ctime
		where 
			product_id in (110019405576,110019405615,110019405627,110019405628,110019405632)
		and 
			from_unixtime(create_time,'yyyyMMdd') >='${begin_date}'
		and 
		    from_unixtime(create_time,'yyyyMMdd') <='${end_date}'
	) s1
	inner join
    (

	    select
			user_id as share_id,
			ad_id,
		    timestamp
		from 
			origin_common.cc_ods_log_cctapp_click_hourly
		where 
			module in
			(
				'login','index_new_share','red_packet','help_farmers','self-support','Netease-recommend','Netease-home','Netease-

				diet','Netease-baby','Netease-electric','insurance','insurance_detail','vip-materia','vip-materia-circle-friend','materia-circle-

				friend-goods','materia-circle-friend-material','materia-circle-friend-new','vip-invite-download','vip-

				invite','vip_shop','coin_share','coin_share_complete','detail_app','detail_material','command_detail',
				'command_circle_of_friends','command_activity','task-share','task-first-hotgeneralize','task-firstshare-friendcircle','task-first-

				generalize-success','task-firstfriend-download','task-firstfriend-VIP','task-invite-friend','task-hotgeneralize','task-share-

				friendcircle','task-call-friends','task-friend-download'
			)
		and 
			zone in 
			(
				'successfully','goods_share','share','share_float','friend','circle-friend','copy','spread','promotion','paste','share-

				immediately','get-currency','share_friend','circlefriend'
			)

	) as s2
	on 
		s1.pay_id=s2.share_id
	where 
		s2.timestamp-s1.create_time <=86400
) as n3
on n1.pay_id = n3.houxu_share_id
group by 
	n1.pay_id
;
////////////////////////////////////////////////////////////////////////////////////////
分享者极其24小时内购买  
select 
	s1.share_id,
	count(s2.share_id)
from 
	(
		select
			user_id as share_id
		from 
			origin_common.cc_ods_log_cctapp_click_hourly
		where 
			module in
			(
			'login','index_new_share','red_packet','help_farmers','self-support','Netease-recommend','Netease-home','Netease-

			diet','Netease-baby','Netease-electric','insurance','insurance_detail','vip-materia','vip-materia-circle-friend','materia-circle-

			friend-goods','materia-circle-friend-material','materia-circle-friend-new','vip-invite-download','vip-

			invite','vip_shop','coin_share','coin_share_complete','detail_app','detail_material','command_detail',
			'command_circle_of_friends','command_activity','task-share','task-first-hotgeneralize','task-firstshare-friendcircle','task-first-

			generalize-success','task-firstfriend-download','task-firstfriend-VIP','task-invite-friend','task-hotgeneralize','task-share-

			friendcircle','task-call-friends','task-friend-download'
			)
		and 
			zone in 
			(
			'successfully','goods_share','share','share_float','friend','circle-friend','copy','spread','promotion','paste','share-

			immediately','get-currency','share_friend','circlefriend'
			)
	) s1
left join
	(
		select 
			p0.share_id
		from 
		    (
        select 
			n2.product_id,
			n1.share_id,
			timestamp
		from
			(
				select
				    user_id as share_id,
				    ad_id,
				    timestamp
				from 
					origin_common.cc_ods_log_cctapp_click_hourly
				where 
					ds >= '${begin_date}'
				and  
					ds <='${end_date}'
				and 
					module in
						(
						'login','index_new_share','red_packet','help_farmers','self-support','Netease-recommend','Netease-home','Netease-

						diet','Netease-baby','Netease-electric','insurance','insurance_detail','vip-materia','vip-materia-circle-friend','materia-circle-

						friend-goods','materia-circle-friend-material','materia-circle-friend-new','vip-invite-download','vip-

						invite','vip_shop','coin_share','coin_share_complete','detail_app','detail_material','command_detail',
						'command_circle_of_friends','command_activity','task-share','task-first-hotgeneralize','task-firstshare-friendcircle','task-first-

						generalize-success','task-firstfriend-download','task-firstfriend-VIP','task-invite-friend','task-hotgeneralize','task-share-

						friendcircle','task-call-friends','task-friend-download'
						)
				and 
					zone in 
						(
						'successfully','goods_share','share','share_float','friend','circle-friend','copy','spread','promotion','paste','share-

						immediately','get-currency','share_friend','circlefriend'
						)
		    ) n1
		left join
			(
				select 
					m2.product_id,
					m1.ad_id
				from
					(
						select
						    ad_id,
						    item_id
						from 
						    origin_common.cc_ods_fs_dwxk_ad_items_daily
					) m1
				inner join
					(
						select
						    item_id,
						    app_item_id as product_id
						from 
						    origin_common.cc_ods_dwxk_fs_wk_items
					) m2
				on 
					m1.item_id = m2.item_id
			) n2
		on 
			n1.ad_id=n2.ad_id
		where 
			product_id in (110019405576,110019405615,110019405627,110019405628,110019405632)
			) as p0
		inner join
			(
				select 
					uid,
					create_time
				from 
					origin_common.cc_ods_dwxk_wk_sales_deal_ctime
			)p1
		on 
			p0.share_id=p1.uid 
		where 
			p1.create_time-p0.timestamp<=86400
	)s2
on 
	s1.share_id=s2.share_id
///////////////////////////////////////////////////////////////////////////////////
分享后24小时内再次分享
select 
	s1.share_id,
	count(share_sec_id)
from 
	(
		select
			user_id as share_id
		from 
			origin_common.cc_ods_log_cctapp_click_hourly
		where 
			module in
			(
			'login','index_new_share','red_packet','help_farmers','self-support','Netease-recommend','Netease-home','Netease-

			diet','Netease-baby','Netease-electric','insurance','insurance_detail','vip-materia','vip-materia-circle-friend','materia-circle-

			friend-goods','materia-circle-friend-material','materia-circle-friend-new','vip-invite-download','vip-

			invite','vip_shop','coin_share','coin_share_complete','detail_app','detail_material','command_detail',
			'command_circle_of_friends','command_activity','task-share','task-first-hotgeneralize','task-firstshare-friendcircle','task-first-

			generalize-success','task-firstfriend-download','task-firstfriend-VIP','task-invite-friend','task-hotgeneralize','task-share-

			friendcircle','task-call-friends','task-friend-download'
			)
		and 
			zone in 
			(
			'successfully','goods_share','share','share_float','friend','circle-friend','copy','spread','promotion','paste','share-

			immediately','get-currency','share_friend','circlefriend'
			)
	) s1
left join
(  
    select 
	    share_sec_id
	from 

	   (
	        select 
				n2.product_id,
				n1.share_id,
				timestamp
			from
				(
					select
					    user_id as share_id,
					    ad_id,
					    timestamp
					from 
						origin_common.cc_ods_log_cctapp_click_hourly
					where 
						ds >= '${begin_date}'
					and  
						ds <='${end_date}'
					and 
						module in
						(
						'login','index_new_share','red_packet','help_farmers','self-support','Netease-recommend','Netease-home','Netease-

						diet','Netease-baby','Netease-electric','insurance','insurance_detail','vip-materia','vip-materia-circle-friend','materia-circle-

						friend-goods','materia-circle-friend-material','materia-circle-friend-new','vip-invite-download','vip-

						invite','vip_shop','coin_share','coin_share_complete','detail_app','detail_material','command_detail',
						'command_circle_of_friends','command_activity','task-share','task-first-hotgeneralize','task-firstshare-friendcircle','task-first-

						generalize-success','task-firstfriend-download','task-firstfriend-VIP','task-invite-friend','task-hotgeneralize','task-share-

						friendcircle','task-call-friends','task-friend-download'
						)
					and 
						zone in 
						(
						'successfully','goods_share','share','share_float','friend','circle-friend','copy','spread','promotion','paste','share-

						immediately','get-currency','share_friend','circlefriend'
						)
			    ) n1
			left join
				(
					select 
						m2.product_id,
						m1.ad_id
					from
						(
							select
							    ad_id,
							    item_id
							from 
							    origin_common.cc_ods_fs_dwxk_ad_items_daily
						) m1
					inner join
						(
							select
							    item_id,
							    app_item_id as product_id
							from 
							    origin_common.cc_ods_dwxk_fs_wk_items
						) m2
					on 
						m1.item_id = m2.item_id
				) n2
			on 
				n1.ad_id=n2.ad_id
			where 
				product_id in (110019405576,110019405615,110019405627,110019405628,110019405632)
		) as p0
	inner join
	   (
		    select
				user_id as share_sec_id,
				ad_id,
				timestamp
			from 
				origin_common.cc_ods_log_cctapp_click_hourly
			where 
				module in
				(
				'login','index_new_share','red_packet','help_farmers','self-support','Netease-recommend','Netease-home','Netease-

				diet','Netease-baby','Netease-electric','insurance','insurance_detail','vip-materia','vip-materia-circle-friend','materia-circle-

				friend-goods','materia-circle-friend-material','materia-circle-friend-new','vip-invite-download','vip-

				invite','vip_shop','coin_share','coin_share_complete','detail_app','detail_material','command_detail',
				'command_circle_of_friends','command_activity','task-share','task-first-hotgeneralize','task-firstshare-friendcircle','task-first-

				generalize-success','task-firstfriend-download','task-firstfriend-VIP','task-invite-friend','task-hotgeneralize','task-share-

				friendcircle','task-call-friends','task-friend-download'
				)
			and 
				zone in 
				(
				'successfully','goods_share','share','share_float','friend','circle-friend','copy','spread','promotion','paste','share-

				immediately','get-currency','share_friend','circlefriend'
				)
		
		) as p1
	on 
		p0.share_id=p1.share_sec_id
	where 
		p0.timestamp-p1.timestamp <=86400
）as s2
on 
	s1.share_id=s2.share_sec_id

///////////////////////////////////////////////////////////////////////////////////
浏览者极其24小时内后续动作 

