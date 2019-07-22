#楚楚推推广漏斗代码
alter table
  ${hive.databases.data.name}.${hive.tables.cc_dm_spread_funnel_stat_source.name}
add
  if not exists partition (ds = '${bizdate}') location '${bizdate}';
insert overwrite table ${hive.databases.data.name}.${hive.tables.cc_dm_spread_funnel_stat_source.name} partition (ds = '${bizdate}')

select 
    max(p1.date),--日期
    p1.title,--主题
    p2.types,--用户类别
    sum(p1.page_pv),--页面pv
    sum(coalesce(p1.page_share_pv,0)),--首页分享按钮点击次数
    sum(coalesce(p1.detail_pv,0)),--详情页展现pv
    sum(coalesce(p1.spread_pv,0)),--详情页推广点击次数
    0 as material_click,--详情页素材点击次数
    sum(coalesce(p1.material_pv,0)),--详情页素材页展现pv
    coalesce(sum(coalesce(p1.material_pv,0))/sum(coalesce(p1.detail_pv,0)),0.00) as material_show_rate,--进入素材页转化率(%)
    sum(coalesce(p1.promotion_pv,0)),--素材一键推广点击次数
    sum(coalesce(p1.wechatpro_pv,0)),--分享至微信次数
    sum(coalesce(p1.circlefriendpro_pv,0)),--分享至朋友圈次数
    coalesce((sum(coalesce(p1.wechatpro_pv,0))+sum(coalesce(p1.circlefriendpro_pv,0)))/sum(coalesce(p1.detail_pv,0)),0.00) as promotion_rate,--详情页素材一键推广转化率(%)
    sum(coalesce(p1.pqrcode_pv,0)),--详情页二维码分享点击次数
    sum(coalesce(p1.wechatpqc_pv,0)),--分享至微信次数
    sum(coalesce(p1.circlefriendpqc_pv,0)),--分享至朋友圈次数
    coalesce((sum(coalesce(p1.wechatpqc_pv,0))+sum(coalesce(p1.circlefriendpqc_pv,0)))/sum(coalesce(p1.detail_pv,0)),0.00) as pqrcode_rate--详情页二维码推广转化率(%)
from
(
    select 
        '${bizdate}' as date,
        concat('分类-',t2.title) as title,
        t1.user_id,
        t1.page_pv,
        0 as page_share_pv,
        t3.detail_pv,
        t3.spread_pv,
        t3.promotion_pv,
        t3.wechatpro_pv,
        t3.circlefriendpro_pv,
        t3.pqrcode_pv,
        t3.wechatpqc_pv,
        t3.circlefriendpqc_pv,
        t3.material_pv
    from
    (
        select 
            module,
            zone,
            ad_id,
            user_id,
            count(*) as page_pv
        from 
            origin_common.cc_ods_log_cctapp_click_hourly
        where 
            ds='${bizdate}'
        and module ='category-list'
        and zone='datalist'
        and length(ad_id)>4
        and source in ('cct','cctui')
        group by module,zone,ad_id,user_id
    ) t1
    join
    (
        select 
            *
        from 
            origin_common.cc_ods_fs_cck_ad_material_images
        where 
            ds='${bizdate}'
        and 
            title in ('精选品牌','日用百货','食品酒水','护肤彩妆','母婴用品','数码家电','服饰鞋包')
    ) t2 
    on t1.ad_id=t2.ad_material_id
    left join
    (
        select 
            a1.category,
            a1.user_id,
            sum(if(a1.zone='enter',pv,0)) as detail_pv,
            sum(if(a1.zone='spread',pv,0)) as spread_pv,
            sum(if(a1.zone='promotion',pv,0)) as promotion_pv,
            sum(if(a1.zone='wechatpro',pv,0)) as wechatpro_pv,
            sum(if(a1.zone='circlefriendpro',pv,0)) as circlefriendpro_pv,
            sum(if(a1.zone='pqrcode',pv,0)) as pqrcode_pv,
            sum(if(a1.zone='wechatpqc',pv,0)) as wechatpqc_pv,
            sum(if(a1.zone='circlefriendpqc',pv,0)) as circlefriendpqc_pv,
            sum(if(a1.module='detail_material' and a1.zone='show',pv,0)) as material_pv
        from
        (
            select 
                module,
                zone,
                split(query,'\u0002')[0] as category,
                user_id,
                count(*) as pv
            from 
                origin_common.cc_ods_log_cctapp_click_hourly
            where 
                ds='${bizdate}'
                and module in ('detail','detail_app','detail_material')
                and zone in ('enter','spread','promotion','wechatpro','circlefriendpro','pqrcode','wechatpqc','circlefriendpqc','show')
                and ad_type='category'
                and source in ('cct','cctui')
                and split(query,'\u0002')[0]!=''
            group by module,zone,split(query,'\u0002')[0],user_id
        ) a1
        group by a1.category,a1.user_id
    ) t3 
    on t2.title=t3.category and t1.user_id=t3.user_id

    union all

    select  
        '${bizdate}' as date,
        concat('金刚-',t6.title) as title,
        t6.user_id,
        t6.page_pv,
        t3.page_share_pv,
        t3.detail_pv,
        t3.spread_pv,
        t3.promotion_pv,
        t3.wechatpro_pv,
        t3.circlefriendpro_pv,
        t3.pqrcode_pv,
        t3.wechatpqc_pv,
        t3.circlefriendpqc_pv,
        t3.material_pv
    from
    (
        select 
            t2.title,
            t1.user_id,
            t1.page_pv
        from
        (
            select 
                module,
                zone,
                ad_id,
                user_id,
                count(*) as page_pv
            from origin_common.cc_ods_log_cctapp_click_hourly
            where 
                ds='${bizdate}'
                and module ='cct-home-king'
                and zone='bannerlist'
                and source in ('cct','cctui')
            group by module,zone,ad_id,user_id
        ) t1
        join
        (
            select 
                ad_material_id,
                title
            from 
                origin_common.cc_ods_fs_cck_ad_material_banners
            where 
                ds='${bizdate}'
                and 
                title in ('楚楚助农','楚楚自营','网易严选','京东自营','高佣精选','海外购')
        ) t2 
        on t1.ad_id=t2.ad_material_id
        union all
        select 
            '新人专区' as title,
            user_id,
            count(*) as page_pv
        from 
            origin_common.cc_ods_log_cctapp_click_hourly
        where 
            ds='${bizdate}'
            and module ='noob'
            and zone='show'
            and source in ('cct','cctui')
        group by module,zone,user_id
    ) t6
    left join
    (
        select  
            a4.title,
            a4.user_id,
            sum(if(a4.zone='enter',pv,0)) as detail_pv,
            sum(if(a4.zone='spread',pv,0)) as spread_pv,
            sum(if(a4.zone='promotion',pv,0)) as promotion_pv,
            sum(if(a4.zone='wechatpro',pv,0)) as wechatpro_pv,
            sum(if(a4.zone='circlefriendpro',pv,0)) as circlefriendpro_pv,
            sum(if(a4.zone='pqrcode',pv,0)) as pqrcode_pv,
            sum(if(a4.zone='wechatpqc',pv,0)) as wechatpqc_pv,
            sum(if(a4.zone='circlefriendpqc',pv,0)) as circlefriendpqc_pv,
            sum(if(a4.module='detail_material'  and a4.zone='show',pv,0)) as material_pv,
            sum(if(a4.module='new'  and a4.zone='new-share',pv,0)) as page_share_pv
        from
        (
            select 
                a3.module,
                a3.zone,
                a2.title,
                a3.user_id,
                count(*) as pv
            from
            (
                select 
                    a1.title,
                    a1.page_id
                from
                (
                    select 
                        ad_material_id,
                        title,
                        split(regexp_extract(query,'act_html(.*?).json', 1),'_')[1] as page_id,
                        row_number() over(partition by title order by  ad_material_id desc) as sort_num
                    from 
                        origin_common.cc_ods_fs_cck_ad_material_banners
                    where 
                        ds='${bizdate}'
                        and 
                        title in ('楚楚助农','楚楚自营','海外购')
                        and 
                        template='afp'
                ) a1
                where a1.sort_num=1
            ) a2
            join
            (
                select 
                    *
                from 
                    origin_common.cc_ods_log_cctapp_click_hourly
                where 
                    ds='${bizdate}'
                    and module in ('detail','detail_app','detail_material')
                    and zone in ('enter','spread','promotion','wechatpro','circlefriendpro','pqrcode','wechatpqc','circlefriendpqc','show')
                    and ad_type ='special-activity'
                    and source in ('cct','cctui')
            ) a3
            on a2.page_id=a3.ad_material_id
            group by a3.module,a3.zone,a2.title,a3.user_id
            union all
            select 
                module,
                zone,
                '京东自营' as title,
                user_id,
                count(*) as pv
            from origin_common.cc_ods_log_cctapp_click_hourly
            where 
                ds='${bizdate}'
               and module in ('detail','detail_app','detail_material')
               and zone in ('enter','spread','promotion','wechatpro','circlefriendpro','pqrcode','wechatpqc','circlefriendpqc','show')
               and ad_type ='special-activity'
               and source in ('cct','cctui')
               and ad_material_id in (43291,43321,43348,43347,43322,43349,43350,43351,43352)
            group by module,zone,'京东自营',user_id
            union all
            select 
                module,
                zone,
                '网易严选' as title,
                user_id,
                count(*) as pv
            from origin_common.cc_ods_log_cctapp_click_hourly
            where 
                ds='${bizdate}'
               and module in ('detail','detail_app','detail_material')
               and zone in ('enter','spread','promotion','wechatpro','circlefriendpro','pqrcode','wechatpqc','circlefriendpqc','show')
               and ad_type ='special-activity'
               and source in ('cct','cctui')
               and ad_material_id in (43269,43278,43282,43280,43276,43281,43279)
            group by module,zone,'网易严选',user_id
            union all
            select 
                module,
                zone,
                if(ad_type='cct-new-people-buy.productlist','新人专区','高佣精选') as title,
                user_id,
                count(*) as pv
            from origin_common.cc_ods_log_cctapp_click_hourly
            where 
                ds='${bizdate}'
               and module in ('detail','detail_app','detail_material','new')
               and zone in ('enter','spread','promotion','wechatpro','circlefriendpro','pqrcode','wechatpqc','circlefriendpqc','show','new-share')
               and source in ('cct','cctui')
               and ad_type  in ('cct-new-people-buy.productlist')
            group by module,zone,ad_type,user_id
            union all
            select 
                module,
                zone,
               '高佣精选' as title,
                user_id,
                count(*) as pv
            from origin_common.cc_ods_log_cctapp_click_hourly
            where ds='${bizdate}'
               and module in ('detail','detail_app','detail_material','new')
               and zone in ('enter','spread','promotion','wechatpro','circlefriendpro','pqrcode','wechatpqc','circlefriendpqc','show','new-share')
               and source in ('cct','cctui')
               and ad_type ='special-activity'
               and ad_material_id=48070
            group by module,zone,ad_type,user_id
            union all
            select 
                module,
                zone,
                '新人专区' as title,
                user_id,
                count(*) as pv
            from origin_common.cc_ods_log_cctapp_click_hourly
            where ds='${bizdate}'
               and module in ('new')
               and zone in ('new-share')
               and source in ('cct','cctui')
             group by module,zone,user_id
        ) a4
        group by a4.title,a4.user_id
    ) t3 
    on t6.title=t3.title and t6.user_id=t3.user_id

    union all

    select  
        '${bizdate}' as date,
        concat('首页-',t1.title) as title,
        t1.user_id,
        t1.page_pv,
        0 as page_share_pv,
        t2.detail_pv,
        t2.spread_pv,
        t2.promotion_pv,
        t2.wechatpro_pv,
        t2.circlefriendpro_pv,
        t2.pqrcode_pv,
        t2.wechatpqc_pv,
        t2.circlefriendpqc_pv,
        t2.material_pv
    from
    (
        select 
            (
            case 
                when zone='banner' then '首页banner'
                when zone='banner_fix' then '通栏banner'
                when zone='splash_screen_click' then '闪屏banner'
            else '弹窗' end
            ) as title,
            user_id,
            count(*) as page_pv
        from 
            origin_common.cc_ods_log_cctapp_click_hourly
        where 
            ds='${bizdate}'
            and module ='index'
            and source in ('cct','cctui')
            and zone in ('banner','popup','banner_fix','splash_screen_click')
        group by module,zone,user_id
    ) t1
    left join
    (
        select 
            a1.title,
            a3.user_id,
            sum(detail_pv) as detail_pv,
            sum(spread_pv) as spread_pv,
            sum(promotion_pv) as promotion_pv,
            sum(wechatpro_pv) as wechatpro_pv,
            sum(circlefriendpro_pv) as circlefriendpro_pv,
            sum(pqrcode_pv) as pqrcode_pv,
            sum(wechatpqc_pv) as wechatpqc_pv,
            sum(circlefriendpqc_pv) as circlefriendpqc_pv,
            sum(material_pv) as material_pv
        from
        (
            select 
                distinct 
                (
                case 
                    when ad_key='cct-home-page' then '首页banner'
                    when ad_key='cct-home-slide' then '通栏banner'
                    when ad_key='nimation-second-page' then '闪屏banner'
                else '弹窗' end
                ) as title,
                split(regexp_extract(query,'act_html(.*?).json', 1),'_')[1] as page_id
            from
            (
                select 
                    *
                from 
                    origin_common.cc_ods_fs_cck_xb_policies_hourly
                where 
                    ad_key in ('cct-home-slide','cct-home-page-alert','cct-home-page','animation-second-page')
                   and zone='bannerlist'
                   and end_time>${bizdate_ts}
                   and begin_time<${gmtdate_ts}
                   and status!='delete'
            ) t1
            join
            (
                select 
                    ad_material_id,
                    query
                from origin_common.cc_ods_fs_cck_ad_material_banners
                where 
                    template='afp'
                    and title not like '%测试%'
                    and title not like '%试用%'
                union  all
                select 
                    ad_material_id,
                    query
                from origin_common.cc_ods_fs_cck_ad_material_images
                where 
                    template='afp'
                  and title not like '%测试%'
                  and title not like '%试用%'
            ) t2 
            on t1.ad_material_id=t2.ad_material_id
        ) a1
        left join
        (
            select 
                a2.ad_material_id as page_id,
                a2.user_id,
                sum(if(a2.zone='enter',pv,0)) as detail_pv,
                sum(if(a2.zone='spread',pv,0)) as spread_pv,
                sum(if(a2.zone='promotion',pv,0)) as promotion_pv,
                sum(if(a2.zone='wechatpro',pv,0)) as wechatpro_pv,
                sum(if(a2.zone='circlefriendpro',pv,0)) as circlefriendpro_pv,
                sum(if(a2.zone='pqrcode',pv,0)) as pqrcode_pv,
                sum(if(a2.zone='wechatpqc',pv,0)) as wechatpqc_pv,
                sum(if(a2.zone='circlefriendpqc',pv,0)) as circlefriendpqc_pv,
                sum(if(a2.module='detail_material' and a2.zone='show',pv,0)) as material_pv
            from
            (
                select 
                    module,
                    zone,
                    ad_material_id,
                    user_id,count(*) as pv
                from origin_common.cc_ods_log_cctapp_click_hourly
                where 
                    ds='${bizdate}'
                   and ad_type ='special-activity'
                   and module in ('detail','detail_app','detail_material')
                   and zone in ('enter','spread','promotion','wechatpro','circlefriendpro','pqrcode','wechatpqc','circlefriendpqc','show')
                   and source in ('cct','cctui')
                group by module,zone,ad_material_id,user_id
            ) a2
           group by a2.ad_material_id,a2.user_id
        ) a3 
        on a1.page_id=a3.page_id
        group by a1.title,a3.user_id
    ) t2 
    on t1.title=t2.title and t1.user_id=t2.user_id

    union all

    select  
        '${bizdate}' as date,
        concat('首页-',regexp_replace(t1.ad_type,'seckill-tab','秒杀')) as title,
        t1.user_id,
        t1.page_pv,
        t5.page_share_pv,
        t2.detail_pv,
        t2.spread_pv,
        t2.promotion_pv,
        t2.wechatpro_pv,
        t2.circlefriendpro_pv,
        t2.pqrcode_pv,
        t2.wechatpqc_pv,
        t2.circlefriendpqc_pv,
        t2.material_pv
    from
    (
        select 
            concat('seckill-tab-',ad_id) as ad_type,
            user_id,
            count(*) as page_pv
        from 
            origin_common.cc_ods_log_cctapp_click_hourly
        where ds='${bizdate}'
            and module in ('screenings-info')
            and zone='datalist'
            and source in ('cct','cctui')
            and ad_id not like '%-%'
        group by ad_id,user_id
    ) t1
    left join
    (
        select 
            module as ad_type,
            user_id,
            count(*) as page_share_pv
        from origin_common.cc_ods_log_cctapp_click_hourly
        where ds='${bizdate}'
            and module like 'seckill-tab%'
            and module not in ('seckill-tab-new')
            and module not like 'seckill-tab-old%'
            and zone='productlist-share'
            and source in ('cct','cctui')
        group by module,user_id
    ) t5 
    on t1.ad_type=t5.ad_type and t1.user_id=t5.user_id
    left join
    (
        select 
            regexp_replace(a1.ad_type,'.productlist','') as ad_type,
            a1.user_id,
            sum(if(a1.zone='enter',pv,0)) as detail_pv,
            sum(if(a1.zone='spread',pv,0)) as spread_pv,
            sum(if(a1.zone='promotion',pv,0)) as promotion_pv,
            sum(if(a1.zone='wechatpro',pv,0)) as wechatpro_pv,
            sum(if(a1.zone='circlefriendpro',pv,0)) as circlefriendpro_pv,
            sum(if(a1.zone='pqrcode',pv,0)) as pqrcode_pv,
            sum(if(a1.zone='wechatpqc',pv,0)) as wechatpqc_pv,
            sum(if(a1.zone='circlefriendpqc',pv,0)) as circlefriendpqc_pv,
            sum(if(a1.module='detail_material' and a1.zone='show',pv,0)) as material_pv
        from
        (
            select 
                ad_type,
                module,
                zone,
                user_id,
                count(*) as pv
            from origin_common.cc_ods_log_cctapp_click_hourly
            where 
                ds='${bizdate}'
                and module in ('detail','detail_app','detail_material')
                and zone in ('enter','spread','promotion','wechatpro','circlefriendpro','pqrcode','wechatpqc','circlefriendpqc','show')
                and source in ('cct','cctui')
                and ad_type like 'seckill-tab%'
            group by ad_type,module,zone,user_id
        ) a1
        group by regexp_replace(a1.ad_type,'.productlist',''),a1.user_id
    ) t2 
    on t1.ad_type=t2.ad_type and t1.user_id=t2.user_id

    union all

    select 
        '${bizdate}' as date,
        concat('首页-','搜索页') as title,
        t1.user_id,
        t1.page_pv,
        0 as page_share_pv,
        t2.detail_pv,
        t2.spread_pv,
        t2.promotion_pv,
        t2.wechatpro_pv,
        t2.circlefriendpro_pv,
        t2.pqrcode_pv,
        t2.wechatpqc_pv,
        t2.circlefriendpqc_pv,
        t2.material_pv
    from
    (
        select 
            zone,
            user_id,
            count(*) as page_pv
        from 
            origin_common.cc_ods_log_cctapp_click_hourly
        where 
            ds='${bizdate}'
            and module='index'
            and source in ('cct','cctui')
            and zone='search'
        group by zone,user_id
    ) t1
    left join
    (
        select 
            a1.ad_type,
            a1.user_id,
            sum(if(a1.zone='enter',pv,0)) as detail_pv,
            sum(if(a1.zone='spread',pv,0)) as spread_pv,
            sum(if(a1.zone='promotion',pv,0)) as promotion_pv,
            sum(if(a1.zone='wechatpro',pv,0)) as wechatpro_pv,
            sum(if(a1.zone='circlefriendpro',pv,0)) as circlefriendpro_pv,
            sum(if(a1.zone='pqrcode',pv,0)) as pqrcode_pv,
            sum(if(a1.zone='wechatpqc',pv,0)) as wechatpqc_pv,
            sum(if(a1.zone='circlefriendpqc',pv,0)) as circlefriendpqc_pv,
            sum(if(a1.module='detail_material' and a1.zone='show',pv,0)) as material_pv
        from
        (
            select 
                ad_type,
                module,
                zone,
                user_id,
                count(*) as pv
            from origin_common.cc_ods_log_cctapp_click_hourly
            where 
                ds='${bizdate}'
                  and module in ('detail','detail_app','detail_material')
                  and zone in ('enter','spread','promotion','wechatpro','circlefriendpro','pqrcode','wechatpqc','circlefriendpqc','show')
                  and ad_type ='search'
                  and source in ('cct','cctui')
            group by ad_type,module,zone,user_id
        ) a1
         group by a1.ad_type,a1.user_id
    ) t2 
    on t1.zone=t2.ad_type and t1.user_id=t2.user_id 
) p1
join
(
    select 
        distinct 
        t4.types,
        t4.cct_uid
    from
    (
        select 
            cct_uid,
            (
            case 
                when cck_vip_status=0 and cck_vip_level=0 then 1
                when cck_vip_status=0 and cck_vip_level=1 then 2
                when cck_vip_status=1 then 3 
            else 0 end
            ) as types
        from 
            origin_common.cc_ods_fs_tui_relation
        union all
        select 
            cct_uid,
            4 as types
        from 
            origin_common.cc_ods_fs_tui_relation
    ) t4
) p2 
on p1.user_id=p2.cct_uid
group by p1.title,p2.types

