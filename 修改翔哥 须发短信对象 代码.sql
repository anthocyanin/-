select
    n1.uid as user_id,
    n5.delivery_mobilephone as phone_number
from
(
    select
        t1.uid as uid
    from
    (
        select
            distinct
            t1.uid as uid 
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime t1
        inner join
            origin_common.cc_ods_dwxk_fs_wk_cck_user t2
        where  
            t1.ds>=20180801 
        and 
            t1.ds<=20180930
        and 
            t1.uid>99956039
        and 
            t2.ds=20180930
        and 
            t2.platform=14
    )t1
    left join
    (
        select
            distinct
            t1.uid as uid
        from 
            origin_common.cc_ods_dwxk_wk_sales_deal_ctime t1
        inner join
            origin_common.cc_ods_dwxk_fs_wk_cck_user t2
        where  
            t1.ds>=20181001 
        and 
            t1.ds<=20181021
        and 
            t1.uid>99956039
        and 
            t2.ds=20181021
        and 
            t2.platform=14
    )t3
    on t1.uid=t3.uid
    where t3.uid is null
) n1
left join
(
    select
        h4.user_id as user_id,
        h4.delivery_mobilephone as delivery_mobilephone
    from
    (
        select
            h3.user_id as user_id,
            h3.delivery_mobilephone as delivery_mobilephone,
            h3.phone_count as phone_count,
            row_number()over(partition by h3.user_id order by h3.phone_count desc) as cc
        from
        (
            select
                user_id,
                delivery_mobilephone,
                count(*) as phone_count
            from
                origin_common.cc_order_user_pay_time
            where 
                ds>=20180801 
            and 
                ds<=20180930 
            and 
                source_channel=2
            and
                user_id>99956039
            group by 
                user_id,delivery_mobilephone
        )h3
    ) h4
    where 
        h4.cc=1
)n5
on n1.uid=n5.user_id
