######优惠券
select
    t1.date   as date,
    t1.range  as range,
    t1.id     as template_id,
    from_unixtime(t1.start_time,'yyyyMMdd HH:mm:ss') as start_time,
    from_unixtime(t1.end_time,'yyyyMMdd HH:mm:ss')   as end_time,
    t1.coupon_get_count as coupon_get_count,
    t1.user_get_count   as user_get_count,
    t2.order_add_count  as order_add_count,
    t2.order_pay_count  as order_pay_count,
    t2.coupon_add_count as coupon_add_count,
    t2.coupon_pay_count as coupon_pay_count,
    t2.order_add_price  as order_add_price,
    t2.order_pay_price  as order_pay_price,
    t2.coupon_add_price as coupon_add_price,
    t2.coupon_pay_price as coupon_pay_price,
    cast(t2.coupon_pay_count/t1.coupon_get_count as double) as conversion_rate,
    cast(t2.order_pay_price/t2.order_pay_count as double) as order_single_price,
    cast(t2.order_pay_price/t2.coupon_pay_price as double) as coupon_single_price,
    t2.add_price  as add_price,
    t2.pay_price  as pay_price
from
(
  SELECT
    t02.date       as date,
    max(t01.range) as range,
    t01.id         as id,
    max(t01.start_time) as start_time,
    max(t01.end_time)   as end_time,
    count(distinct t02.coupon_sn)   as  coupon_get_count,
    count(distinct t02.user_id)     as  user_get_count
  FROM
    (
    select
      id,
      range,
      start_time,
      end_time
    from origin_common.cc_coupon_temp
    where ds = 20180619 and platform='ccj_cct' and id in (8489067,8489359,8489089,8489148,8489160,8489944,8489966,8489975,8490176,8490184,8490190,8490197,8490208,8490228,8490232,8490234,8490411,8490421,8490428,8490438,8490929,8490934,8490943,8490949,8490974,8490976,8490979,8490982,8490983,8490985,8490991,8490998,8491145,8491154,8491162,8491166,8491119,8491122,8491130,8491138,8747304,8750073,8792211,8792428,8792651,8792841,8793086,8793261,8793476,8793640,8793845,8794899,8798559,8798706,8798981,8799285,8799435,8799579,8799822,8800027,8422546,8422547,8422548,8422549,8486065,8422551,8422552,8422553,8422554,8422555,8422556,8422557,8422558)
    ) as t01
    left join
    (
    select
        ds as date,
        coupon_sn,
        template_id,
        user_id
    from origin_common.cc_coupon_user
    where ds>=20180613 and ds<=20180616 and platform='ccj_cct'
    ) as t02
    on t01.id = t02.template_id
    group by t02.date,t01.id
) t1
left join
(
select
    t05.date              as date,
    t05.range             as range,
    t05.id                as id,
    t05.start_time        as start_time,
    t05.end_time          as end_time,
    t05.order_add_count   as order_add_count,
    t10.order_pay_count   as order_pay_count,
    t05.coupon_add_count  as coupon_add_count,
    t10.coupon_pay_count  as coupon_pay_count,
    t05.order_add_price   as order_add_price,
    t10.order_pay_price   as order_pay_price,
    t05.coupon_add_price  as coupon_add_price,
    t10.coupon_pay_price  as coupon_pay_price,
    t05.add_price         as add_price,
    t10.pay_price         as pay_price
from
(
    select
        max(t01.range) as range,
        t01.id         as id,
        t04.date       as date,
        max(t01.start_time) as start_time,
        max(t01.end_time) as end_time,
        count(t04.order_sn) as order_add_count,
        count(distinct t04.coupon_sn) as coupon_add_count,
        sum(t04.total_fee) as order_add_price,
        sum(t04.add_fee) as add_price,
        sum(t04.used_money) as coupon_add_price
    from
    (
        select
            range,
            id,
            start_time,
            end_time
        from origin_common.cc_coupon_temp
        where ds = 20180619 and platform='ccj_cct' and id in (8489067,8489359,8489089,8489148,8489160,8489944,8489966,
        8489975,8490176,8490184,8490190,8490197,8490208,8490228,8490232,8490234,8490411,8490421,8490428,8490438,8490929,8490934,8490943,8490949,8490974,8490976,8490979,8490982,8490983,8490985,8490991,8490998,8491145,8491154,8491162,8491166,8491119,8491122,8491130,8491138,8747304,8750073,8792211,8792428,8792651,8792841,8793086,8793261,8793476,8793640,8793845,8794899,8798559,8798706,8798981,8799285,8799435,8799579,8799822,8800027,8422546,8422547,8422548,8422549,8486065,8422551,8422552,8422553,8422554,8422555,8422556,8422557,8422558)
    ) as t01
 
  left join
    (
        select
            t02.date         as date,
            t02.order_sn     as order_sn,
            t02.template_id  as template_id,
            t02.coupon_sn    as coupon_sn,
            t02.used_money   as used_money,
            t03.total_fee    as total_fee,
            t03.pay_fee      as add_fee
        from
        (
            select
                ds as date,
                order_sn,
                template_id,
                coupon_sn,
                used_money
            from origin_common.cc_order_coupon_addtime
            where ds>=20180613 and ds<=20180616 and template_id in (8489067,8489359,8489089,8489148,8489160,8489944,8489966,8489975,8490176,8490184,8490190,8490197,8490208,8490228,8490232,8490234,8490411,8490421,8490428,8490438,8490929,8490934,8490943,8490949,8490974,8490976,8490979,8490982,8490983,8490985,8490991,8490998,8491145,8491154,8491162,8491166,8491119,8491122,8491130,8491138,8747304,8750073,8792211,8792428,8792651,8792841,8793086,8793261,8793476,8793640,8793845,8794899,8798559,8798706,8798981,8799285,8799435,8799579,8799822,8800027,8422546,8422547,8422548,8422549,8486065,8422551,8422552,8422553,8422554,8422555,8422556,8422557,8422558)
        ) as t02
        left join
        (
            select
                ds as date,
                order_sn,
                total_fee,
                pay_fee
            from origin_common.cc_order_user_add_time
            where ds>=20180613 and ds<=20180616 and source_channel = 2
        ) as t03
        on t02.date=t03.date and t02.order_sn=t03.order_sn
    ) as t04
    on t01.id=t04.template_id
    group by t04.date,t01.id
) as t05

left join
(
    select
        max(t06.range)      as range,
        t06.id              as id,
        t09.date            as date,
        max(t06.start_time) as start_time,
        max(t06.end_time)   as end_time,
        count(t09.order_sn) as order_pay_count,
        count(distinct t09.coupon_sn) as coupon_pay_count,
        sum(t09.total_fee)  as order_pay_price,
        sum(t09.pay_fee)    as pay_price,
        sum(t09.used_money) as coupon_pay_price
    from
    (
        select
            range,
            id,
            start_time,
            end_time
        from origin_common.cc_coupon_temp
        where ds = 20180619 and platform='ccj_cct' and id in (8489067,8489359,8489089,8489148,8489160,8489944,8489966,8489975,8490176,8490184,8490190,8490197,8490208,8490228,8490232,8490234,8490411,8490421,8490428,8490438,8490929,8490934,8490943,8490949,8490974,8490976,8490979,8490982,8490983,8490985,8490991,8490998,8491145,8491154,8491162,8491166,8491119,8491122,8491130,8491138,8747304,8750073,8792211,8792428,8792651,8792841,8793086,8793261,8793476,8793640,8793845,8794899,8798559,8798706,8798981,8799285,8799435,8799579,8799822,8800027,8422546,8422547,8422548,8422549,8486065,8422551,8422552,8422553,8422554,8422555,8422556,8422557,8422558)
    ) as t06
    left join
    (
        select
            t07.date        as date,
            t07.order_sn    as order_sn,
            t07.template_id as template_id,
            t07.coupon_sn   as coupon_sn,
            t07.used_money  as used_money,
            t08.total_fee   as total_fee,
            t08.pay_fee     as pay_fee
        from
        (
            select
                ds as date,
                order_sn,
                template_id,
                coupon_sn,
                used_money
            from origin_common.cc_order_coupon_paytime
            where ds>=20180613 and ds<=20180616 and template_id in (8489067,8489359,8489089,8489148,8489160,8489944,8489966,8489975,8490176,8490184,8490190,8490197,8490208,8490228,8490232,8490234,8490411,8490421,8490428,8490438,8490929,8490934,8490943,8490949,8490974,8490976,8490979,8490982,8490983,8490985,8490991,8490998,8491145,8491154,8491162,8491166,8491119,8491122,8491130,8491138,8747304,8750073,8792211,8792428,8792651,8792841,8793086,8793261,8793476,8793640,8793845,8794899,8798559,8798706,8798981,8799285,8799435,8799579,8799822,8800027,8422546,8422547,8422548,8422549,8486065,8422551,8422552,8422553,8422554,8422555,8422556,8422557,8422558)
        ) as t07
        left join
        (
            select
                ds as date,
                order_sn,
                total_fee,
                pay_fee
            from origin_common.cc_order_user_pay_time
            where ds>=20180613 and ds<=20180616 and source_channel = 2
        ) as t08
        on t07.date=t08.date and t07.order_sn=t08.order_sn
    ) as t09
    on t06.id=t09.template_id
    group by t09.date,t06.id
) as t10
on t05.date=t10.date and t05.id=t10.id
) t2
on t1.date=t2.date and t1.id=t2.id