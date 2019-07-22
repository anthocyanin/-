######线上pma port=4035 拉取供应商系统商品库存、供货价new，下沉到sku_id关联
select
    t3.pm_sid,
    concat("\'",t3.pm_pid),
    t3.pm_title,
    t1.pb_sku_id,
    t1.pb_batch,
    t1.pb_price,
    t1.pb_stock,
    t1.pb_desc
from
(
    select
        pb_sku_id
        pb_stock,
        pb_price,
        pb_batch,
        pb_desc,
    from
        op_product_batches
)t1
left join
(
    select
        psm_sku_id
        psm_pid,
    from
        op_product_skus_map
)t2
on t1.pb_sku_id=t2.psm_sku_id
left join
(
    select
        pm_pid,
        pm_sid,
        pm_title
    from op_products_map
)t3
on t2.psm_pid=t3.pm_pid
where t3.pm_sid is not null