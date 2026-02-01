-- vim: set ft=sql:
-- EXPLAIN (FORMAT JSON)
with revenue (supplier_no, total_revenue) as (
    select
        mod((s_w_id * s_i_id), 10000) as supplier_no,
        sum(ol_amount) as total_revenue
    from
        order_line,
        stock
    where
        ol_i_id = s_i_id
        and ol_supply_w_id = s_w_id
        and ol_delivery_d >= '1995-10-02 20:10:02.277640'
        and ol_delivery_d < '1996-01-02 20:10:02.277640'
    group by
        mod((s_w_id * s_i_id), 10000)
)
select
    su_suppkey,
    su_name,
    su_address,
    su_phone,
    total_revenue
from
    supplier,
    revenue
where
    su_suppkey = supplier_no
    and total_revenue = (select max(total_revenue) from revenue)
order by
    su_suppkey;
