-- vim: set ft=sql:
-- EXPLAIN (FORMAT JSON)
select
    100.00 * sum(case when i_data like 'PR%' then ol_amount else 0 end)
        / (1 + sum(ol_amount)) as promo_revenue
from
    order_line,
    item
where
    ol_i_id = i_id
    and ol_delivery_d >= '1996-01-02 20:10:02.277640'
    and ol_delivery_d < '1996-02-02 20:10:02.277640'
