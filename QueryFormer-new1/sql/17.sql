-- vim: set ft=sql:
-- EXPLAIN (FORMAT JSON)
select
    sum(ol_amount) / 2.0 as avg_yearly
from
    order_line, (
        select
            i_id, avg(ol_quantity) as a
        from
            item,
            order_line
        where
            i_data like '%b'
            and ol_i_id = i_id
            and ol_delivery_d >= '1992-01-02 20:10:02.277640'
        group by
            i_id) t
where
    ol_i_id = t.i_id
    and ol_quantity < t.a
    and ol_delivery_d >= '1992-01-02 20:10:02.277640';