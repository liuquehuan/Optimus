-- vim: set ft=sql:
-- EXPLAIN (FORMAT JSON)
select
    o_ol_cnt,
    sum(case when o_carrier_id = 1 or o_carrier_id = 2 then 1 else 0 end)
        as high_line_count,
    sum(case when o_carrier_id <> 1 and o_carrier_id <> 2 then 1 else 0 end)
        as low_line_count
from
    orders,
    order_line
where
    ol_w_id = o_w_id
    and ol_d_id = o_d_id
    and ol_o_id = o_id
    and o_entry_d <= ol_delivery_d
    and ol_delivery_d >= '1996-01-02 20:10:02.277640'
    and ol_delivery_d < '1997-01-02 20:10:02.277640'
    and o_entry_d >= '1992-01-02 20:10:02.277640'
group by
    o_ol_cnt
order by
    o_ol_cnt;
