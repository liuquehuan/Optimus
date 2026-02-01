-- vim: set ft=sql:
-- EXPLAIN (FORMAT JSON)
select
    o_ol_cnt,
    count(*) as order_count
from
    orders
where
    o_entry_d >= '1994-01-02 20:10:02.277640'
    and o_entry_d < '1994-04-02 20:10:02.277640'
    and exists (
        select
            *
        from
            order_line
        where
            o_id = ol_o_id
            and o_w_id = ol_w_id
            and o_d_id = ol_d_id
            and ol_delivery_d >= o_entry_d
            and ol_delivery_d >= '1992-01-02 20:10:02.277640'
    )
group by
    o_ol_cnt
order by
    o_ol_cnt;
