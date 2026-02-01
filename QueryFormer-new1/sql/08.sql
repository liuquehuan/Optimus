-- vim: set ft=sql:
-- EXPLAIN (FORMAT JSON)
select
    extract(year from o_entry_d) as l_year,
    sum(case when n2.n_name = 'Germany' then ol_amount else 0 end)
        / (0.0000001+sum(ol_amount)) as mkt_share
from
    item,
    supplier,
    stock,
    order_line,
    orders,
    customer,
    nation n1,
    nation n2,
    region
where
    i_id = s_i_id
    and ol_i_id = s_i_id
    and ol_supply_w_id = s_w_id
    and mod((s_w_id * s_i_id), 10000) = su_suppkey
    and ol_w_id = o_w_id
    and ol_d_id = o_d_id
    and ol_o_id = o_id
    and c_id = o_c_id
    and c_w_id = o_w_id
    and c_d_id = o_d_id
    and n1.n_nationkey = c_nationkey
    and n1.n_regionkey = r_regionkey
    and ol_i_id < 1000
    and r_name = 'Europe'
    and su_nationkey = n2.n_nationkey
    and o_entry_d between '1995-01-02 20:10:02.277640' and '1997-01-01 20:10:02.277640'
    and i_data like '%b'
    and i_id = ol_i_id
    and ol_delivery_d >= '1992-01-02 20:10:02.277640'
group by
    extract(year from o_entry_d)
order by
    l_year;
