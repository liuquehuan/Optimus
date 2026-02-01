-- vim: set ft=sql:
-- EXPLAIN (FORMAT JSON)
select
    su_nationkey as supp_nation,
    c_nationkey,
    extract(year from o_entry_d) as l_year,
    sum(ol_amount) as revenue
from
    supplier,
    stock,
    order_line,
    orders,
    customer,
    nation n1,
    nation n2
where
    ol_supply_w_id = s_w_id
    and ol_i_id = s_i_id
    and mod((s_w_id * s_i_id), 10000) = su_suppkey
    and ol_w_id = o_w_id
    and ol_d_id = o_d_id
    and ol_o_id = o_id
    and c_id = o_c_id
    and c_w_id = o_w_id
    and c_d_id = o_d_id
    and su_nationkey = n1.n_nationkey
    and c_nationkey = n2.n_nationkey
    and (
        (n1.n_name = 'Germany' and n2.n_name = 'Cambodia')
        or
        (n1.n_name = 'Cambodia' and n2.n_name = 'Germany')
    )
    and ol_delivery_d between '1995-01-02 20:10:02.277640' and '1997-01-01 20:10:02.277640'
    and o_entry_d >= '1992-01-02 20:10:02.277640'
group by
    su_nationkey,
    c_nationkey,
    extract(year from o_entry_d)
order by
    su_nationkey,
    c_nationkey,
    l_year;
