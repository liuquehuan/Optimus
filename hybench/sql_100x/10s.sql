With big_account_inbound as (select t.targetid as id, c.name as name, sum(t.amount) as amount
from customer c, transfer t
where date(t.timestamp::timestamp) between '2023-11-01' and (cast('2023-11-01' as date) + 60)
and c.custid=t.targetid
group by name, id
order by amount DESC limit 10),
big_account_outbound as (select b.id, b.name as name, sum(t.amount) as amount
from big_account_inbound b, transfer t
where date(t.timestamp::timestamp) between '2023-11-01' and (cast('2023-11-01' as date) + 60)
and b.id=t.sourceid
group by b.id, b.name)
select bin.id, bin.name, (sum(bin.amount)-sum(bout.amount)) as revenue
from big_account_inbound bin, big_account_outbound bout
where bin.id=bout.id
group by bin.id, bin.name
order by revenue DESC;