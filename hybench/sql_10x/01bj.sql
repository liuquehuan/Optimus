select sourceid,targetid,
case when sourceid= 1579299 then 'outbound' when targetid= 1579299 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1579299 or targetid = 1579299
group by sourceid,targetid
order by total_amount desc;