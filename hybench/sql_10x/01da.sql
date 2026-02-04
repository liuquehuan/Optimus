select sourceid,targetid,
case when sourceid= 1871004 then 'outbound' when targetid= 1871004 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1871004 or targetid = 1871004
group by sourceid,targetid
order by total_amount desc;