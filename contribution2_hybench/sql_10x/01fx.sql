select sourceid,targetid,
case when sourceid= 1086767 then 'outbound' when targetid= 1086767 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1086767 or targetid = 1086767
group by sourceid,targetid
order by total_amount desc;