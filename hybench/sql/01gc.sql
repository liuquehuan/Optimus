select sourceid,targetid,
case when sourceid= 18789 then 'outbound' when targetid= 18789 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 18789 or targetid = 18789
group by sourceid,targetid
order by total_amount desc;