select sourceid,targetid,
case when sourceid= 215093 then 'outbound' when targetid= 215093 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 215093 or targetid = 215093
group by sourceid,targetid
order by total_amount desc;