select sourceid,targetid,
case when sourceid= 213086 then 'outbound' when targetid= 213086 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 213086 or targetid = 213086
group by sourceid,targetid
order by total_amount desc;