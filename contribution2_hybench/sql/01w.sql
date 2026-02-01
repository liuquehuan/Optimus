select sourceid,targetid,
case when sourceid= 92070 then 'outbound' when targetid= 92070 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 92070 or targetid = 92070
group by sourceid,targetid
order by total_amount desc;