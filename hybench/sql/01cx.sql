select sourceid,targetid,
case when sourceid= 298702 then 'outbound' when targetid= 298702 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 298702 or targetid = 298702
group by sourceid,targetid
order by total_amount desc;