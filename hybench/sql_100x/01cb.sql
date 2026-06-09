select sourceid,targetid,
case when sourceid= 17181095 then 'outbound' when targetid= 17181095 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 17181095 or targetid = 17181095
group by sourceid,targetid
order by total_amount desc;