select sourceid,targetid,
case when sourceid= 15239340 then 'outbound' when targetid= 15239340 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 15239340 or targetid = 15239340
group by sourceid,targetid
order by total_amount desc;