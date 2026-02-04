select sourceid,targetid,
case when sourceid= 25200521 then 'outbound' when targetid= 25200521 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 25200521 or targetid = 25200521
group by sourceid,targetid
order by total_amount desc;