select sourceid,targetid,
case when sourceid= 106336 then 'outbound' when targetid= 106336 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 106336 or targetid = 106336
group by sourceid,targetid
order by total_amount desc;