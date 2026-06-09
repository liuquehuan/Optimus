select sourceid,targetid,
case when sourceid= 2697620 then 'outbound' when targetid= 2697620 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2697620 or targetid = 2697620
group by sourceid,targetid
order by total_amount desc;