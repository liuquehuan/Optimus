select sourceid,targetid,
case when sourceid= 229398 then 'outbound' when targetid= 229398 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 229398 or targetid = 229398
group by sourceid,targetid
order by total_amount desc;