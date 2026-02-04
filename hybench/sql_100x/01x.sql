select sourceid,targetid,
case when sourceid= 4270417 then 'outbound' when targetid= 4270417 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 4270417 or targetid = 4270417
group by sourceid,targetid
order by total_amount desc;