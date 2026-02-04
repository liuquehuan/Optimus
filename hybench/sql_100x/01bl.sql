select sourceid,targetid,
case when sourceid= 8352948 then 'outbound' when targetid= 8352948 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 8352948 or targetid = 8352948
group by sourceid,targetid
order by total_amount desc;