select sourceid,targetid,
case when sourceid= 10872054 then 'outbound' when targetid= 10872054 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 10872054 or targetid = 10872054
group by sourceid,targetid
order by total_amount desc;