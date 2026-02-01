select sourceid,targetid,
case when sourceid= 942040 then 'outbound' when targetid= 942040 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 942040 or targetid = 942040
group by sourceid,targetid
order by total_amount desc;