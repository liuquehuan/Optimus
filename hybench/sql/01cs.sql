select sourceid,targetid,
case when sourceid= 188149 then 'outbound' when targetid= 188149 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 188149 or targetid = 188149
group by sourceid,targetid
order by total_amount desc;