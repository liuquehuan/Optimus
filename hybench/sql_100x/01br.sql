select sourceid,targetid,
case when sourceid= 29721622 then 'outbound' when targetid= 29721622 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 29721622 or targetid = 29721622
group by sourceid,targetid
order by total_amount desc;