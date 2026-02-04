select sourceid,targetid,
case when sourceid= 9004362 then 'outbound' when targetid= 9004362 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 9004362 or targetid = 9004362
group by sourceid,targetid
order by total_amount desc;