select sourceid,targetid,
case when sourceid= 28194362 then 'outbound' when targetid= 28194362 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 28194362 or targetid = 28194362
group by sourceid,targetid
order by total_amount desc;