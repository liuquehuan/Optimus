select sourceid,targetid,
case when sourceid= 4342 then 'outbound' when targetid= 4342 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 4342 or targetid = 4342
group by sourceid,targetid
order by total_amount desc;