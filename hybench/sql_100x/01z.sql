select sourceid,targetid,
case when sourceid= 6723358 then 'outbound' when targetid= 6723358 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 6723358 or targetid = 6723358
group by sourceid,targetid
order by total_amount desc;