select sourceid,targetid,
case when sourceid= 129012 then 'outbound' when targetid= 129012 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 129012 or targetid = 129012
group by sourceid,targetid
order by total_amount desc;