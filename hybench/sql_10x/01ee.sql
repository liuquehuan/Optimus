select sourceid,targetid,
case when sourceid= 370335 then 'outbound' when targetid= 370335 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 370335 or targetid = 370335
group by sourceid,targetid
order by total_amount desc;