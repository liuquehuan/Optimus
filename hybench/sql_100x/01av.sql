select sourceid,targetid,
case when sourceid= 13218135 then 'outbound' when targetid= 13218135 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 13218135 or targetid = 13218135
group by sourceid,targetid
order by total_amount desc;