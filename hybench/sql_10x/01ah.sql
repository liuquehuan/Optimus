select sourceid,targetid,
case when sourceid= 2779094 then 'outbound' when targetid= 2779094 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2779094 or targetid = 2779094
group by sourceid,targetid
order by total_amount desc;