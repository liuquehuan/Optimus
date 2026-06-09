select sourceid,targetid,
case when sourceid= 26212 then 'outbound' when targetid= 26212 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 26212 or targetid = 26212
group by sourceid,targetid
order by total_amount desc;