select sourceid,targetid,
case when sourceid= 208812 then 'outbound' when targetid= 208812 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 208812 or targetid = 208812
group by sourceid,targetid
order by total_amount desc;