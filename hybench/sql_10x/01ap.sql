select sourceid,targetid,
case when sourceid= 587670 then 'outbound' when targetid= 587670 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 587670 or targetid = 587670
group by sourceid,targetid
order by total_amount desc;