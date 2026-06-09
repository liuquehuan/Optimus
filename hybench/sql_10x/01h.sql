select sourceid,targetid,
case when sourceid= 756597 then 'outbound' when targetid= 756597 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 756597 or targetid = 756597
group by sourceid,targetid
order by total_amount desc;