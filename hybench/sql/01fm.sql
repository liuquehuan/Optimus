select sourceid,targetid,
case when sourceid= 82016 then 'outbound' when targetid= 82016 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 82016 or targetid = 82016
group by sourceid,targetid
order by total_amount desc;