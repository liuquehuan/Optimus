select sourceid,targetid,
case when sourceid= 117081 then 'outbound' when targetid= 117081 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 117081 or targetid = 117081
group by sourceid,targetid
order by total_amount desc;