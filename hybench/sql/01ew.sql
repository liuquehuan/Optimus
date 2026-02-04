select sourceid,targetid,
case when sourceid= 190692 then 'outbound' when targetid= 190692 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 190692 or targetid = 190692
group by sourceid,targetid
order by total_amount desc;