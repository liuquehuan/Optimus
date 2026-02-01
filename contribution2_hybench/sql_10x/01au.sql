select sourceid,targetid,
case when sourceid= 146526 then 'outbound' when targetid= 146526 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 146526 or targetid = 146526
group by sourceid,targetid
order by total_amount desc;