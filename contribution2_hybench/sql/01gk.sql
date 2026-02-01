select sourceid,targetid,
case when sourceid= 102663 then 'outbound' when targetid= 102663 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 102663 or targetid = 102663
group by sourceid,targetid
order by total_amount desc;