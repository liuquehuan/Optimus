select sourceid,targetid,
case when sourceid= 98265 then 'outbound' when targetid= 98265 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 98265 or targetid = 98265
group by sourceid,targetid
order by total_amount desc;