select sourceid,targetid,
case when sourceid= 228579 then 'outbound' when targetid= 228579 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 228579 or targetid = 228579
group by sourceid,targetid
order by total_amount desc;