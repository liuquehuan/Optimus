select sourceid,targetid,
case when sourceid= 196732 then 'outbound' when targetid= 196732 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 196732 or targetid = 196732
group by sourceid,targetid
order by total_amount desc;