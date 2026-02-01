select sourceid,targetid,
case when sourceid= 90250 then 'outbound' when targetid= 90250 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 90250 or targetid = 90250
group by sourceid,targetid
order by total_amount desc;