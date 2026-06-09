select sourceid,targetid,
case when sourceid= 30737 then 'outbound' when targetid= 30737 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 30737 or targetid = 30737
group by sourceid,targetid
order by total_amount desc;