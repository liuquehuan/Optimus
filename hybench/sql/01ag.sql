select sourceid,targetid,
case when sourceid= 45211 then 'outbound' when targetid= 45211 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 45211 or targetid = 45211
group by sourceid,targetid
order by total_amount desc;