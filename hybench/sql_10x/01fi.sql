select sourceid,targetid,
case when sourceid= 1252512 then 'outbound' when targetid= 1252512 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1252512 or targetid = 1252512
group by sourceid,targetid
order by total_amount desc;