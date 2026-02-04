select sourceid,targetid,
case when sourceid= 23315566 then 'outbound' when targetid= 23315566 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 23315566 or targetid = 23315566
group by sourceid,targetid
order by total_amount desc;