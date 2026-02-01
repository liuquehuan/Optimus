select sourceid,targetid,
case when sourceid= 67246 then 'outbound' when targetid= 67246 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 67246 or targetid = 67246
group by sourceid,targetid
order by total_amount desc;