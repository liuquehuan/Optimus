select sourceid,targetid,
case when sourceid= 155321 then 'outbound' when targetid= 155321 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 155321 or targetid = 155321
group by sourceid,targetid
order by total_amount desc;