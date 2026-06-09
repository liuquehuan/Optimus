select sourceid,targetid,
case when sourceid= 196402 then 'outbound' when targetid= 196402 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 196402 or targetid = 196402
group by sourceid,targetid
order by total_amount desc;