select sourceid,targetid,
case when sourceid= 21426 then 'outbound' when targetid= 21426 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 21426 or targetid = 21426
group by sourceid,targetid
order by total_amount desc;