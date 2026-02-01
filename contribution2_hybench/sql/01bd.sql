select sourceid,targetid,
case when sourceid= 250445 then 'outbound' when targetid= 250445 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 250445 or targetid = 250445
group by sourceid,targetid
order by total_amount desc;