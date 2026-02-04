select sourceid,targetid,
case when sourceid= 18661152 then 'outbound' when targetid= 18661152 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 18661152 or targetid = 18661152
group by sourceid,targetid
order by total_amount desc;