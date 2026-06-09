select sourceid,targetid,
case when sourceid= 122051 then 'outbound' when targetid= 122051 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 122051 or targetid = 122051
group by sourceid,targetid
order by total_amount desc;