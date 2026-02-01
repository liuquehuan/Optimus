select sourceid,targetid,
case when sourceid= 1056303 then 'outbound' when targetid= 1056303 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1056303 or targetid = 1056303
group by sourceid,targetid
order by total_amount desc;