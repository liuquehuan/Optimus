select sourceid,targetid,
case when sourceid= 604925 then 'outbound' when targetid= 604925 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 604925 or targetid = 604925
group by sourceid,targetid
order by total_amount desc;