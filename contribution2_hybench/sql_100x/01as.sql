select sourceid,targetid,
case when sourceid= 8135532 then 'outbound' when targetid= 8135532 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 8135532 or targetid = 8135532
group by sourceid,targetid
order by total_amount desc;