select sourceid,targetid,
case when sourceid= 177445 then 'outbound' when targetid= 177445 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 177445 or targetid = 177445
group by sourceid,targetid
order by total_amount desc;