select sourceid,targetid,
case when sourceid= 7803491 then 'outbound' when targetid= 7803491 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 7803491 or targetid = 7803491
group by sourceid,targetid
order by total_amount desc;