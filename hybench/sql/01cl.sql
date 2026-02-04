select sourceid,targetid,
case when sourceid= 96328 then 'outbound' when targetid= 96328 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 96328 or targetid = 96328
group by sourceid,targetid
order by total_amount desc;