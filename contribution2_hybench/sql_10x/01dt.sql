select sourceid,targetid,
case when sourceid= 1099432 then 'outbound' when targetid= 1099432 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1099432 or targetid = 1099432
group by sourceid,targetid
order by total_amount desc;