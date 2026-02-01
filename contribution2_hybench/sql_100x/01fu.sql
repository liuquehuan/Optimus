select sourceid,targetid,
case when sourceid= 907295 then 'outbound' when targetid= 907295 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 907295 or targetid = 907295
group by sourceid,targetid
order by total_amount desc;