select sourceid,targetid,
case when sourceid= 68252 then 'outbound' when targetid= 68252 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 68252 or targetid = 68252
group by sourceid,targetid
order by total_amount desc;