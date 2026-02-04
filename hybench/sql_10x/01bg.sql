select sourceid,targetid,
case when sourceid= 701455 then 'outbound' when targetid= 701455 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 701455 or targetid = 701455
group by sourceid,targetid
order by total_amount desc;