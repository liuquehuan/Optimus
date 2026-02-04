select sourceid,targetid,
case when sourceid= 25322015 then 'outbound' when targetid= 25322015 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 25322015 or targetid = 25322015
group by sourceid,targetid
order by total_amount desc;