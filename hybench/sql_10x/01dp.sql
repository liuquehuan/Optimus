select sourceid,targetid,
case when sourceid= 20300 then 'outbound' when targetid= 20300 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 20300 or targetid = 20300
group by sourceid,targetid
order by total_amount desc;