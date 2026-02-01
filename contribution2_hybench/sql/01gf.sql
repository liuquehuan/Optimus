select sourceid,targetid,
case when sourceid= 170713 then 'outbound' when targetid= 170713 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 170713 or targetid = 170713
group by sourceid,targetid
order by total_amount desc;