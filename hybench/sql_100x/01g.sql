select sourceid,targetid,
case when sourceid= 16617360 then 'outbound' when targetid= 16617360 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 16617360 or targetid = 16617360
group by sourceid,targetid
order by total_amount desc;