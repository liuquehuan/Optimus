select sourceid,targetid,
case when sourceid= 15687332 then 'outbound' when targetid= 15687332 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 15687332 or targetid = 15687332
group by sourceid,targetid
order by total_amount desc;