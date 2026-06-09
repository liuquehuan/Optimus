select sourceid,targetid,
case when sourceid= 9450720 then 'outbound' when targetid= 9450720 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 9450720 or targetid = 9450720
group by sourceid,targetid
order by total_amount desc;