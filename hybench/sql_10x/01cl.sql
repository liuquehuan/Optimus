select sourceid,targetid,
case when sourceid= 964680 then 'outbound' when targetid= 964680 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 964680 or targetid = 964680
group by sourceid,targetid
order by total_amount desc;