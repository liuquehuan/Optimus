select sourceid,targetid,
case when sourceid= 266758 then 'outbound' when targetid= 266758 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 266758 or targetid = 266758
group by sourceid,targetid
order by total_amount desc;