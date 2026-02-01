select sourceid,targetid,
case when sourceid= 240515 then 'outbound' when targetid= 240515 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 240515 or targetid = 240515
group by sourceid,targetid
order by total_amount desc;