select sourceid,targetid,
case when sourceid= 14738 then 'outbound' when targetid= 14738 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 14738 or targetid = 14738
group by sourceid,targetid
order by total_amount desc;