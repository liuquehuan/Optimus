select sourceid,targetid,
case when sourceid= 6820 then 'outbound' when targetid= 6820 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 6820 or targetid = 6820
group by sourceid,targetid
order by total_amount desc;