select sourceid,targetid,
case when sourceid= 1147649 then 'outbound' when targetid= 1147649 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1147649 or targetid = 1147649
group by sourceid,targetid
order by total_amount desc;