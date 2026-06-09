select sourceid,targetid,
case when sourceid= 467210 then 'outbound' when targetid= 467210 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 467210 or targetid = 467210
group by sourceid,targetid
order by total_amount desc;