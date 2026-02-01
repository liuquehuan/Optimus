select sourceid,targetid,
case when sourceid= 7118 then 'outbound' when targetid= 7118 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 7118 or targetid = 7118
group by sourceid,targetid
order by total_amount desc;