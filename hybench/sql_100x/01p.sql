select sourceid,targetid,
case when sourceid= 16334800 then 'outbound' when targetid= 16334800 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 16334800 or targetid = 16334800
group by sourceid,targetid
order by total_amount desc;