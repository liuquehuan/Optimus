select sourceid,targetid,
case when sourceid= 2269340 then 'outbound' when targetid= 2269340 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2269340 or targetid = 2269340
group by sourceid,targetid
order by total_amount desc;