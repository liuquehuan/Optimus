select sourceid,targetid,
case when sourceid= 144836 then 'outbound' when targetid= 144836 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 144836 or targetid = 144836
group by sourceid,targetid
order by total_amount desc;