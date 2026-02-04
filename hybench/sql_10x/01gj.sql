select sourceid,targetid,
case when sourceid= 1150544 then 'outbound' when targetid= 1150544 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1150544 or targetid = 1150544
group by sourceid,targetid
order by total_amount desc;