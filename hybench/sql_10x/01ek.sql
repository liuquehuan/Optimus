select sourceid,targetid,
case when sourceid= 2362360 then 'outbound' when targetid= 2362360 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2362360 or targetid = 2362360
group by sourceid,targetid
order by total_amount desc;