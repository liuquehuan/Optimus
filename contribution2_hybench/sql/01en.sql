select sourceid,targetid,
case when sourceid= 114974 then 'outbound' when targetid= 114974 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 114974 or targetid = 114974
group by sourceid,targetid
order by total_amount desc;