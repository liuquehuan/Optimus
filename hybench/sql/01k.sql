select sourceid,targetid,
case when sourceid= 20818 then 'outbound' when targetid= 20818 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 20818 or targetid = 20818
group by sourceid,targetid
order by total_amount desc;