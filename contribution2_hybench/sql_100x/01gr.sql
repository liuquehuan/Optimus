select sourceid,targetid,
case when sourceid= 5802825 then 'outbound' when targetid= 5802825 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 5802825 or targetid = 5802825
group by sourceid,targetid
order by total_amount desc;