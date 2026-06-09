select sourceid,targetid,
case when sourceid= 1005707 then 'outbound' when targetid= 1005707 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1005707 or targetid = 1005707
group by sourceid,targetid
order by total_amount desc;