select sourceid,targetid,
case when sourceid= 216976 then 'outbound' when targetid= 216976 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 216976 or targetid = 216976
group by sourceid,targetid
order by total_amount desc;