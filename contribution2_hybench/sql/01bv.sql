select sourceid,targetid,
case when sourceid= 59911 then 'outbound' when targetid= 59911 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 59911 or targetid = 59911
group by sourceid,targetid
order by total_amount desc;