select sourceid,targetid,
case when sourceid= 20639022 then 'outbound' when targetid= 20639022 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 20639022 or targetid = 20639022
group by sourceid,targetid
order by total_amount desc;