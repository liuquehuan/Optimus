select sourceid,targetid,
case when sourceid= 166847 then 'outbound' when targetid= 166847 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 166847 or targetid = 166847
group by sourceid,targetid
order by total_amount desc;