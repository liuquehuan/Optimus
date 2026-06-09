select sourceid,targetid,
case when sourceid= 2555913 then 'outbound' when targetid= 2555913 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2555913 or targetid = 2555913
group by sourceid,targetid
order by total_amount desc;