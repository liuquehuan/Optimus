select sourceid,targetid,
case when sourceid= 10805956 then 'outbound' when targetid= 10805956 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 10805956 or targetid = 10805956
group by sourceid,targetid
order by total_amount desc;