select sourceid,targetid,
case when sourceid= 14616578 then 'outbound' when targetid= 14616578 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 14616578 or targetid = 14616578
group by sourceid,targetid
order by total_amount desc;