select sourceid,targetid,
case when sourceid= 274122 then 'outbound' when targetid= 274122 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 274122 or targetid = 274122
group by sourceid,targetid
order by total_amount desc;