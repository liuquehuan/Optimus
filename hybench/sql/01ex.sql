select sourceid,targetid,
case when sourceid= 74129 then 'outbound' when targetid= 74129 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 74129 or targetid = 74129
group by sourceid,targetid
order by total_amount desc;