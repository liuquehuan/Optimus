select sourceid,targetid,
case when sourceid= 207000 then 'outbound' when targetid= 207000 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 207000 or targetid = 207000
group by sourceid,targetid
order by total_amount desc;