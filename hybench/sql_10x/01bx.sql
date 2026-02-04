select sourceid,targetid,
case when sourceid= 1460747 then 'outbound' when targetid= 1460747 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1460747 or targetid = 1460747
group by sourceid,targetid
order by total_amount desc;