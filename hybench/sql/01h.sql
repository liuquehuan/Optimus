select sourceid,targetid,
case when sourceid= 280904 then 'outbound' when targetid= 280904 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 280904 or targetid = 280904
group by sourceid,targetid
order by total_amount desc;