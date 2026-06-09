select sourceid,targetid,
case when sourceid= 207722 then 'outbound' when targetid= 207722 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 207722 or targetid = 207722
group by sourceid,targetid
order by total_amount desc;