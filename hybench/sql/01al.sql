select sourceid,targetid,
case when sourceid= 51945 then 'outbound' when targetid= 51945 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 51945 or targetid = 51945
group by sourceid,targetid
order by total_amount desc;