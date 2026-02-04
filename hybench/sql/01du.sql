select sourceid,targetid,
case when sourceid= 113803 then 'outbound' when targetid= 113803 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 113803 or targetid = 113803
group by sourceid,targetid
order by total_amount desc;