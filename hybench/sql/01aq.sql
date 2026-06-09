select sourceid,targetid,
case when sourceid= 128811 then 'outbound' when targetid= 128811 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 128811 or targetid = 128811
group by sourceid,targetid
order by total_amount desc;