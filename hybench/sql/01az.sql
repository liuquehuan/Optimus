select sourceid,targetid,
case when sourceid= 9600 then 'outbound' when targetid= 9600 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 9600 or targetid = 9600
group by sourceid,targetid
order by total_amount desc;