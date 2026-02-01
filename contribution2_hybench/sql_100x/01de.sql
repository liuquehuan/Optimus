select sourceid,targetid,
case when sourceid= 28524507 then 'outbound' when targetid= 28524507 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 28524507 or targetid = 28524507
group by sourceid,targetid
order by total_amount desc;