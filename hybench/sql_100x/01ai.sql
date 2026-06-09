select sourceid,targetid,
case when sourceid= 5031459 then 'outbound' when targetid= 5031459 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 5031459 or targetid = 5031459
group by sourceid,targetid
order by total_amount desc;