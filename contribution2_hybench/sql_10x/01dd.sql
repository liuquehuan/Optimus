select sourceid,targetid,
case when sourceid= 2776203 then 'outbound' when targetid= 2776203 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2776203 or targetid = 2776203
group by sourceid,targetid
order by total_amount desc;