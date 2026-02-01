select sourceid,targetid,
case when sourceid= 739552 then 'outbound' when targetid= 739552 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 739552 or targetid = 739552
group by sourceid,targetid
order by total_amount desc;