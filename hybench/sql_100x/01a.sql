select sourceid,targetid,
case when sourceid= 10504451 then 'outbound' when targetid= 10504451 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 10504451 or targetid = 10504451
group by sourceid,targetid
order by total_amount desc;