select sourceid,targetid,
case when sourceid= 11007837 then 'outbound' when targetid= 11007837 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 11007837 or targetid = 11007837
group by sourceid,targetid
order by total_amount desc;