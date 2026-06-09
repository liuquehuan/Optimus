select sourceid,targetid,
case when sourceid= 1122864 then 'outbound' when targetid= 1122864 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1122864 or targetid = 1122864
group by sourceid,targetid
order by total_amount desc;