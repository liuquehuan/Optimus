select sourceid,targetid,
case when sourceid= 274239 then 'outbound' when targetid= 274239 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 274239 or targetid = 274239
group by sourceid,targetid
order by total_amount desc;