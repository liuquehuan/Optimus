select sourceid,targetid,
case when sourceid= 20553248 then 'outbound' when targetid= 20553248 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 20553248 or targetid = 20553248
group by sourceid,targetid
order by total_amount desc;