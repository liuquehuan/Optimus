select sourceid,targetid,
case when sourceid= 267751 then 'outbound' when targetid= 267751 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 267751 or targetid = 267751
group by sourceid,targetid
order by total_amount desc;