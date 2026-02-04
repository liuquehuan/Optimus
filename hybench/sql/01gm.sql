select sourceid,targetid,
case when sourceid= 267211 then 'outbound' when targetid= 267211 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 267211 or targetid = 267211
group by sourceid,targetid
order by total_amount desc;