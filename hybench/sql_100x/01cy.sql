select sourceid,targetid,
case when sourceid= 17076260 then 'outbound' when targetid= 17076260 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 17076260 or targetid = 17076260
group by sourceid,targetid
order by total_amount desc;