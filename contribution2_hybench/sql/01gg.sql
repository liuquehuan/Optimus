select sourceid,targetid,
case when sourceid= 117347 then 'outbound' when targetid= 117347 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 117347 or targetid = 117347
group by sourceid,targetid
order by total_amount desc;