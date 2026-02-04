select sourceid,targetid,
case when sourceid= 10378 then 'outbound' when targetid= 10378 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 10378 or targetid = 10378
group by sourceid,targetid
order by total_amount desc;