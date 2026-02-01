select sourceid,targetid,
case when sourceid= 4914342 then 'outbound' when targetid= 4914342 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 4914342 or targetid = 4914342
group by sourceid,targetid
order by total_amount desc;