select sourceid,targetid,
case when sourceid= 99436 then 'outbound' when targetid= 99436 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 99436 or targetid = 99436
group by sourceid,targetid
order by total_amount desc;