select sourceid,targetid,
case when sourceid= 143426 then 'outbound' when targetid= 143426 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 143426 or targetid = 143426
group by sourceid,targetid
order by total_amount desc;