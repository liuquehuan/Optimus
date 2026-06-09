select sourceid,targetid,
case when sourceid= 91568 then 'outbound' when targetid= 91568 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 91568 or targetid = 91568
group by sourceid,targetid
order by total_amount desc;