select sourceid,targetid,
case when sourceid= 2475224 then 'outbound' when targetid= 2475224 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2475224 or targetid = 2475224
group by sourceid,targetid
order by total_amount desc;