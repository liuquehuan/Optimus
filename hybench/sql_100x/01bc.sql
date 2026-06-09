select sourceid,targetid,
case when sourceid= 15787473 then 'outbound' when targetid= 15787473 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 15787473 or targetid = 15787473
group by sourceid,targetid
order by total_amount desc;