select sourceid,targetid,
case when sourceid= 1099903 then 'outbound' when targetid= 1099903 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1099903 or targetid = 1099903
group by sourceid,targetid
order by total_amount desc;