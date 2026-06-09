select sourceid,targetid,
case when sourceid= 1823987 then 'outbound' when targetid= 1823987 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1823987 or targetid = 1823987
group by sourceid,targetid
order by total_amount desc;