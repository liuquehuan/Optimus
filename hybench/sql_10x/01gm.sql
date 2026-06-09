select sourceid,targetid,
case when sourceid= 1349182 then 'outbound' when targetid= 1349182 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1349182 or targetid = 1349182
group by sourceid,targetid
order by total_amount desc;