select sourceid,targetid,
case when sourceid= 39143 then 'outbound' when targetid= 39143 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 39143 or targetid = 39143
group by sourceid,targetid
order by total_amount desc;