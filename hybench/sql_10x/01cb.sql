select sourceid,targetid,
case when sourceid= 1880172 then 'outbound' when targetid= 1880172 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1880172 or targetid = 1880172
group by sourceid,targetid
order by total_amount desc;