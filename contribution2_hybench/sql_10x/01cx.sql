select sourceid,targetid,
case when sourceid= 2326232 then 'outbound' when targetid= 2326232 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2326232 or targetid = 2326232
group by sourceid,targetid
order by total_amount desc;