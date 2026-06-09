select sourceid,targetid,
case when sourceid= 1862251 then 'outbound' when targetid= 1862251 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1862251 or targetid = 1862251
group by sourceid,targetid
order by total_amount desc;