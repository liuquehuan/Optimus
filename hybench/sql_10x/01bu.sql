select sourceid,targetid,
case when sourceid= 1858804 then 'outbound' when targetid= 1858804 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1858804 or targetid = 1858804
group by sourceid,targetid
order by total_amount desc;