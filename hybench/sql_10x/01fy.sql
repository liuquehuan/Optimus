select sourceid,targetid,
case when sourceid= 1866460 then 'outbound' when targetid= 1866460 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1866460 or targetid = 1866460
group by sourceid,targetid
order by total_amount desc;