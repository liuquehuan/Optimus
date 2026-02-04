select sourceid,targetid,
case when sourceid= 1364091 then 'outbound' when targetid= 1364091 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1364091 or targetid = 1364091
group by sourceid,targetid
order by total_amount desc;