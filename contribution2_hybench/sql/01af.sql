select sourceid,targetid,
case when sourceid= 286015 then 'outbound' when targetid= 286015 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 286015 or targetid = 286015
group by sourceid,targetid
order by total_amount desc;