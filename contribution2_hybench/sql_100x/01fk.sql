select sourceid,targetid,
case when sourceid= 27570380 then 'outbound' when targetid= 27570380 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 27570380 or targetid = 27570380
group by sourceid,targetid
order by total_amount desc;