select sourceid,targetid,
case when sourceid= 236610 then 'outbound' when targetid= 236610 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 236610 or targetid = 236610
group by sourceid,targetid
order by total_amount desc;