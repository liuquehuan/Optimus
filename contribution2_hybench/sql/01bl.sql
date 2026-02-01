select sourceid,targetid,
case when sourceid= 9553 then 'outbound' when targetid= 9553 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 9553 or targetid = 9553
group by sourceid,targetid
order by total_amount desc;