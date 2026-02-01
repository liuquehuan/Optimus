select sourceid,targetid,
case when sourceid= 14430384 then 'outbound' when targetid= 14430384 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 14430384 or targetid = 14430384
group by sourceid,targetid
order by total_amount desc;