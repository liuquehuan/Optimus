select sourceid,targetid,
case when sourceid= 25191531 then 'outbound' when targetid= 25191531 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 25191531 or targetid = 25191531
group by sourceid,targetid
order by total_amount desc;