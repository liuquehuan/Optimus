select sourceid,targetid,
case when sourceid= 5581 then 'outbound' when targetid= 5581 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 5581 or targetid = 5581
group by sourceid,targetid
order by total_amount desc;