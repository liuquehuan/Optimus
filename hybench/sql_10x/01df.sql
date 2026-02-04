select sourceid,targetid,
case when sourceid= 767496 then 'outbound' when targetid= 767496 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 767496 or targetid = 767496
group by sourceid,targetid
order by total_amount desc;