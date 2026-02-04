select sourceid,targetid,
case when sourceid= 676528 then 'outbound' when targetid= 676528 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 676528 or targetid = 676528
group by sourceid,targetid
order by total_amount desc;