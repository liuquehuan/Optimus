select sourceid,targetid,
case when sourceid= 1453528 then 'outbound' when targetid= 1453528 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1453528 or targetid = 1453528
group by sourceid,targetid
order by total_amount desc;