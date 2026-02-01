select sourceid,targetid,
case when sourceid= 2717034 then 'outbound' when targetid= 2717034 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2717034 or targetid = 2717034
group by sourceid,targetid
order by total_amount desc;