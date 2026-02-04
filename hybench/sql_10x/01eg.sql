select sourceid,targetid,
case when sourceid= 2745901 then 'outbound' when targetid= 2745901 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 2745901 or targetid = 2745901
group by sourceid,targetid
order by total_amount desc;