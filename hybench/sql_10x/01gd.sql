select sourceid,targetid,
case when sourceid= 1443447 then 'outbound' when targetid= 1443447 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 1443447 or targetid = 1443447
group by sourceid,targetid
order by total_amount desc;