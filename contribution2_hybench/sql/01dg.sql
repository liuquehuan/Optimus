select sourceid,targetid,
case when sourceid= 107608 then 'outbound' when targetid= 107608 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 107608 or targetid = 107608
group by sourceid,targetid
order by total_amount desc;