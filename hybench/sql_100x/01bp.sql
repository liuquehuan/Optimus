select sourceid,targetid,
case when sourceid= 20662911 then 'outbound' when targetid= 20662911 then 'inbound' end as direction,
sum(amount) as total_amount
from transfer where sourceid= 20662911 or targetid = 20662911
group by sourceid,targetid
order by total_amount desc;